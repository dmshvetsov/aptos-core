// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db_metadata::DbMetadataSchema,
    jellyfish_merkle_node::JellyfishMerkleNodeSchema,
    metrics::PRUNER_VERSIONS,
    pruner::{db_pruner::DBPruner, state_store::generics::StaleNodeIndexSchemaTrait},
    schema::db_metadata::DbMetadataValue,
    state_merkle_db::StateMerkleDb,
    OTHER_TIMERS_SECONDS,
};
use anyhow::Result;
use aptos_infallible::Mutex;
use aptos_jellyfish_merkle::{node_type::NodeKey, StaleNodeIndex};
use aptos_schemadb::{schema::KeyCodec, ReadOptions, SchemaBatch, DB};
use aptos_types::transaction::{AtomicVersion, Version};
use claims::{assert_ge, assert_lt};
use once_cell::sync::Lazy;
use std::sync::{atomic::Ordering, Arc};

pub mod generics;
pub(crate) mod state_value_pruner;

#[cfg(test)]
mod test;

static TREE_PRUNER_WORKER_POOL: Lazy<rayon::ThreadPool> = Lazy::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(16)
        .thread_name(|index| format!("tree_pruner_worker_{}", index))
        .build()
        .unwrap()
});

/// Responsible for pruning the state tree.
#[derive(Debug)]
pub struct StateMerklePruner<S> {
    /// State DB.
    state_merkle_db: Arc<StateMerkleDb>,
    /// Keeps track of the target version that the pruner needs to achieve.
    target_version: AtomicVersion,
    /// Overall min readable version.
    progress: Mutex<Version>,
    /// Min readable version for each shard.
    shard_progresses: Mutex<Vec<Version>>,
    /// The version that is going to be pruned next.
    next_version: Mutex<Option<Version>>,
    _phantom: std::marker::PhantomData<S>,
}

impl<S: StaleNodeIndexSchemaTrait> DBPruner for StateMerklePruner<S>
where
    StaleNodeIndex: KeyCodec<S>,
{
    fn name(&self) -> &'static str {
        S::name()
    }

    fn prune(&self, batch_size: usize) -> Result<Version> {
        if !self.is_pruning_pending() {
            return Ok(self.min_readable_version());
        }
        let min_readable_version = self.min_readable_version();
        let target_version = self.target_version();

        self.prune_state_merkle(min_readable_version, target_version, batch_size)
    }

    fn save_min_readable_version(
        &self,
        version: Version,
        batch: &SchemaBatch,
    ) -> anyhow::Result<()> {
        // TODO(grao): Support sharding here.
        batch.put::<DbMetadataSchema>(&S::tag(None), &DbMetadataValue::Version(version))
    }

    fn initialize_min_readable_version(&self) -> Result<Version> {
        let min_readable_version = self
            .state_merkle_db
            .metadata_db()
            .get::<DbMetadataSchema>(&S::tag(None))?
            .map_or(0, |v| v.expect_version());
        self.finish_pending_pruning(min_readable_version)?;
        Ok(min_readable_version)
    }

    fn min_readable_version(&self) -> Version {
        *self.progress.lock()
    }

    fn set_target_version(&self, target_version: Version) {
        self.target_version.store(target_version, Ordering::Relaxed);
        PRUNER_VERSIONS
            .with_label_values(&[S::name(), "target"])
            .set(target_version as i64);
    }

    fn target_version(&self) -> Version {
        self.target_version.load(Ordering::Relaxed)
    }

    // used only by blanket `initialize()`, use the underlying implementation instead elsewhere.
    fn record_progress(&self, min_readable_version: Version) {
        *self.progress.lock() = min_readable_version;
        PRUNER_VERSIONS
            .with_label_values(&[S::name(), "min_readable"])
            .set(min_readable_version as i64);
    }

    fn is_pruning_pending(&self) -> bool {
        self.target_version() > *self.progress.lock()
    }

    /// (For tests only.) Updates the minimal readable version kept by pruner.
    fn testonly_update_min_version(&self, version: Version) {
        self.record_progress(version);
    }
}

impl<S: StaleNodeIndexSchemaTrait> StateMerklePruner<S>
where
    StaleNodeIndex: KeyCodec<S>,
{
    pub fn new(state_merkle_db: Arc<StateMerkleDb>) -> Self {
        let num_shards = state_merkle_db.num_shards();
        let mut shard_progresses = Vec::with_capacity(num_shards as usize);
        for shard_id in 0..num_shards {
            let db_shard = state_merkle_db.db_shard(shard_id);
            shard_progresses
                .push(Self::get_progress(&db_shard, Some(shard_id)).expect("Must succeed."));
        }

        let pruner = StateMerklePruner {
            state_merkle_db,
            target_version: AtomicVersion::new(0),
            progress: Mutex::new(0),
            shard_progresses: Mutex::new(shard_progresses),
            next_version: Mutex::new(None),
            _phantom: std::marker::PhantomData,
        };
        pruner.initialize();
        pruner
    }

    fn get_progress(state_merkle_db_shard: &DB, shard_id: Option<u8>) -> Result<Version> {
        Ok(state_merkle_db_shard
            .get::<DbMetadataSchema>(&S::tag(shard_id))?
            .map_or(0, |v| v.expect_version()))
    }

    fn prune_state_merkle(
        &self,
        min_readable_version: Version,
        target_version: Version,
        batch_size: usize,
    ) -> Result<Version> {
        let mut min_readable_version = min_readable_version;
        let mut target_version_for_this_batch =
            self.next_version.lock().unwrap_or(min_readable_version);
        while target_version_for_this_batch <= target_version {
            self.record_progress(target_version_for_this_batch);
            let next_version =
                self.prune_top_levels(min_readable_version, target_version_for_this_batch)?;
            *self.next_version.lock() = next_version;
            self.prune_shards(target_version_for_this_batch, batch_size)?;
            min_readable_version = target_version_for_this_batch;
            if let Some(next_version) = next_version {
                target_version_for_this_batch = next_version;
            } else {
                break;
            }
        }
        Ok(min_readable_version)
    }

    fn prune_top_levels(
        &self,
        min_readable_version: Version,
        target_version: Version,
    ) -> Result<Option<Version>> {
        let batch = SchemaBatch::new();
        let next_version = self.prune_state_merkle_shard(
            self.state_merkle_db.metadata_db(),
            min_readable_version,
            target_version,
            usize::max_value(),
            &batch,
        )?;
        batch.put::<DbMetadataSchema>(&S::tag(None), &DbMetadataValue::Version(target_version))?;
        self.state_merkle_db.metadata_db().write_schemas(batch)?;

        Ok(next_version)
    }

    fn prune_single_shard(
        &self,
        shard_id: u8,
        target_version: Version,
        batch_size: usize,
    ) -> Result<()> {
        let _timer = OTHER_TIMERS_SECONDS
            .with_label_values(&["state_merkle_pruner___prune_single_shard"])
            .start_timer();
        let shard_min_readable_version = self.get_shard_progress(shard_id);
        if shard_min_readable_version != target_version {
            assert_lt!(shard_min_readable_version, target_version);
            self.update_shard_progress(shard_id, target_version);
            let db_shard = self.state_merkle_db.db_shard(shard_id);
            loop {
                let batch = SchemaBatch::new();
                let next_version = self.prune_state_merkle_shard(
                    &db_shard,
                    shard_min_readable_version,
                    target_version,
                    batch_size,
                    &batch,
                )?;
                if let Some(next_version) = next_version {
                    if next_version <= target_version {
                        db_shard.write_schemas(batch)?;
                        continue;
                    }
                }
                batch.put::<DbMetadataSchema>(
                    &S::tag(Some(shard_id)),
                    &DbMetadataValue::Version(target_version),
                )?;
                db_shard.write_schemas(batch)?;
                break;
            }
        }

        Ok(())
    }

    fn prune_shards(&self, target_version: Version, batch_size: usize) -> Result<()> {
        let num_shards = self.state_merkle_db.num_shards();
        TREE_PRUNER_WORKER_POOL.scope(|s| {
            for shard_id in 0..num_shards {
                s.spawn(move |_| {
                    self.prune_single_shard(shard_id, target_version, batch_size)
                        .unwrap_or_else(|_| {
                            panic!("Failed to prune state merkle shard {shard_id}.")
                        });
                });
            }
        });

        Ok(())
    }

    fn finish_pending_pruning(&self, min_readable_version: Version) -> Result<()> {
        self.prune_shards(min_readable_version, usize::max_value())
    }

    fn get_shard_progress(&self, shard_id: u8) -> Version {
        self.shard_progresses.lock()[shard_id as usize]
    }

    fn update_shard_progress(&self, shard_id: u8, progress: Version) {
        self.shard_progresses.lock()[shard_id as usize] = progress;
    }

    // If the existing schema batch is not none, this function only adds items need to be
    // deleted to the schema batch and the caller is responsible for committing the schema batches
    // to the DB.
    fn prune_state_merkle_shard(
        &self,
        state_merkle_db_shard: &DB,
        min_readable_version: Version,
        target_version: Version,
        batch_size: usize,
        batch: &SchemaBatch,
    ) -> Result<Option<Version>> {
        assert_ne!(batch_size, 0);
        assert_ge!(target_version, min_readable_version);
        let (indices, next_version) = self.get_stale_node_indices(
            state_merkle_db_shard,
            min_readable_version,
            target_version,
            batch_size,
        )?;

        indices.into_iter().try_for_each(|index| {
            batch.delete::<JellyfishMerkleNodeSchema>(&index.node_key)?;
            batch.delete::<S>(&index)
        })?;

        Ok(next_version)
    }

    fn get_stale_node_indices(
        &self,
        state_merkle_db_shard: &DB,
        start_version: Version,
        target_version: Version,
        batch_size: usize,
    ) -> Result<(Vec<StaleNodeIndex>, Option<Version>)> {
        let mut indices = Vec::new();
        let mut iter = state_merkle_db_shard.iter::<S>(ReadOptions::default())?;
        iter.seek(&StaleNodeIndex {
            stale_since_version: start_version,
            node_key: NodeKey::new_empty_path(0),
        })?;

        let mut next_version = None;
        // over fetch by 1
        for _ in 0..=batch_size {
            if let Some((index, _)) = iter.next().transpose()? {
                next_version = Some(index.stale_since_version);
                if index.stale_since_version <= target_version {
                    indices.push(index);
                    continue;
                }
            }
            break;
        }

        if indices.len() > batch_size {
            indices.pop();
        }
        Ok((indices, next_version))
    }
}
