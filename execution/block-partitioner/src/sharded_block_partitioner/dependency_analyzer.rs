// Copyright © Aptos Foundation

use crate::sharded_block_partitioner::messages::DependencyAnalysisMsg;
use aptos_types::transaction::analyzed_transaction::{AnalyzedTransaction, StorageLocation};
use std::{collections::HashSet, sync::Arc};
use std::collections::HashMap;
use std::hash::Hash;
use std::io::Read;
use itertools::Itertools;
use aptos_crypto::hash::CryptoHash;

pub struct DependencyAnalyzer {
    shard_id: usize,
    num_shards: usize,
    // Exclusive read set, i.e. the set of storage locations that are read by this shard but not written to by this shard.
    // This is done to avoid duplication of keys in the read set and write set, which is slows down the conflict detection.
    exclusive_read_locations: HashMap<StorageLocation, usize>,
    write_locations: HashMap<StorageLocation, usize>,
}

impl DependencyAnalyzer {
    pub fn new(shard_id: usize, num_shards: usize, analyzed_txns: &[AnalyzedTransaction]) -> Self {
        let mut exclusive_read_locations = HashMap::new();
        let mut write_locations = HashMap::new();
        for analyzed_txn in analyzed_txns {
            for write_location in analyzed_txn.write_hints().iter() {
                if !write_locations.contains_key(write_location) {
                    let shard_id = CryptoHash::hash(write_location).byte(0) as usize % num_shards;
                    write_locations.insert(write_location.clone(), shard_id);
                }
            }
        }

        for analyzed_txn in analyzed_txns {
            for read_location in analyzed_txn.read_hints().iter() {
                if !write_locations.contains_key(read_location) && !exclusive_read_locations.contains_key(read_location) {
                    let anchor_shard_id = CryptoHash::hash(read_location).byte(0) as usize % num_shards;
                    exclusive_read_locations.insert(read_location.clone(), anchor_shard_id);
                }
            }
        }

        Self {
            shard_id,
            num_shards,
            exclusive_read_locations,
            write_locations,
        }
    }

    pub fn get_anchor_shard_id(&self, storage_location: &StorageLocation) -> Option<usize> {
        if let Some(shard_id) = self.write_locations.get(storage_location) {
            return Some(*shard_id);
        }
        if let Some(shard_id) = self.exclusive_read_locations.get(storage_location) {
            return Some(*shard_id);
        }
        None
    }

    pub fn get_dependency_analysis_msg(&self) -> DependencyAnalysisMsg {
        let exclusive_read_locations: Arc<HashSet<StorageLocation>> = Arc::new(self.exclusive_read_locations.keys().cloned().collect());
        let write_locations: Arc<HashSet<StorageLocation>> = Arc::new(self.write_locations.keys().cloned().collect());
        DependencyAnalysisMsg::new(
            self.shard_id,
            exclusive_read_locations,
            write_locations,
        )
    }
}
