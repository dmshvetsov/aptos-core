// Copyright © Aptos Foundation

use crate::sharded_block_partitioner::messages::DependencyAnalysisMsg;
use aptos_types::transaction::analyzed_transaction::{AnalyzedTransaction, StorageLocation};
use std::{collections::HashSet, sync::Arc};

pub struct DependencyAnalyzer {
    shard_id: usize,
    // Exclusive read set, i.e. the set of storage locations that are read by this shard but not written to by this shard.
    // This is done to avoid duplication of keys in the read set and write set, which is slows down the conflict detection.
    exclusive_read_set: Arc<HashSet<StorageLocation>>,
    write_set: Arc<HashSet<StorageLocation>>,
}

impl DependencyAnalyzer {
    pub fn new(shard_id: usize, analyzed_txns: &[AnalyzedTransaction]) -> Self {
        let mut read_set = HashSet::new();
        let mut write_set = HashSet::new();
        for analyzed_txn in analyzed_txns {
            write_set.extend(analyzed_txn.write_hints().iter().cloned());
        }
        for analyzed_txn in analyzed_txns {
            for txn_read_set in analyzed_txn.read_hints().iter() {
                if !write_set.contains(txn_read_set) {
                    read_set.insert(txn_read_set.clone());
                }
            }
        }
        Self {
            shard_id,
            exclusive_read_set: Arc::new(read_set),
            write_set: Arc::new(write_set),
        }
    }

    pub fn get_dependency_analysis_msg(&self) -> DependencyAnalysisMsg {
        DependencyAnalysisMsg::new(
            self.shard_id,
            self.exclusive_read_set.clone(),
            self.write_set.clone(),
        )
    }
}
