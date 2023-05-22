// Copyright © Aptos Foundation

use std::collections::HashSet;
use std::sync::Arc;
use aptos_types::transaction::analyzed_transaction::{AnalyzedTransaction, StorageLocation};
use aptos_types::transaction::Transaction;
use crate::sharded_block_partitioner::messages::DependencyAnalysisMsg;

pub struct DependencyAnalyzer {
    shard_id: usize,
    read_set: Arc<HashSet<StorageLocation>>,
    write_set: Arc<HashSet<StorageLocation>>,
}

impl DependencyAnalyzer {
    pub fn new(shard_id: usize, analyzed_txns: &[AnalyzedTransaction]) -> Self {
        let mut read_set = HashSet::new();
        let mut write_set = HashSet::new();
        for analyzed_txn in analyzed_txns {
            read_set.extend(analyzed_txn.read_hints().iter().map(|(storage_location)| storage_location.clone()));
            write_set.extend(analyzed_txn.write_hints().iter().map(|(storage_location)| storage_location.clone()));
        }
        Self {
            shard_id,
            read_set: Arc::new(read_set),
            write_set: Arc::new(write_set),
        }
    }

    pub fn get_dependency_analysis_msg(&self) -> DependencyAnalysisMsg {
        DependencyAnalysisMsg::new(self.shard_id, self.read_set.clone(), self.write_set.clone())
    }
}
