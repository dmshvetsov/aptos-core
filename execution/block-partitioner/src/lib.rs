// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod dependency_aware_partitioner;
pub mod test_utils;
pub mod transaction_dependency_graph;
pub mod sharded_block_partitioner;

use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use std::collections::HashMap;

pub trait BlockPartitioner: Send + Sync {
    /// Partitions the transactions into `num
    ///
    /// _shards` shards. Returns two maps, one for map of
    ///
    ///
    /// shard id to vector of accepted transaction in order of execution with original index
    /// and other is the map of shard id to vector of rejected transactions with original index.
    fn partition(
        &self,
        transactions: Vec<AnalyzedTransaction>,
        num_shards: usize,
    ) -> (
        HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
        HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
    );
}

/// An implementation of partitioner that splits the transactions into equal-sized chunks.
pub struct UniformPartitioner {}

impl BlockPartitioner for UniformPartitioner {
    fn partition(
        &self,
        transactions: Vec<AnalyzedTransaction>,
        num_shards: usize,
    ) -> (
        HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
        HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
    ) {
        // pre-poluate the hashmap with empty vectors for both accepted and rejected transactions
        // for each shard.
        let mut accpeted_transactions: HashMap<usize, Vec<(usize, AnalyzedTransaction)>> =
            HashMap::new();
        let mut rejected_transactions: HashMap<usize, Vec<(usize, AnalyzedTransaction)>> =
            HashMap::new();
        for i in 0..num_shards {
            accpeted_transactions.insert(i, Vec::new());
            rejected_transactions.insert(i, Vec::new());
        }
        let total_txns = transactions.len();
        if total_txns == 0 {
            return (HashMap::new(), HashMap::new());
        }
        let txns_per_shard = (total_txns as f64 / num_shards as f64).ceil() as usize;
        let mut accpeted_transactions: HashMap<usize, Vec<(usize, AnalyzedTransaction)>> =
            HashMap::new();

        for (index, txn) in transactions.into_iter().enumerate() {
            let partion_index = get_shard_for_index(txns_per_shard, index);
            accpeted_transactions
                .entry(partion_index)
                .and_modify(|v| v.push((index, txn)));
        }
        (accpeted_transactions, HashMap::new())
    }
}

fn get_shard_for_index(txns_per_shard: usize, index: usize) -> usize {
    index / txns_per_shard
}
