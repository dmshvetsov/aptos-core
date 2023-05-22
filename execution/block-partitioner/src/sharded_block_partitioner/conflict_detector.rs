// Copyright © Aptos Foundation

use crate::sharded_block_partitioner::messages::{
    DependencyAnalysisMsg, DiscardedSendersMsg, PartitioningStatus,
};
use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use std::{collections::HashSet, sync::Arc};

pub struct CrossShardConflictDetector {
    shard_id: usize,
    // transaction partitioning status
    partitioning_status: Vec<PartitioningStatus>,
}

impl CrossShardConflictDetector {
    pub fn new(shard_id: usize, txns: &[AnalyzedTransaction]) -> Self {
        Self {
            shard_id,
            partitioning_status: vec![PartitioningStatus::Accepted; txns.len()],
        }
    }

    pub fn discard_conflicting_transactions(
        &mut self,
        analyzed_transactions: &[AnalyzedTransaction],
        cross_shard_dependencies: &[DependencyAnalysisMsg],
    ) -> DiscardedSendersMsg {
        // Iterate through all the transactions and if any shard has taken read/write lock on the storage location
        // and has a smaller shard id than the current shard id, discard the transaction
        let mut discarded_senders = HashSet::new();
        // for (storage_location, min_shard_id) in self.read_set.iter() {
        //     println!("readset: min shard is  {:?} for shard id : {:?}",  min_shard_id, self.shard_id);
        // }
        // for (storage_location, min_shard_id) in self.write_set.iter() {
        //     println!("writeset: min shard is  {:?} for shard id : {:?}",  min_shard_id, self.shard_id);
        // }
        for (index, txn) in analyzed_transactions.iter().enumerate() {
            if self.check_for_write_conflict(self.shard_id, txn, cross_shard_dependencies) {
                self.partitioning_status[index] = PartitioningStatus::Discarded;
                if let Some(sender) = txn.get_sender() {
                    discarded_senders.insert(sender);
                }
            }
            if self.check_for_read_conflict(self.shard_id, txn, cross_shard_dependencies) {
                self.partitioning_status[index] = PartitioningStatus::Discarded;
                if let Some(sender) = txn.get_sender() {
                    discarded_senders.insert(sender);
                }
                continue;
            }
        }
        DiscardedSendersMsg::new(self.shard_id, Arc::new(discarded_senders))
    }

    pub(crate) fn discard_discarded_sender_transactions(
        &mut self,
        analyzed_transactions: &[AnalyzedTransaction],
        cross_shard_discarded_senders: &[DiscardedSendersMsg],
    ) -> &[PartitioningStatus] {
        // Iterate through all the transactions and if any shard has discarded the sender
        // and has a smaller shard id than the current shard id, discard the transaction
        for (i, txn) in analyzed_transactions.iter().enumerate() {
            if let Some(sender) = txn.get_sender() {
                for (shard_id, discarded_senders) in
                    cross_shard_discarded_senders.iter().enumerate()
                {
                    // Ignore if this is from the same shard
                    if shard_id == self.shard_id {
                        // We only need to check if any shard id < current shard id has taken a write lock on the storage location
                        break;
                    }
                    if discarded_senders.discarded_senders.contains(&sender) {
                        self.partitioning_status[i] = PartitioningStatus::Discarded;
                        break;
                    }
                }
            }
        }
        &self.partitioning_status
    }

    fn check_for_read_conflict(
        &self,
        current_shard_id: usize,
        txn: &AnalyzedTransaction,
        cross_shard_dependencies: &[DependencyAnalysisMsg],
    ) -> bool {
        for read_location in txn.read_hints().iter() {
            for (shard_id, dependency) in cross_shard_dependencies.iter().enumerate() {
                // Ignore if this is from the same shard
                if shard_id == current_shard_id {
                    // We only need to check if any shard id < current shard id has taken a write lock on the storage location
                    break;
                }
                if dependency.write_set.contains(read_location) {
                    return true;
                }
            }
        }
        false
    }

    fn check_for_write_conflict(
        &self,
        current_shard_id: usize,
        txn: &AnalyzedTransaction,
        cross_shard_dependencies: &[DependencyAnalysisMsg],
    ) -> bool {
        for write_location in txn.write_hints().iter() {
            for (shard_id, dependency) in cross_shard_dependencies.iter().enumerate() {
                // Ignore if this is from the same shard
                if shard_id == current_shard_id {
                    // We only need to check if any shard id < current shard id has taken a write lock on the storage location
                    break;
                }
                if dependency.exclusive_read_set.contains(write_location) {
                    return true;
                }
                if dependency.write_set.contains(write_location) {
                    return true;
                }
            }
        }
        false
    }
}
