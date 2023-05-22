// Copyright © Aptos Foundation

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use aptos_types::transaction::analyzed_transaction::{AnalyzedTransaction, StorageLocation};
use aptos_types::transaction::Transaction;
use move_core_types::account_address::AccountAddress;
use crate::sharded_block_partitioner::messages::{DependencyAnalysisMsg, DiscardedSendersMsg, PartitioningStatus};

pub struct CrossShardConflictDetector {
    shard_id: usize,
    // set of shards we have received dependency analysis from
    received_dependency_analysis_from: HashSet<usize>,
    // minimum shard id that has taken read lock on the storage location
    read_set: HashMap<StorageLocation, usize>,
    // minimum shard id that has taken write lock on the storage location
    write_set: HashMap<StorageLocation, usize>,
    // transaction partitioning status
    partitioning_status: Vec<PartitioningStatus>,
    // set of shards we have received discarded senders from
    received_discarded_senders_from: HashSet<usize>,
    // minimum shard id that has discarded this sender
    discarded_senders: HashMap<AccountAddress, usize>,
}

impl CrossShardConflictDetector {
    pub fn new(shard_id: usize, txns: &[AnalyzedTransaction]) -> Self {
        // let mut read_set: HashMap<StorageLocation, usize> = HashMap::new();
        // let mut write_set: HashMap<StorageLocation, usize> = HashMap::new();
        // for analyzed_txn in txns.iter() {
        //     for storage_location in analyzed_txn.read_hints().iter() {
        //         read_set.insert(storage_location.clone(), shard_id);
        //     }
        //     for storage_location in analyzed_txn.write_hints().iter() {
        //         write_set.insert(storage_location.clone(), shard_id);
        //     }
        // }
        Self {
            shard_id,
            received_dependency_analysis_from: HashSet::new(),
            read_set:HashMap::new(),
            write_set: HashMap::new(),
            partitioning_status: vec![PartitioningStatus::Accepted; txns.len()],
            received_discarded_senders_from: HashSet::new(),
            discarded_senders: HashMap::new(),
        }
    }

    pub fn add_dependency_analysis(&mut self, dependency_analysis_msg: DependencyAnalysisMsg) {
        assert_ne!(dependency_analysis_msg.source_shard_id, self.shard_id);
        assert!(self.received_dependency_analysis_from.insert(dependency_analysis_msg.source_shard_id));
        let source_shard_id = dependency_analysis_msg.source_shard_id;
        for storage_location in dependency_analysis_msg.read_set.iter() {
            // set the shard id to the minimum of the current shard id and the shard id that has taken read lock
            // if the shard id doesn't exists, set it to the current shard id
            let shard_id = self.read_set.entry(storage_location.clone()).or_insert(source_shard_id);
            *shard_id = std::cmp::min(*shard_id, source_shard_id);
            //
            // if self.shard_id == 3 {
            //     println!("Updated min shard_id to {:?} for shard_id 3 and for location {:?}", *shard_id, storage_location);
            // }
        }
        for storage_location in dependency_analysis_msg.write_set.iter() {
            // set the shard id to the minimum of the current shard id and the shard id that has taken write lock
            // if the shard id doesn't exists, set it to the current shard id
            let shard_id = self.write_set.entry(storage_location.clone()).or_insert(source_shard_id);
            *shard_id = std::cmp::min(*shard_id, source_shard_id);
        }
    }

    pub fn add_discarded_senders(&mut self, discarded_senders_msg: DiscardedSendersMsg) {
        assert_ne!(discarded_senders_msg.source_shard_id, self.shard_id);
        assert!(self.received_discarded_senders_from.insert(discarded_senders_msg.source_shard_id));
        let source_shard_id = discarded_senders_msg.source_shard_id;
        for sender in discarded_senders_msg.discarded_senders.iter() {
            // set the shard id to the minimum of the current shard id and the shard id that has discarded the sender
            // if the shard id doesn't exists, set it to the current shard id
            let shard_id = self.discarded_senders.entry(sender.clone()).or_insert(source_shard_id);
            *shard_id = std::cmp::min(*shard_id, source_shard_id);
        }
    }

    pub fn discard_conflicting_transactions(&mut self, analyzed_transactions: &[AnalyzedTransaction]) -> DiscardedSendersMsg {
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
            if self.check_for_read_conflict(self.shard_id, txn) {
                self.partitioning_status[index] = PartitioningStatus::Discarded;
                if let Some(sender) = txn.get_sender() {
                    discarded_senders.insert(sender);
                }
                continue;
            }
            if self.check_for_write_conflict(self.shard_id, txn) {
                self.partitioning_status[index] = PartitioningStatus::Discarded;
                if let Some(sender) = txn.get_sender() {
                    discarded_senders.insert(sender);
                }
            }
        }
        DiscardedSendersMsg::new(self.shard_id, Arc::new(discarded_senders))
    }

    pub(crate) fn discard_discarded_sender_transactions(&mut self, analyzed_transactions: &[AnalyzedTransaction]) -> &[PartitioningStatus] {
        // Iterate through all the transactions and if any shard has discarded the sender
        // and has a smaller shard id than the current shard id, discard the transaction
        for (i, txn) in analyzed_transactions.iter().enumerate() {
            if let Some(sender) = txn.get_sender() {
                if let Some(conflicting_shard_id) = self.discarded_senders.get(&sender) {
                    if *conflicting_shard_id < self.shard_id {
                        self.partitioning_status[i] = PartitioningStatus::Discarded;
                    }
                }
            }
        }

        &self.partitioning_status
    }

    fn check_for_read_conflict(&self, current_shard_id: usize,  txn: &AnalyzedTransaction) -> bool {
        for read_location in txn.read_hints().iter() {
            if let Some(conflicting_shard_id) = self.write_set.get(read_location) {
                if *conflicting_shard_id < current_shard_id {
                    return true;
                }
            }
        }
        false
    }

    fn check_for_write_conflict(&self, current_shard_id: usize, txn: &AnalyzedTransaction) -> bool {
        for write_location in txn.write_hints().iter() {
            if let Some(conflicting_shard_id) = self.read_set.get(write_location) {
                if *conflicting_shard_id < current_shard_id {
                    return true;
                }
            }
            if let Some(conflicting_shard_id) = self.write_set.get(write_location) {
                if *conflicting_shard_id < current_shard_id {
                    return true;
                }
            }
        }
        false
    }
}
