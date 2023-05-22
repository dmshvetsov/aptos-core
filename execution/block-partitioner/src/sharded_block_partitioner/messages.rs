// Copyright © Aptos Foundation

use aptos_types::transaction::analyzed_transaction::{AnalyzedTransaction, StorageLocation};
use move_core_types::account_address::AccountAddress;
use std::{collections::HashSet, sync::Arc};

pub enum ControlMsg {
    PartitionBlock(PartitionBlockMsg),
    Stop,
}

#[derive(Clone, Debug)]
pub enum CrossShardMsg {
    DependencyAnalysis(DependencyAnalysisMsg),
    DiscardedSenders(DiscardedSendersMsg),
}

pub struct PartitionBlockMsg {
    pub transactions: Vec<AnalyzedTransaction>,
    pub index_offset: usize,
}

impl PartitionBlockMsg {
    pub fn new(transactions: Vec<AnalyzedTransaction>, index_offset: usize) -> Self {
        Self {
            transactions,
            index_offset,
        }
    }
}

pub struct PartitionedBlockResponse {
    pub accepted_txns: Vec<(usize, AnalyzedTransaction)>,
    pub rejected_txns: Vec<(usize, AnalyzedTransaction)>,
}

impl PartitionedBlockResponse {
    pub fn new(
        accepted_txns: Vec<(usize, AnalyzedTransaction)>,
        rejected_txns: Vec<(usize, AnalyzedTransaction)>,
    ) -> Self {
        Self {
            accepted_txns,
            rejected_txns,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct DependencyAnalysisMsg {
    pub source_shard_id: usize,
    pub exclusive_read_set: Arc<HashSet<StorageLocation>>,
    pub write_set: Arc<HashSet<StorageLocation>>,
}

impl DependencyAnalysisMsg {
    pub fn new(
        source_shard_id: usize,
        exclusive_read_set: Arc<HashSet<StorageLocation>>,
        write_set: Arc<HashSet<StorageLocation>>,
    ) -> Self {
        Self {
            source_shard_id,
            exclusive_read_set,
            write_set,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct DiscardedSendersMsg {
    pub source_shard_id: usize,
    pub discarded_senders: Arc<HashSet<AccountAddress>>,
}

impl DiscardedSendersMsg {
    pub fn new(source_shard_id: usize, discarded_senders: Arc<HashSet<AccountAddress>>) -> Self {
        Self {
            source_shard_id,
            discarded_senders,
        }
    }
}

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum PartitioningStatus {
    // Transaction is accepted after partitioning.
    Accepted,
    // Transaction is discarded due to creating cross-shard dependency.
    Discarded,
}
