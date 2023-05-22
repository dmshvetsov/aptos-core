// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use aptos_logger::{error, trace};
use aptos_types::transaction::TransactionOutput;
use std::sync::{
    mpsc::{Receiver, Sender},
    Arc,
};
use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use crate::sharded_block_partitioner::conflict_detector::CrossShardConflictDetector;
use crate::sharded_block_partitioner::dependency_analyzer;
use crate::sharded_block_partitioner::dependency_analyzer::DependencyAnalyzer;
use crate::sharded_block_partitioner::messages::{ControlMsg, CrossShardMsg, PartitionBlockMsg, PartitionedBlockResponse, PartitioningStatus};
use crate::sharded_block_partitioner::messages::CrossShardMsg::DiscardedSenders;

/// A remote block executor that receives transactions from a channel and executes them in parallel.
/// Currently it runs in the local machine and it will be further extended to run in a remote machine.
pub struct PartitioningShard{
    shard_id: usize,
    control_rx: Receiver<ControlMsg>,
    result_tx: Sender<PartitionedBlockResponse>,
    message_rx: Receiver<CrossShardMsg>,
    messages_tx: Vec<Sender<CrossShardMsg>>,
}

impl PartitioningShard {
    pub fn new(
        shard_id: usize,
        control_rx: Receiver<ControlMsg>,
        result_tx: Sender<PartitionedBlockResponse>,
        message_rx: Receiver<CrossShardMsg>,
        messages_tx: Vec<Sender<CrossShardMsg>>,
    ) -> Self {
        Self {
            shard_id,
            control_rx,
            result_tx,
            message_rx,
            messages_tx,
        }
    }

    fn partition_block(&self, partition_msg: PartitionBlockMsg) {
        let PartitionBlockMsg{
            transactions,
            index_offset,
        } = partition_msg;
        let num_transactions = transactions.len();
        let num_shards = self.messages_tx.len();
        let dependency_analysis_msg = DependencyAnalyzer::new(
            self.shard_id,
            &transactions,
        ).get_dependency_analysis_msg();

        for i in 0..num_shards {
            if i != self.shard_id {
                self.messages_tx[i].send(CrossShardMsg::DependencyAnalysis(dependency_analysis_msg.clone())).unwrap();
            }
        }
        let mut conflict_detector = CrossShardConflictDetector::new(
            self.shard_id,
            &transactions,
        );
        // Receive the dependency analysis messages from other shards
        for _ in 0..num_shards - 1 {
            let msg = self.message_rx.recv().unwrap();
            match msg {
                CrossShardMsg::DependencyAnalysis(dependency_analysis_msg) => {
                    conflict_detector.add_dependency_analysis(dependency_analysis_msg);
                }
                _ => {
                    panic!("Unexpected message type");
                }
            }
        }
        let discarded_sender_msg = conflict_detector.discard_conflicting_transactions(&transactions);
        // broadcast discarded sender message to all shards
        for i in 0..num_shards {
            if i != self.shard_id {
                self.messages_tx[i].send(DiscardedSenders(discarded_sender_msg.clone())).unwrap();
            }
        }
        // Receive the discarded sender messages from other shards
        for _ in 0..num_shards - 1 {
            let msg = self.message_rx.recv().unwrap();
            match msg {
                DiscardedSenders(discarded_sender_msg) => {
                    conflict_detector.add_discarded_senders(discarded_sender_msg);
                }
                _ => {
                    panic!("Unexpected message type");
                }
            }
        }

        let partitioning_status = conflict_detector.discard_discarded_sender_transactions(&transactions);
        // split the transaction into accepted and discarded statuses
        let mut accepted_txns: Vec<(usize, AnalyzedTransaction)> = Vec::new();
        let mut rejected_txns: Vec<(usize, AnalyzedTransaction)> = Vec::new();
        for (i, txn) in transactions.into_iter().enumerate() {
            if partitioning_status[i] == PartitioningStatus::Accepted {
                accepted_txns.push((index_offset + i, txn));
            } else {
                rejected_txns.push((index_offset + i, txn));
            }
        }

        // send the result back to the controller
        self.result_tx.send(PartitionedBlockResponse::new(
            accepted_txns, rejected_txns)).unwrap();
    }

    pub fn start(&self) {
        loop {
            let command = self.control_rx.recv().unwrap();
            match command {
                ControlMsg::PartitionBlock(msg) => {
                    self.partition_block(msg);
                }
                ControlMsg::Stop => {
                    break;
                }
            }
        }
        trace!("Shard {} is shutting down", self.shard_id);
    }
}
