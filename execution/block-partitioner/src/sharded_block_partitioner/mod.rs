// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::sharded_block_partitioner::{
    messages::{ControlMsg, CrossShardMsg, PartitionBlockMsg, PartitionedBlockResponse},
    partitioning_shard::PartitioningShard,
};
use aptos_logger::{error, info};
use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use std::{
    collections::HashMap,
    sync::mpsc::{Receiver, Sender},
    thread,
    time::Instant,
};

mod conflict_detector;
pub mod dependency_analyzer;
mod messages;
mod partitioning_shard;

pub struct ShardedBlockPartitioner {
    num_shards: usize,
    control_txs: Vec<Sender<ControlMsg>>,
    result_rxs: Vec<Receiver<PartitionedBlockResponse>>,
    shard_threads: Vec<thread::JoinHandle<()>>,
}

impl ShardedBlockPartitioner {
    pub fn new(num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_executor_shards must be > 0");
        // create channels for cross shard messages across all shards. This is a full mesh connection.
        // Each shard has a vector of channels for sending messages to other shards and
        // a vector of channels for receiving messages from other shards.
        let mut messages_txs = vec![];
        let mut messages_rxs = vec![];
        for _ in 0..num_shards {
            messages_txs.push(vec![]);
            messages_rxs.push(vec![]);
            for _ in 0..num_shards {
                let (messages_tx, messages_rx) = std::sync::mpsc::channel();
                messages_txs.last_mut().unwrap().push(messages_tx);
                messages_rxs.last_mut().unwrap().push(messages_rx);
            }
        }
        let mut control_txs = vec![];
        let mut result_rxs = vec![];
        let mut shard_join_handles = vec![];
        for (i, message_rxs) in messages_rxs.into_iter().enumerate() {
            let (control_tx, control_rx) = std::sync::mpsc::channel();
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            control_txs.push(control_tx);
            result_rxs.push(result_rx);
            shard_join_handles.push(spawn_partitioning_shard(
                i,
                control_rx,
                result_tx,
                message_rxs,
                messages_txs.iter().map(|txs| txs[i].clone()).collect(),
            ));
        }
        Self {
            num_shards,
            control_txs,
            result_rxs,
            shard_threads: shard_join_handles,
        }
    }

    pub fn partition(
        &self,
        transactions: Vec<AnalyzedTransaction>,
    ) -> (
        HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
        HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
    ) {
        let now = Instant::now();
        let total_txns = transactions.len();
        let txns_per_shard = (total_txns as f64 / self.num_shards as f64).ceil() as usize;

        let mut partitioned_txns = Vec::new();
        for chunk in transactions.chunks(txns_per_shard) {
            partitioned_txns.push(chunk.to_vec());
        }

        let current_shards = partitioned_txns.len();

        let mut index_offset = 0;
        for (shard_id, analyzed_txns) in partitioned_txns.into_iter().enumerate() {
            let block_size = analyzed_txns.len();
            let partitioning_msg = PartitionBlockMsg::new(analyzed_txns, index_offset);
            index_offset += block_size;
            if let Err(e) =
                self.control_txs[shard_id].send(ControlMsg::PartitionBlock(partitioning_msg))
            {
                error!(
                    "Failed to send partitioning message to executor shard: {:?}",
                    e
                );
            }
        }

        // pre-poluate the hashmap with empty vectors for both accepted and rejected transactions
        // for each shard.
        let mut accpeted_txns: HashMap<usize, Vec<(usize, AnalyzedTransaction)>> = HashMap::new();
        let mut rejected_txns: HashMap<usize, Vec<(usize, AnalyzedTransaction)>> = HashMap::new();
        for i in 0..self.num_shards {
            accpeted_txns.insert(i, Vec::new());
            rejected_txns.insert(i, Vec::new());
        }

        // wait for all remote shards to send the partitioned transactions back
        for shard_id in 0..current_shards {
            let result = self.result_rxs[shard_id].recv().unwrap();
            for (index, accepted_txn) in result.accepted_txns.into_iter() {
                accpeted_txns
                    .get_mut(&shard_id)
                    .unwrap()
                    .push((index, accepted_txn));
            }
            for (index, rejected_txn) in result.rejected_txns.into_iter() {
                rejected_txns
                    .get_mut(&shard_id)
                    .unwrap()
                    .push((index, rejected_txn));
            }
        }

        info!("Receiving Partitioning Messages took: {:?}", now.elapsed());

        (accpeted_txns, rejected_txns)
    }
}

impl Drop for ShardedBlockPartitioner {
    /// Best effort stops all the executor shards and waits for the thread to finish.
    fn drop(&mut self) {
        // send stop command to all executor shards
        for control_tx in self.control_txs.iter() {
            if let Err(e) = control_tx.send(ControlMsg::Stop) {
                error!("Failed to send stop command to executor shard: {:?}", e);
            }
        }

        // wait for all executor shards to stop
        for shard_thread in self.shard_threads.drain(..) {
            shard_thread.join().unwrap_or_else(|e| {
                error!("Failed to join executor shard thread: {:?}", e);
            });
        }
    }
}

fn spawn_partitioning_shard(
    shard_id: usize,
    control_rx: Receiver<ControlMsg>,
    result_tx: Sender<PartitionedBlockResponse>,
    message_rxs: Vec<Receiver<CrossShardMsg>>,
    messages_txs: Vec<Sender<CrossShardMsg>>,
) -> thread::JoinHandle<()> {
    // create and start a new executor shard in a separate thread
    thread::Builder::new()
        .name(format!("partitioning-shard-{}", shard_id))
        .spawn(move || {
            let partitioning_shard =
                PartitioningShard::new(shard_id, control_rx, result_tx, message_rxs, messages_txs);
            partitioning_shard.start();
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    use crate::{
        get_shard_for_index,
        sharded_block_partitioner::{messages::PartitioningStatus, ShardedBlockPartitioner},
        test_utils::{create_signed_p2p_transaction, generate_test_account, TestAccount},
    };
    use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
    use std::collections::HashMap;

    fn verify_txn_statuses(
        txn_statuses: &HashMap<usize, PartitioningStatus>,
        expected_txn_statuses: &HashMap<usize, PartitioningStatus>,
    ) {
        assert_eq!(txn_statuses.len(), expected_txn_statuses.len());
        for (index, status) in txn_statuses {
            assert_eq!(status, expected_txn_statuses.get(index).unwrap());
        }
    }

    fn verify_txn_shards(
        orig_txns: &Vec<AnalyzedTransaction>,
        accepted_txns: &HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
        rejected_txns: &HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
        num_shards: usize,
    ) {
        // create a map of transaction to its shard index.
        let mut txn_shard_map = HashMap::new();
        for (shard_index, txns) in accepted_txns {
            for (_, txn) in txns {
                txn_shard_map.insert(txn, *shard_index);
            }
        }
        for (shard_index, txns) in rejected_txns {
            for (_, txn) in txns {
                txn_shard_map.insert(txn, *shard_index);
            }
        }
        let txns_per_shard = (orig_txns.len() as f64 / num_shards as f64).ceil() as usize;
        // verify that all the transactions are present in the map.
        assert_eq!(txn_shard_map.len(), orig_txns.len());
        for (index, txn) in orig_txns.iter().enumerate() {
            assert_eq!(
                get_shard_for_index(txns_per_shard, index),
                *txn_shard_map.get(txn).unwrap()
            );
        }
    }

    fn populate_txn_statuses(
        txns_map: &HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
        txn_statuses: &mut HashMap<usize, PartitioningStatus>,
        status: PartitioningStatus,
    ) {
        for txns in txns_map.values() {
            for (index, _) in txns {
                txn_statuses.insert(*index, status);
            }
        }
    }

    fn verify_txn_statuses_and_shards(
        orig_txns: &Vec<AnalyzedTransaction>,
        accepted_txns: &HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
        rejected_txns: &HashMap<usize, Vec<(usize, AnalyzedTransaction)>>,
        expected_txn_statuses: &HashMap<usize, PartitioningStatus>,
        num_shards: usize,
    ) {
        let mut txn_statuses = HashMap::new();

        populate_txn_statuses(
            accepted_txns,
            &mut txn_statuses,
            PartitioningStatus::Accepted,
        );

        populate_txn_statuses(
            rejected_txns,
            &mut txn_statuses,
            PartitioningStatus::Discarded,
        );

        verify_txn_statuses(&txn_statuses, expected_txn_statuses);
        verify_txn_shards(orig_txns, accepted_txns, rejected_txns, num_shards);
    }

    #[test]
    // Test that the partitioner works correctly for a single sender and multiple receivers.
    // In this case the expectation is that only the first shard will contain transactions and all
    // other shards will be empty.
    fn test_single_sender_single_receiver_txns() {
        let mut sender = generate_test_account();
        let receiver = generate_test_account();
        let num_txns = 10;
        let mut transactions = Vec::new();
        for _ in 0..num_txns {
            transactions
                .push(create_signed_p2p_transaction(&mut sender, vec![&receiver]).remove(0));
        }
        let partitioner = ShardedBlockPartitioner::new(4);
        let (accepted_txns, rejected_txns) = partitioner.partition(transactions.clone());
        let mut expected_txn_statuses = HashMap::new();
        for index in 0..num_txns {
            if index < 3 {
                expected_txn_statuses.insert(index, PartitioningStatus::Accepted);
            } else {
                expected_txn_statuses.insert(index, PartitioningStatus::Discarded);
            }
        }
        verify_txn_statuses_and_shards(
            &transactions,
            &accepted_txns,
            &rejected_txns,
            &expected_txn_statuses,
            4,
        );
    }

    #[test]
    // Test that the partitioner works correctly for a single sender and multiple receivers.
    // In this case the expectation is that only the first shard will contain transactions and all
    // other shards will be empty.
    fn test_single_sender_txns() {
        let mut sender = generate_test_account();
        let mut receivers = Vec::new();
        let num_txns = 10;
        for _ in 0..num_txns {
            receivers.push(generate_test_account());
        }
        let transactions = create_signed_p2p_transaction(
            &mut sender,
            receivers.iter().collect::<Vec<&TestAccount>>(),
        );
        let partitioner = ShardedBlockPartitioner::new(4);
        let (accepted_txns, rejected_txns) = partitioner.partition(transactions.clone());
        // Create a map of transaction index to its expected status, first 3 transactions are expected to be accepted
        // and the rest are expected to be rejected.
        // println!("Accepted txns: {:?}", accepted_txns);
        // println!("Rejected txns: {:?}", rejected_txns);
        let mut expected_txn_statuses = HashMap::new();
        for index in 0..num_txns {
            if index < 3 {
                expected_txn_statuses.insert(index, PartitioningStatus::Accepted);
            } else {
                expected_txn_statuses.insert(index, PartitioningStatus::Discarded);
            }
        }
        verify_txn_statuses_and_shards(
            &transactions,
            &accepted_txns,
            &rejected_txns,
            &expected_txn_statuses,
            4,
        );
    }
}
