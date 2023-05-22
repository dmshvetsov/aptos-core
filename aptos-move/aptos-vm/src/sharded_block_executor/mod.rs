// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::sharded_block_executor::executor_shard::ExecutorShard;
use aptos_block_partitioner::{BlockPartitioner, UniformPartitioner};
use aptos_logger::{error, info, trace};
use aptos_state_view::StateView;
use aptos_types::transaction::{
    analyzed_transaction::AnalyzedTransaction, Transaction, TransactionOutput,
};
use move_core_types::vm_status::VMStatus;
use std::{
    marker::PhantomData,
    sync::{
        mpsc::{Receiver, Sender},
        Arc,
    },
    thread,
};

mod executor_shard;
/// A wrapper around sharded block executors that manages multiple shards and aggregates the results.
pub struct ShardedBlockExecutor<S: StateView + Sync + Send + 'static> {
    num_executor_shards: usize,
    partitioner: Arc<dyn BlockPartitioner>,
    command_txs: Vec<Sender<ExecutorShardCommand<S>>>,
    shard_threads: Vec<thread::JoinHandle<()>>,
    result_rxs: Vec<Receiver<Result<Vec<TransactionOutput>, VMStatus>>>,
    phantom: PhantomData<S>,
}

pub struct ExecuteBlockCommand<S: StateView + Sync + Send + 'static> {
    state_view: Arc<S>,
    accepted_transactions: Vec<Transaction>,
    accepted_transaction_indices: Vec<usize>,
    rejected_transaction_indices: Vec<usize>,
    concurrency_level_per_shard: usize,
}

impl<S: StateView + Sync + Send + 'static> ExecuteBlockCommand<S> {
    pub fn new(
        state_view: Arc<S>,
        accepted_transactions: Vec<Transaction>,
        accepted_transaction_indices: Vec<usize>,
        rejected_transaction_indices: Vec<usize>,
        concurrency_level_per_shard: usize,
    ) -> Self {
        Self {
            state_view,
            accepted_transactions,
            accepted_transaction_indices,
            rejected_transaction_indices,
            concurrency_level_per_shard,
        }
    }
}

pub enum ExecutorShardCommand<S: StateView + Sync + Send + 'static> {
    ExecuteBlock(ExecuteBlockCommand<S>),
    Stop,
}

impl<S: StateView + Sync + Send + 'static> ShardedBlockExecutor<S> {
    pub fn new(
        num_executor_shards: usize,
        executor_threads_per_shard: Option<usize>,
        maybe_gas_limit: Option<u64>,
    ) -> Self {
        assert!(num_executor_shards > 0, "num_executor_shards must be > 0");
        let executor_threads_per_shard = executor_threads_per_shard.unwrap_or_else(|| {
            (num_cpus::get() as f64 / num_executor_shards as f64).ceil() as usize
        });
        let mut command_txs = vec![];
        let mut result_rxs = vec![];
        let mut shard_join_handles = vec![];
        for i in 0..num_executor_shards {
            let (transactions_tx, transactions_rx) = std::sync::mpsc::channel();
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            command_txs.push(transactions_tx);
            result_rxs.push(result_rx);
            shard_join_handles.push(spawn_executor_shard(
                i,
                executor_threads_per_shard,
                transactions_rx,
                result_tx,
                maybe_gas_limit,
            ));
        }
        info!(
            "Creating a new ShardedBlockExecutor with {} shards and concurrency per shard {}",
            num_executor_shards, executor_threads_per_shard
        );
        Self {
            num_executor_shards,
            partitioner: Arc::new(UniformPartitioner {}),
            command_txs,
            shard_threads: shard_join_handles,
            result_rxs,
            phantom: PhantomData,
        }
    }

    /// Execute a block of transactions in parallel by splitting the block into num_remote_executors partitions and
    /// dispatching each partition to a remote executor shard.
    pub fn execute_block(
        &self,
        state_view: Arc<S>,
        block: Vec<AnalyzedTransaction>,
        concurrency_level_per_shard: usize,
    ) -> Result<Vec<TransactionOutput>, VMStatus> {
        let block_size = block.len();
        let (shard_to_accepted_txns, mut shard_to_rejected_txns) =
            self.partitioner.partition(block, self.num_executor_shards);
        let num_partitions = shard_to_accepted_txns.len();
        for (shard_index, accepted_txns) in shard_to_accepted_txns.into_iter() {
            let (accepted_txn_indices, accepted_txns) = accepted_txns
                .into_iter()
                .map(|(i, t)| (i, t.into_inner()))
                .unzip();

            let (rejected_txn_indices, _): (Vec<usize>, Vec<AnalyzedTransaction>) =
                shard_to_rejected_txns
                    .remove(&shard_index)
                    .unwrap()
                    .into_iter()
                    .unzip();
            let execute_block_command = ExecuteBlockCommand::new(
                state_view.clone(),
                accepted_txns,
                accepted_txn_indices,
                rejected_txn_indices,
                concurrency_level_per_shard,
            );
            self.command_txs[shard_index]
                .send(ExecutorShardCommand::ExecuteBlock(execute_block_command))
                .unwrap();
        }
        // wait for all remote executors to send the result back and append them in order by shard id
        let mut aggregated_results = vec![];
        trace!("ShardedBlockExecutor Waiting for results");
        for i in 0..num_partitions {
            let result = self.result_rxs[i].recv().unwrap();
            aggregated_results.extend(result?);
        }
        assert!(
            aggregated_results.len() == block_size,
            "ShardedBlockExecutor: expected {} outputs but got {}",
            block_size,
            aggregated_results.len()
        );
        Ok(aggregated_results)
    }
}

impl<S: StateView + Sync + Send + 'static> Drop for ShardedBlockExecutor<S> {
    /// Best effort stops all the executor shards and waits for the thread to finish.
    fn drop(&mut self) {
        // send stop command to all executor shards
        for command_tx in self.command_txs.iter() {
            if let Err(e) = command_tx.send(ExecutorShardCommand::Stop) {
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

fn spawn_executor_shard<S: StateView + Sync + Send + 'static>(
    shard_id: usize,
    concurrency_level: usize,
    command_rx: Receiver<ExecutorShardCommand<S>>,
    result_tx: Sender<Result<Vec<TransactionOutput>, VMStatus>>,
    maybe_gas_limit: Option<u64>,
) -> thread::JoinHandle<()> {
    // create and start a new executor shard in a separate thread
    thread::Builder::new()
        .name(format!("executor-shard-{}", shard_id))
        .spawn(move || {
            let executor_shard = ExecutorShard::new(
                shard_id,
                concurrency_level,
                command_rx,
                result_tx,
                maybe_gas_limit,
            );
            executor_shard.start();
        })
        .unwrap()
}
