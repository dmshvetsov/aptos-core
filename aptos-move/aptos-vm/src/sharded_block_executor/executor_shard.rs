// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use crate::{
    block_executor::BlockAptosVM,
    sharded_block_executor::{ExecuteBlockCommand, ExecutorShardCommand},
};
use aptos_logger::{error, trace};
use aptos_state_view::StateView;
use aptos_types::transaction::TransactionOutput;
use move_core_types::vm_status::VMStatus;
use std::sync::{
    mpsc::{Receiver, Sender},
    Arc,
};

/// A remote block executor that receives transactions from a channel and executes them in parallel.
/// Currently it runs in the local machine and it will be further extended to run in a remote machine.
pub struct ExecutorShard<S: StateView + Sync + Send + 'static> {
    shard_id: usize,
    executor_thread_pool: Arc<rayon::ThreadPool>,
    command_rx: Receiver<ExecutorShardCommand<S>>,
    result_tx: Sender<Result<Vec<TransactionOutput>, VMStatus>>,
    maybe_gas_limit: Option<u64>,
}

impl<S: StateView + Sync + Send + 'static> ExecutorShard<S> {
    pub fn new(
        shard_id: usize,
        num_executor_threads: usize,
        command_rx: Receiver<ExecutorShardCommand<S>>,
        result_tx: Sender<Result<Vec<TransactionOutput>, VMStatus>>,
        maybe_gas_limit: Option<u64>,
    ) -> Self {
        let executor_thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(num_executor_threads)
                .build()
                .unwrap(),
        );
        Self {
            shard_id,
            executor_thread_pool,
            command_rx,
            result_tx,
            maybe_gas_limit,
        }
    }

    fn execute_block(
        &self,
        command: ExecuteBlockCommand<S>,
    ) -> Result<Vec<TransactionOutput>, VMStatus> {
        let ExecuteBlockCommand {
            state_view,
            accepted_transactions,
            accepted_transaction_indices,
            rejected_transaction_indices,
            concurrency_level_per_shard,
        } = command;
        let ret = BlockAptosVM::execute_block(
            self.executor_thread_pool.clone(),
            accepted_transactions,
            state_view.as_ref(),
            concurrency_level_per_shard,
            self.maybe_gas_limit,
        );
        let outputs = match ret {
            Ok(outputs) => outputs,
            Err(err) => {
                error!("Error executing block: {:?}", err);
                return Err(err);
            },
        };
        println!(
            "accepted_transaction_indices length is : {:?}",
            accepted_transaction_indices.len()
        );
        println!(
            "rejected_transaction_indices length is : {:?}",
            rejected_transaction_indices.len()
        );

        let mut ordered_outputs = vec![
            TransactionOutput::retried();
            accepted_transaction_indices.len()
                + rejected_transaction_indices.len()
        ];

        let mut index = 0;
        let mut accepted_index = 0;
        let mut rejected_index = 0;
        let mut outout_iter = outputs.into_iter();

        while index < ordered_outputs.len() {
            if accepted_index < accepted_transaction_indices.len()
                && rejected_index < rejected_transaction_indices.len()
            {
                if accepted_transaction_indices[accepted_index]
                    < rejected_transaction_indices[rejected_index]
                {
                    ordered_outputs[index] = outout_iter.next().unwrap();
                    accepted_index += 1;
                } else {
                    ordered_outputs[index] = TransactionOutput::retried();
                    rejected_index += 1;
                }
            } else if accepted_index >= accepted_transaction_indices.len() {
                ordered_outputs[index] = TransactionOutput::retried();
                rejected_index += 1;
            } else if rejected_index >= rejected_transaction_indices.len() {
                ordered_outputs[index] = outout_iter.next().unwrap();
                accepted_index += 1;
            }
            index += 1;
        }

        drop(state_view);
        Ok(ordered_outputs)
    }

    pub fn start(&self) {
        loop {
            let command = self.command_rx.recv().unwrap();
            match command {
                ExecutorShardCommand::ExecuteBlock(command) => {
                    let result = self.execute_block(command);
                    self.result_tx.send(result).unwrap();
                },
                ExecutorShardCommand::Stop => {
                    break;
                },
            }
        }
        trace!("Shard {} is shutting down", self.shard_id);
    }
}
