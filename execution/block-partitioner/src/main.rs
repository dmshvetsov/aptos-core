// Copyright © Aptos Foundation

use aptos_block_partitioner::{
    dependency_aware_partitioner::DependencyAwareUniformPartitioner,
    test_utils::{create_signed_p2p_transaction, generate_test_account},
    BlockPartitioner,
};
use rand::{rngs::OsRng, Rng};
use std::{sync::Mutex, time::Instant};
use aptos_block_partitioner::sharded_block_partitioner::ShardedBlockPartitioner;

fn main() {
    println!("Hello, world!");
    let mut rng = OsRng;
    let num_accounts = 1000000;
    let mut accounts = Vec::new();
    for _ in 0..num_accounts {
        accounts.push(Mutex::new(generate_test_account()));
    }
    let num_txns = 100000;
    let mut transactions = Vec::new();
    let num_shards = 16;

    for _ in 0..num_txns {
        // randomly select a sender and receiver from accounts
        let sender_index = rng.gen_range(0, num_accounts);
        let receiver_index = rng.gen_range(0, num_accounts);
        let receiver = accounts[receiver_index].lock().unwrap();
        let mut sender = accounts[sender_index].lock().unwrap();
        transactions.push(create_signed_p2p_transaction(&mut sender, vec![&receiver]).remove(0));
    }
    // profile the time taken
    let partitioner = ShardedBlockPartitioner::new(num_shards);
    for _ in 0..50 {
        println!("Starting to partition");
        let now = Instant::now();
        let (accepted_txns, _) = partitioner.partition(transactions.clone());
        let elapsed = now.elapsed();
        println!("Time taken to partition: {:?}", elapsed);
        println!("Number of accepted transactions: {}", accepted_txns.len());
    }
}
