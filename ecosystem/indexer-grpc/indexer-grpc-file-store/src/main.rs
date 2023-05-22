// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use aptos_indexer_grpc_file_store::processor::Processor;
use aptos_indexer_grpc_server_framework::{RunnableConfig, ServerArgs};
use aptos_indexer_grpc_utils::config::IndexerGrpcFileStoreConfig;


#[tokio::main]
async fn main() -> Result<()> {
    let args = ServerArgs::parse();
    args.run::<IndexerGrpcFileStoreWorkerConfig>().await
}
