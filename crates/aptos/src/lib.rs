// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0


pub mod account;
pub mod common;
pub mod config;
pub mod genesis;
pub mod governance;
pub mod move_tool;
pub mod node;
pub mod op;
pub mod stake;
#[cfg(any(test, feature = "fuzzing"))]
pub mod test;
pub mod update;

use crate::common::{
    types::{CliCommand, CliResult, CliTypedResult},
    utils::cli_build_information,
};
use async_trait::async_trait;
use clap::Parser;
use std::collections::BTreeMap;
use std::ffi::CString;
use futures::executor::block_on;
use futures::TryFutureExt;

/// Command Line Interface (CLI) for developing and interacting with the Aptos blockchain
#[derive(Parser)]
#[clap(name = "aptos", author, version, propagate_version = true)]
pub enum Tool {
    #[clap(subcommand)]
    Account(account::AccountTool),
    #[clap(subcommand)]
    Config(config::ConfigTool),
    #[clap(subcommand)]
    Genesis(genesis::GenesisTool),
    #[clap(subcommand)]
    Governance(governance::GovernanceTool),
    Info(InfoTool),
    Init(common::init::InitTool),
    #[clap(subcommand)]
    Key(op::key::KeyTool),
    #[clap(subcommand)]
    Move(move_tool::MoveTool),
    #[clap(subcommand)]
    Multisig(account::MultisigAccountTool),
    #[clap(subcommand)]
    Node(node::NodeTool),
    #[clap(subcommand)]
    Stake(stake::StakeTool),
    Update(update::UpdateTool),
}

impl Tool {
    pub async fn execute(self) -> CliResult {
        use Tool::*;
        match self {
            Account(tool) => tool.execute().await,
            Config(tool) => tool.execute().await,
            Genesis(tool) => tool.execute().await,
            Governance(tool) => tool.execute().await,
            Info(tool) => tool.execute_serialized().await,
            // TODO: Replace entirely with config init
            Init(tool) => tool.execute_serialized_success().await,
            Key(tool) => tool.execute().await,
            Move(tool) => tool.execute().await,
            Multisig(tool) => tool.execute().await,
            Node(tool) => tool.execute().await,
            Stake(tool) => tool.execute().await,
            Update(tool) => tool.execute_serialized().await,
        }
    }
}

/// Show build information about the CLI
///
/// This is useful for debugging as well as determining what versions are compatible with the CLI
#[derive(Parser)]
pub struct InfoTool {}

#[async_trait]
impl CliCommand<BTreeMap<String, String>> for InfoTool {
    fn command_name(&self) -> &'static str {
        "GetCLIInfo"
    }

    async fn execute(self) -> CliTypedResult<BTreeMap<String, String>> {
        Ok(cli_build_information())
    }
}

#[no_mangle]
pub extern "C" fn run_aptos_from_ts() -> *const std::os::raw::c_char {
    println!("Hello from Rust!");
    let result = "testing from Jin";
    let c_str = CString::new(result).unwrap();

    // Return a pointer to the C string
    c_str.into_raw()
}
