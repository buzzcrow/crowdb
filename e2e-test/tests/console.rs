// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod discovery;
mod fixture;
mod group_publication;
mod hardware;
mod logical_publication;

#[path = "console/common/rpc_response_proxy.rs"]
mod rpc_response_proxy;

#[path = "console/common/bootstrap_authority.rs"]
mod bootstrap_authority;
mod bootstrap_intent;
mod bootstrap_publication;
mod cli_data;
mod cli_direct;
mod cli_fixture;
mod cli_hardware;
mod cli_management;
mod cli_observation;
mod cli_registry;
mod cluster;
mod deployment;
mod logical_delete;
mod logical_fanout;
mod replica_cleanup;
mod server_data;
mod server_management;
mod web_authority;
