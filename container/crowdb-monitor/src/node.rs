// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persistent identity and management lifecycle for independently deployed nodes.

mod candidates;
mod discovery;
mod durable;
mod identity;
mod management;
mod runtime;
mod single;
pub(crate) mod startup;
mod storage;
#[cfg(feature = "test-util")]
pub use storage::blank::verify_blank_medium_for_tests;
pub use storage::discover_disks;
#[cfg(feature = "test-util")]
pub use storage::file_operations::file_operations_for_tests;
#[cfg(feature = "test-util")]
pub use storage::file_preparation::{accept_file_for_tests, prepare_file_for_tests};
#[cfg(feature = "test-util")]
pub use storage::retained::check_retained_medium_for_tests;
#[cfg(feature = "test-util")]
pub use storage::supplied::SuppliedOperationsForTests;
mod trust;
pub use single::run_single_node;

#[cfg(feature = "test-util")]
pub use runtime::prepare_supplied_disk_for_tests;
#[cfg(feature = "test-util")]
pub use runtime::publish_file_for_tests;
#[cfg(feature = "test-util")]
pub use runtime::InitializationForTests;
#[cfg(feature = "test-util")]
pub use runtime::NodeSetupForTests;
pub use runtime::{control_node, run_node, run_node_with_inputs, NodeRuntimeConfig};
#[cfg(feature = "test-util")]
pub use startup::accept_startup_mode_for_tests;
pub use startup::{accept_node_policy, read_node_policy};

pub use management::{serve_node_management, NodeManagementError};

pub use candidates::CandidateCache;
pub use discovery::{DiscoveryConfig, DiscoveryError, NodeDiscovery, NODE_SERVICE_TYPE};

pub use identity::{NodeIdentity, NodeIdentityError};

pub(crate) fn write_single_binding(
    root: &std::path::Path,
    binding: &crowdb_protocol::mgmt::node::NodeBinding,
) -> std::io::Result<()> {
    durable::write(&root.join("node-binding.json"), binding)
}

#[cfg(feature = "test-util")]
pub use runtime::NodeServiceForTests;
