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
mod trust;
pub use single::run_single_node;

pub use runtime::{control_node, run_node, NodeRuntimeConfig};

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
