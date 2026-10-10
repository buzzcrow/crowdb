// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persistent identity and management lifecycle for independently deployed nodes.

mod candidates;
mod discovery;
mod identity;
mod management;

pub use management::{serve_node_management, NodeManagementError};

pub use candidates::CandidateCache;
pub use discovery::{DiscoveryConfig, DiscoveryError, NodeDiscovery, NODE_SERVICE_TYPE};

pub use identity::{NodeIdentity, NodeIdentityError};
