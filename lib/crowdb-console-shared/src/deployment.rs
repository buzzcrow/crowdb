// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable deployment operations and Group 0 cluster identity.

pub mod admission;
mod bootstrap;
pub mod node_update;
pub mod registry;
pub mod services;

pub use bootstrap::{PreparedBootstrap, CLUSTER_KEY};
