// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Connection routes never change durable membership or voting configuration.

use super::group::PxGroup;
use std::{collections::BTreeMap, net::SocketAddr};

impl PxGroup {
    /// Apply a complete verified route view to existing remote identities.
    /// Missing routes block outgoing calls rather than reusing an unverified IP.
    pub fn set_recovery_routes(&self, routes: &BTreeMap<u64, SocketAddr>) {
        for remote in &self.remote_replicas {
            if let Some(remote) = remote.as_real() {
                remote.set_recovery_endpoint(routes.get(&remote.node_id).copied());
            }
        }
    }
}
