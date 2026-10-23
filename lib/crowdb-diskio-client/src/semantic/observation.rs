// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use super::{DiskioClient, RouteSnapshot};
use crate::DiskId;

/// An inspectable retained route observation, never a placement authority.
#[derive(Clone, Debug)]
pub struct DiskTopologyView {
    snapshot: Arc<RouteSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiskPlacement {
    pub disk_id: DiskId,
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub instance_id: u64,
    pub endpoint: Arc<str>,
}

impl DiskTopologyView {
    /// Process-local route-cache generation, independent of catalog epochs.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.snapshot.generation
    }

    #[must_use]
    pub fn age(&self) -> Duration {
        self.snapshot.published_at.elapsed()
    }

    pub fn disks(&self) -> impl Iterator<Item = DiskPlacement> + '_ {
        self.snapshot.routes.iter().map(|(disk_id, route)| DiskPlacement {
            disk_id: *disk_id,
            rack_id: route.rack_id,
            node_id: route.node_id,
            disk_group_id: route.disk_group_id,
            instance_id: route.instance_id,
            endpoint: Arc::clone(&route.endpoint),
        })
    }
}

impl DiskioClient {
    #[must_use]
    pub fn topology(&self) -> DiskTopologyView {
        DiskTopologyView {
            snapshot: self.routes.load_full(),
        }
    }
}
