// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ChunkIoClient;

impl ChunkIoClient {
    /// Retains existing disk routes without opening any new connection group.
    #[must_use]
    pub fn disk_topology(&self) -> Option<crowdb_diskio_client::DiskTopologyView> {
        self.topology
            .as_ref()
            .map(|topology| topology.disk_writer.topology())
    }

    /// Actual native transport configuration and counters for these routes.
    /// # Errors
    /// Returns unavailable topology for clients constructed from low-level seams.
    pub fn transport_status_json(&self) -> crate::Result<Vec<u8>> {
        self.topology
            .as_ref()
            .ok_or_else(|| crate::IoError::Topology("client has no discovered production topology".into()))?
            .disk_writer
            .transport_status_json()
    }
}
