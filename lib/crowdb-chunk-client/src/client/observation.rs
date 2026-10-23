// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ChunkIoClient;

impl ChunkIoClient {
    /// Retains existing disk routes without opening any new connection group.
    #[must_use]
    pub fn disk_topology(&self) -> Option<crowdb_diskio_client::DiskTopologyView> {
        self.disk_writer.disk_topology()
    }

    /// Actual native transport configuration and counters for these routes.
    /// # Errors
    /// Returns unavailable topology for clients constructed from low-level seams.
    pub fn transport_status_json(&self) -> crate::Result<Vec<u8>> {
        self.disk_writer
            .transport_observation()?
            .ok_or_else(|| crate::IoError::Topology("disk writer has no native transport observation".into()))
    }
}
