// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only ChunkDB and physical DiskIO evidence through current client routes.

use crowdb_protocol::{chunkdb::rpc::ListChunksRequest, common::HwStatus, diskdb::rpc::Segment};

use super::{ChunkIoClient, DiskWriter, IoError, Result};

impl ChunkIoClient {
    /// Observe current ChunkDB routing and read a bounded header from each online disk.
    /// The caller owns the observation deadline. This never allocates or repairs chunks.
    ///
    /// # Errors
    /// Returns discovery, ownership, transport or physical read failures.
    pub async fn observe_readiness(&self) -> Result<String> {
        let topology = self
            .topology
            .as_ref()
            .ok_or_else(|| IoError::Topology("readiness requires authoritative discovery".into()))?;
        let (disks, chunks) = tokio::join!(
            topology.hardware.list_all_disks(),
            topology.chunkdb.list_chunks(ListChunksRequest {
                max_keys: 1,
                ..Default::default()
            }),
        );
        chunks.map_err(|error| IoError::ReadFailed(format!("ChunkDB read: {error}")))?;
        let disks = disks.map_err(|error| IoError::Topology(error.to_string()))?;
        let online: Vec<_> = disks
            .iter()
            .filter(|disk| disk.value.status == HwStatus::Up as i32)
            .collect();
        if online.is_empty() {
            return Err(IoError::Topology("no online disks are available".into()));
        }
        // Keep background observation work bounded independently of cluster size.
        for batch in online.chunks(8) {
            let mut reads = tokio::task::JoinSet::new();
            for disk in batch {
                let writer = topology.disk_writer.clone();
                let id = disk.disk_id;
                reads.spawn(async move {
                    let segment = Segment {
                        disk_id: Some(id),
                        unit_count: 1,
                        ..Default::default()
                    };
                    let bytes = writer
                        .read(&segment, 4096, 0, 4096)
                        .await
                        .map_err(|error| IoError::ReadFailed(format!("DiskIO disk {id:?}: {error}")))?;
                    if bytes.len() != 4096 {
                        return Err(IoError::ReadFailed(format!(
                            "DiskIO disk {id:?} returned a short header"
                        )));
                    }
                    Ok(())
                });
            }
            while let Some(read) = reads.join_next().await {
                read.map_err(|error| IoError::ReadFailed(error.to_string()))??;
            }
        }
        Ok(format!(
            "ChunkDB read and 4096-byte DiskIO reads succeeded on {} online disks",
            online.len()
        ))
    }
}
