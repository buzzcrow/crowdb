// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only ChunkDB and physical DiskIO evidence through current client routes.

use crowdb_protocol::{common::HwStatus, diskdb::rpc::Segment};
use std::{future::Future, time::Duration};
use tokio::time::Instant;

use crate::metrics::{OperationMetrics, ReadinessMetricsSnapshot};

use super::{ChunkIoClient, DiskWriter, IoError, Result};

impl ChunkIoClient {
    /// Observe current ChunkDB routing and read a bounded header from each online disk.
    /// Every phase shares the caller's total budget. This never allocates or repairs chunks.
    ///
    /// # Errors
    /// Returns discovery, ownership, transport or physical read failures.
    pub async fn observe_readiness(&self, budget: Duration) -> Result<String> {
        let deadline = Instant::now() + budget;
        let mut total = self.readiness.total.start();
        let topology = self
            .topology
            .as_ref()
            .ok_or_else(|| IoError::Topology("readiness requires authoritative discovery".into()))?;
        let (disks, chunks) = tokio::join!(
            observe("hardware", deadline, &self.readiness.hardware, async {
                topology
                    .hardware
                    .list_all_disks()
                    .await
                    .map_err(|error| IoError::Topology(error.to_string()))
            }),
            observe("ChunkDB", deadline, &self.readiness.chunkdb, async {
                topology
                    .chunkdb
                    .observe_readiness()
                    .await
                    .map_err(|error| IoError::ReadFailed(format!("ChunkDB read: {error}")))
            }),
        );
        chunks?;
        let disks = disks?;
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
                let metrics = self.readiness.clone();
                let id = disk.disk_id;
                reads.spawn(async move {
                    let segment = Segment {
                        disk_id: Some(id),
                        unit_count: 1,
                        ..Default::default()
                    };
                    observe("DiskIO", deadline, &metrics.diskio, async {
                        let bytes = writer.read(&segment, 4096, 0, 4096).await?;
                        if bytes.len() != 4096 {
                            return Err(IoError::ReadFailed("short disk header".into()));
                        }
                        Ok(())
                    })
                    .await
                    .map_err(|error| IoError::ReadFailed(format!("DiskIO disk {id:?}: {error}")))
                });
            }
            while let Some(read) = reads.join_next().await {
                read.map_err(|error| IoError::ReadFailed(error.to_string()))??;
            }
        }
        total.mark_success();
        Ok(format!(
            "ChunkDB read and 4096-byte DiskIO reads succeeded on {} online disks",
            online.len()
        ))
    }

    /// Cumulative backend probe timings; reading these never probes dependencies.
    #[must_use]
    pub fn readiness_metrics(&self) -> ReadinessMetricsSnapshot {
        self.readiness.snapshot()
    }
}

async fn observe<T>(
    phase: &str,
    deadline: Instant,
    metrics: &OperationMetrics,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let mut measurement = metrics.start();
    let result = tokio::time::timeout_at(deadline, future)
        .await
        .unwrap_or_else(|_| {
            Err(IoError::ReadFailed(format!(
                "{phase} read exceeded the readiness deadline"
            )))
        });
    if result.is_ok() {
        measurement.mark_success();
    }
    result
}

#[cfg(feature = "test-util")]
/// Exercise the production phase deadline and accounting with controlled work.
pub async fn observe_readiness_phase_for_tests(
    metrics: &crate::metrics::ReadinessMetrics,
    deadline: Instant,
    work: impl Future<Output = Result<()>>,
) -> Result<()> {
    observe("DiskIO", deadline, &metrics.diskio, work).await
}
