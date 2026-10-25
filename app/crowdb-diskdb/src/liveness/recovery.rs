// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unknown writes recover only across a newly fenced process session.

use std::time::Duration;

use crate::model::disk_group_container::DdbDiskGroupContainer;
use crowdb_protocol::common::DiskId;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct UncertainDiskWrite {
    pub disk_group_id: u64,
    pub disk_id: DiskId,
    pub reason: &'static str,
}

/// Snapshot uncertainty without opening a closed gate or changing disk state.
#[must_use]
pub fn uncertain_writes(container: &DdbDiskGroupContainer) -> Vec<UncertainDiskWrite> {
    container
        .disk_group_ids()
        .into_iter()
        .filter_map(|id| container.get_disk_group(id))
        .flat_map(|group| {
            group
                .disk_snapshot()
                .iter()
                .filter(|disk| disk.write_gate.outcome_unknown())
                .map(|disk| UncertainDiskWrite {
                    disk_group_id: group.disk_group_id,
                    disk_id: disk.disk_id,
                    reason: "metadata-write-outcome-unknown: waiting for a new process session, ownership fence and allocation-state reconstruction",
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A supervisor may restart the process; current gates remain closed until exit.
pub async fn wait_required(container: &DdbDiskGroupContainer) -> Vec<UncertainDiskWrite> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let writes = uncertain_writes(container);
        if !writes.is_empty() {
            container.enter_degraded_mode();
            return writes;
        }
    }
}
