// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Distinguish unavailable disks from exhausted allocatable capacity.

use super::{AllocateParams, FBDiskdbRetCode};

pub(super) fn describe(params: &AllocateParams) -> (FBDiskdbRetCode, String) {
    let usage = params.dg.aggregate_usage();
    let unavailable = !params.dg.allocatable() || usage.allocatable_disk_count == 0;
    let code = if unavailable {
        FBDiskdbRetCode::Unavailable
    } else {
        FBDiskdbRetCode::NoSpace
    };
    let states: Vec<_> = params
        .dg
        .disk_snapshot()
        .iter()
        .map(|disk| {
            format!(
                "{:?}: status={:?}, write_gate_closed={}, write_outcome_unknown={}",
                disk.disk_id,
                disk.effective_status(),
                disk.write_gate.closed(),
                disk.write_gate.outcome_unknown()
            )
        })
        .collect();
    let message =
        format!(
        "DiskDB allocation {}: disk_group={}, group_status={:?}, requested_blocks={}, units_per_block={}, \
         allocatable_disks={}/{}, capacity_bytes={}, busy_bytes={}, free_bytes={}, \
         allocatable_free_bytes={}, disks=[{}]",
        if unavailable { "unavailable" } else { "has no eligible space" },
        params.dg.disk_group_id,
        params.dg.status(),
        params.count,
        params.unit_count,
        usage.allocatable_disk_count,
        usage.disk_count,
        usage.capacity_bytes,
        usage.busy_bytes,
        usage.free_bytes,
        usage.allocatable_free_bytes,
        states.join("; ")
    );
    (code, message)
}
