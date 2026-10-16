// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish evidence only after administrative closure and durable-write drainage.

use super::{warn, HwStatus, KeepAlive, ObservedDiskGroup, Ordering};
use crowdb_protocol::{
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter},
    key::DiskKey,
};

impl KeepAlive {
    pub(super) async fn confirm_exclusions(&self, observed: &[ObservedDiskGroup]) {
        if !self.ownership_fencing {
            return;
        }
        let candidates: Vec<_> = observed
            .iter()
            .flat_map(|group| {
                group
                    .disks
                    .iter()
                    .filter(|(_, disk)| disk.status == HwStatus::Offline as i32)
                    .map(move |(disk_id, disk)| (group, disk_id, disk))
            })
            .collect();
        if candidates.is_empty() {
            return;
        }
        // Bound authority work per tick and rotate past unready/unreachable disks.
        let offset = self.exclusion_cursor.fetch_add(32, Ordering::Relaxed) % candidates.len();
        for index in 0..candidates.len().min(32) {
            let (group, disk_id, observed_disk) = candidates[(offset + index) % candidates.len()];
            let Some(fence) = &group.fence else {
                continue;
            };
            let Some(runtime) = self.container.get_disk_group(group.owner.dg_id) else {
                continue;
            };
            if runtime.ownership_fence() != group.fence {
                continue;
            }
            let disks = runtime.disk_snapshot();
            let Some(disk) = disks.iter().find(|disk| disk.disk_id == *disk_id) else {
                continue;
            };
            if !disk.write_gate.quiescent() {
                continue;
            }
            let key = DiskKey {
                rack_id: group.owner.rack_id,
                node_id: group.owner.node_id,
                disk_group_id: group.owner.dg_id,
                disk_id: *disk_id,
            };
            let result = async {
                let Some((current, revision)) = self.hw.get_disk_with_revision(&key).await? else {
                    return Ok(());
                };
                if current != *observed_disk {
                    return Ok(());
                }
                self.hw
                    .acknowledge_disk_exclusion(&DiskExclusionRecord {
                        version: 1,
                        rack_id: key.rack_id,
                        node_id: key.node_id,
                        disk_group_id: key.disk_group_id,
                        disk_id: *disk_id,
                        disk_revision: revision,
                        reporter: DiskExclusionReporter::Diskdb,
                        instance_id: fence.instance_id,
                        owner_revision: Some(fence.generation),
                    })
                    .await
            }
            .await;
            if let Err(error) = result {
                warn!(disk = ?disk_id, %error, "disk exclusion confirmation remains pending");
            }
        }
    }
}
