// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Execute accepted Online validation independently of the requesting UI.
use super::{warn, Arc, AtomicBool, DdbDisk, DdbDiskGroup, HwStatus, KeepAlive, ObservedDiskGroup, Ordering};
use crowdb_protocol::{disk_storage::DiskOnlineState, key::DiskKey};

struct Running(Arc<AtomicBool>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl KeepAlive {
    pub(super) async fn start_online_validation(&self, observed: &[ObservedDiskGroup]) {
        if !self.ownership_fencing || self.kv.is_none() || self.online_running.load(Ordering::Acquire) {
            return;
        }
        let candidates: Vec<_> = observed
            .iter()
            .flat_map(|group| {
                group
                    .disks
                    .iter()
                    .filter(|(_, disk)| disk.status == HwStatus::Offline as i32)
                    .map(move |(id, _)| (group, *id))
            })
            .collect();
        if candidates.is_empty() {
            return;
        }
        let offset = self.online_cursor.fetch_add(32, Ordering::Relaxed) % candidates.len();
        for index in 0..candidates.len().min(32) {
            let (observed, id) = candidates[(offset + index) % candidates.len()];
            let Some(group) = self.container.get_disk_group(observed.owner.dg_id) else {
                continue;
            };
            let Some(disk) = group.get_disk(id) else {
                continue;
            };
            let Some(fence) = group.ownership_fence() else {
                continue;
            };
            if Some(fence.clone()) != observed.fence || !disk.write_gate.quiescent() {
                continue;
            }
            let key = DiskKey {
                rack_id: group.rack_id,
                node_id: group.node_id,
                disk_group_id: group.disk_group_id,
                disk_id: id,
            };
            let accepted = match self.hw.disk_online_operation(&key).await {
                Ok(Some(record)) if record.state == DiskOnlineState::Pending => record,
                Ok(_) => continue,
                Err(error) => {
                    warn!(disk = ?id, %error, "Online authorization observation failed");
                    continue;
                }
            };
            match self.hw.disk_removal_readiness(&key).await {
                Ok(ready)
                    if ready.disk_revision == accepted.offline_revision && ready.conditions.is_empty() => {}
                _ => continue,
            }
            if self
                .online_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }
            let running = Running(self.online_running.clone());
            let hardware = self.hw.clone();
            let kv = self.kv.as_ref().expect("checked KV client").for_group(&group);
            tokio::spawn(async move {
                let _running = running;
                let validation = validate_disk(&kv, &group, &accepted).await;
                if !disk.write_gate.quiescent() {
                    return;
                }
                let failure = validation.err().map(|error| error.to_string());
                if let Err(error) = hardware
                    .finish_disk_online(
                        &key,
                        &accepted,
                        fence.instance_id,
                        fence.generation,
                        failure.as_deref(),
                    )
                    .await
                {
                    warn!(disk = ?id, %error, "Online validation outcome awaits authority reconciliation");
                }
            });
            return;
        }
    }

    pub(super) async fn restore_validated_online_disk(&self, group: &Arc<DdbDiskGroup>, old: &Arc<DdbDisk>) {
        if !old.write_gate.quiescent() {
            return;
        }
        let key = DiskKey {
            rack_id: group.rack_id,
            node_id: group.node_id,
            disk_group_id: group.disk_group_id,
            disk_id: old.disk_id,
        };
        let result = async {
            let Some(operation) = self.hw.disk_online_operation(&key).await? else {
                return Ok(());
            };
            if operation.state != DiskOnlineState::Complete {
                return Ok(());
            }
            let Some((disk, _)) = self.hw.get_disk_with_revision(&key).await? else {
                return Ok(());
            };
            if disk.status != HwStatus::Up as i32 {
                return Ok(());
            }
            let mut geometry = disk.clone();
            geometry.status = operation.disk.geometry.status;
            if geometry != operation.disk.geometry
                || self.hw.disk_storage(&key).await?.as_ref() != Some(&operation.disk)
            {
                return Ok(());
            }
            // Old snapshots retain their closed gate. The new Init admission is
            // published before its strict background loader can expose allocations.
            group.remove_disk_from_memory(&old.disk_id);
            self.disk_add_init(group, old.disk_id, &disk);
            Ok::<(), crowdb_kv_client::Error>(())
        }
        .await;
        if let Err(error) = result {
            warn!(disk = ?old.disk_id, %error, "validated Online admission remains excluded");
        }
    }
}

async fn validate_disk(
    kv: &crate::ddb_kv_client::DdbKvClient,
    group: &DdbDiskGroup,
    accepted: &crowdb_protocol::disk_storage::DiskOnlineRecord,
) -> Result<(), crate::recovery::ZoneLoadError> {
    let geometry = &accepted.disk.geometry;
    for zone in 0..geometry.zone_count {
        let capacity = crate::recovery::unit_capacity_for_zone(
            geometry,
            zone,
            geometry.zone_count,
            geometry.zone_size_units,
        );
        crate::recovery::retained::load_retained_zone(
            kv,
            group.bind(),
            accepted.disk.disk_id,
            zone,
            group.disk_group_id,
            capacity,
            geometry.unit_size_bytes,
        )
        .await?;
    }
    Ok(())
}
