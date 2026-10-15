use super::{
    full_scan, info, journal_replay, unit_capacity_for_zone, warn, Arc, Bind, Counter, DdbDisk, DdbDiskGroup,
    DdbKvClient, DiskId, DiskMetrics, DiskValue, HwStateMachine, HwStatus, KeepAlive,
};

impl KeepAlive {
    /// Add an initializing disk and start loading its zones when bound.
    pub(super) fn disk_add_init(&self, dg: &Arc<DdbDiskGroup>, disk_id: DiskId, disk_value: &DiskValue) {
        let mut disk = DdbDisk::new(
            disk_id,
            dg.disk_group_id,
            dg.node_id,
            dg.rack_id,
            disk_value.clone(),
        );
        disk.metrics = Some(Arc::new(DiskMetrics::new()));
        let disk = Arc::new(disk);

        // Add the Init disk to the group (not allocatable until
        // transitioned to Up).
        if disk_value.status == HwStatus::Offline as i32 {
            disk.exclude();
        }
        dg.add_disk(disk.clone());
        dg.rebuild_allocating_disks();

        if disk.write_gate.closed() {
            return;
        }

        // Spawn background zone load if we have a kv client and the
        // disk-group is bound to a real data group.
        if let Some(ref kv) = self.kv {
            let bind = dg.bind();
            if bind == (0, 0) {
                info!(
                    disk = ?disk_id,
                    dg_id = dg.disk_group_id,
                    "disk-add init: disk-group unbound; deferring zone load until bind is set"
                );
                return;
            }
            if disk.try_claim_zone_load() {
                self.spawn_zone_load(dg, &disk, disk_value, bind, kv);
            }
        } else {
            // No kv client (test mode) — transition Init → Up with
            // empty zones so the disk becomes allocatable.
            let target = HwStatus::try_from(disk_value.status).unwrap_or(HwStatus::Up);
            let final_status = if HwStateMachine::is_legal_transition(HwStatus::Init, target) {
                target
            } else {
                warn!(
                    disk = ?disk_id,
                    target = ?target,
                    "disk-add init: illegal Init → target; falling back to Offline"
                );
                HwStatus::Offline
            };
            if let Err(e) = self.status_machine.transition_disk(&disk, final_status) {
                warn!(disk = ?disk_id, error = %e, "disk-add init: Init → final_status failed");
            }
            disk.rebuild_active_zones(self.config.zone_rotate_count);
            dg.rebuild_allocating_disks();
        }
    }

    /// Spawn the background zone load task for a disk. Extracted from
    /// `disk_add_init` so it can also be called from
    /// `reconcile_existing_disk` when a deferred Init disk's bind
    /// becomes set.
    pub(super) fn spawn_zone_load(
        &self,
        dg: &Arc<DdbDiskGroup>,
        disk: &Arc<DdbDisk>,
        disk_value: &DiskValue,
        bind: Bind,
        kv: &DdbKvClient,
    ) {
        let disk_id = disk.disk_id;
        let zone_rotate_count = self.config.zone_rotate_count;
        let status_machine = self.status_machine.clone();
        let dg = Arc::clone(dg);
        let disk = Arc::clone(disk);
        let kv = Arc::new(kv.for_group(&dg));
        let cas_retry_metric = self.cas_retry_metric.clone();
        let disk_value_owned = disk_value.clone();
        tokio::spawn(async move {
            Self::background_zone_load(
                bind,
                disk_id,
                disk_value_owned,
                kv,
                cas_retry_metric,
                zone_rotate_count,
                status_machine,
                dg,
                disk,
            )
            .await;
        });
    }

    /// Background zone load task for a new Init-state disk. Loads all
    /// zones via strategy 2 (journal replay) with strategy 1 (full
    /// scan) fallback, then transitions `Init → disk_value.status`.
    /// B.2: writes baseline `ZoneValue` records for fresh-disk zones
    /// that had no snapshot.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn background_zone_load(
        bind: Bind,
        disk_id: DiskId,
        disk_value: DiskValue,
        kv: Arc<DdbKvClient>,
        cas_retry_metric: Option<Arc<Counter>>,
        zone_rotate_count: u32,
        status_machine: HwStateMachine,
        dg: Arc<DdbDiskGroup>,
        disk: Arc<DdbDisk>,
    ) {
        let zone_count = disk_value.zone_count;
        let zone_size_units = disk_value.zone_size_units;
        let mut all_ok = true;
        let fresh = match kv.disk_has_records(bind, &disk_id).await {
            Ok(has_records) => !has_records,
            Err(error) => {
                warn!(disk = ?disk_id, error = %error, "disk record discovery failed; quarantining disk");
                all_ok = false;
                false
            }
        };
        let baseline_slot = if fresh {
            match kv.get_applied_slot(bind).await {
                Ok(slot) => slot,
                Err(error) => {
                    warn!(disk = ?disk_id, error = %error, "fresh disk frontier discovery failed; quarantining disk");
                    all_ok = false;
                    0
                }
            }
        } else {
            0
        };
        let mut baselines = Vec::with_capacity(crate::ddb_kv_client::MAX_ZONE_INIT_BATCH);
        let mut baseline_count = 0;

        for zi in 0..zone_count {
            if !all_ok {
                break;
            }
            let unit_capacity = unit_capacity_for_zone(&disk_value, zi, zone_count, zone_size_units);
            let (zone, _max_freed_ts, snapshot_found) = if fresh {
                let zone = crate::model::zone::DdbZone::new(disk_id, zi, dg.disk_group_id, unit_capacity);
                zone.snapshot_slot
                    .store(baseline_slot, std::sync::atomic::Ordering::Release);
                (zone, 0, false)
            } else {
                match journal_replay::load_zone_inner(&kv, bind, disk_id, zi, dg.disk_group_id, unit_capacity)
                    .await
                {
                    Ok((zone, max_freed_ts, found)) => (zone, max_freed_ts, found),
                    Err(e) => {
                        warn!(
                            disk = ?disk_id,
                            zone = zi,
                            error = %e,
                            "init-state load: strategy 2 failed; falling back to full scan"
                        );
                        match full_scan::rebuild_zone_bitmap_full_scan(
                            &kv,
                            bind,
                            disk_id,
                            zi,
                            dg.disk_group_id,
                            unit_capacity,
                        )
                        .await
                        {
                            // Strategy 1 fallback — not a fresh disk
                            // (it has records); no baseline needed.
                            Ok((zone, _)) => (zone, 0, true),
                            Err(e2) => {
                                tracing::error!(
                                    disk = ?disk_id,
                                    zone = zi,
                                    error = %e2,
                                    "init-state load: strategy 1 also failed; quarantining disk"
                                );
                                all_ok = false;
                                break;
                            }
                        }
                    }
                }
            };

            // Attach CAS retry metric if configured.
            let zone = if let Some(ref counter) = cas_retry_metric {
                Arc::new(zone.with_cas_retry_metric(Arc::clone(counter)))
            } else {
                Arc::new(zone)
            };

            // B.2: if no snapshot was found (fresh disk zone), queue
            // for baseline write. Strategy 1 fallback zones have
            // records (snapshot_found = true) — no baseline needed.
            if !snapshot_found {
                baselines.push((zi, zone.to_zone_value()));
                baseline_count += 1;
                if baselines.len() == crate::ddb_kv_client::MAX_ZONE_INIT_BATCH {
                    if let Err(error) = kv.put_zone_batch(bind, &disk_id, &baselines).await {
                        warn!(disk = ?disk_id, error = %error, "baseline zone batch failed; quarantining disk");
                        all_ok = false;
                        break;
                    }
                    baselines.clear();
                }
            }

            disk.add_zone(&zone);
        }

        if all_ok && !baselines.is_empty() {
            if let Err(error) = kv.put_zone_batch(bind, &disk_id, &baselines).await {
                warn!(disk = ?disk_id, error = %error, "baseline zone batch failed; quarantining disk");
                all_ok = false;
            }
        }
        if all_ok && baseline_count > 0 {
            info!(disk = ?disk_id, baseline_count, "baseline ZoneValue records written for fresh-disk zones");
        }

        // Transition Init → final status.
        let target = if all_ok {
            HwStatus::try_from(disk_value.status).unwrap_or(HwStatus::Up)
        } else {
            HwStatus::Offline
        };
        let final_status = if HwStateMachine::is_legal_transition(HwStatus::Init, target) {
            target
        } else {
            warn!(
                disk = ?disk_id,
                target = ?target,
                "init-state load: illegal Init → target; falling back to Offline"
            );
            HwStatus::Offline
        };
        // R77: write back Offline to group 0 before the local
        // transition so the next sync tick sees Offline (not Up) —
        // prevents the recover_disk_to_up loop on zone-load failure.
        if final_status == HwStatus::Offline {
            if let Err(e) = kv.mark_disk_offline_if_owned(&dg, &disk_id).await {
                warn!(
                    disk = ?disk_id,
                    status = ?final_status,
                    error = %e,
                    "init-state load: write-back Offline failed (best-effort; local transition proceeds)"
                );
            }
        }
        if let Err(e) = status_machine.transition_disk(&disk, final_status) {
            warn!(disk = ?disk_id, error = %e, "init-state load: Init → final_status failed");
        }
        disk.rebuild_active_zones(zone_rotate_count);
        dg.rebuild_allocating_disks();
        info!(disk = ?disk_id, status = ?final_status, "init-state zone load complete");
    }
}
