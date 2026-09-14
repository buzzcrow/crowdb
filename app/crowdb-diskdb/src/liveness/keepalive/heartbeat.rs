use super::{warn, DiskGroupUsageSummary, KeepAlive, Ordering};

impl KeepAlive {
    /// Heartbeat to the service registry. Returns `false` on failure
    /// (caller skips the rest of the tick). Tracks missed count and
    /// enters degraded mode on threshold breach.
    pub(super) async fn heartbeat(&self) -> bool {
        let instance_id = self.container.instance_id;
        // Compute per-disk-group usage summaries from the in-memory
        // bitmap (R74 §8 keepalive piggyback). Recomputed each tick
        // (not cached); derived, not a source of truth.
        let owned_dg_ids: Vec<u64> = self.container.disk_group_ids();
        let group_usages: Vec<DiskGroupUsageSummary> = self
            .container
            .disk_group_ids()
            .into_iter()
            .filter_map(|dg_id| {
                let dg = self.container.get_disk_group(dg_id)?;
                let u = dg.aggregate_usage();
                Some(DiskGroupUsageSummary {
                    disk_group_id: dg_id,
                    capacity_bytes: u.capacity_bytes,
                    used_bytes: u.busy_bytes,
                    free_bytes: u.free_bytes,
                    disk_count: u.disk_count,
                    allocatable_disk_count: u.allocatable_disk_count,
                    allocatable_capacity_bytes: u.allocatable_capacity_bytes,
                    allocatable_used_bytes: u.allocatable_busy_bytes,
                    allocatable_free_bytes: u.allocatable_free_bytes,
                    sampled_at_ms: unix_time_ms(),
                })
            })
            .collect();
        if let Err(e) = self
            .svc
            .heartbeat_diskdb(instance_id, &self.rpc_endpoint, &owned_dg_ids, &group_usages)
            .await
        {
            warn!(error = %e, "sync: heartbeat failed");
            let count = self.missed_count.fetch_add(1, Ordering::SeqCst) + 1;
            if count >= self.config.miss_threshold {
                self.container.enter_degraded_mode();
            }
            return false;
        }
        true
    }
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
