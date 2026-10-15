// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Startup fallback is forbidden for retained media validation.
use super::{full_scan, Bind, DdbZone, DiskGroupId, DiskId, ZoneLoadError, ZoneLoader};

impl ZoneLoader {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn complete_zone_load(
        &self,
        result: Result<Result<(DdbZone, u64), ZoneLoadError>, tokio::task::JoinError>,
        disk_id: DiskId,
        zone_index: u32,
        group: DiskGroupId,
        unit_capacity: u32,
        bind: Bind,
        strict: bool,
    ) -> Result<(DdbZone, u64), ZoneLoadError> {
        Ok(match result {
            Ok(Ok((zone, max_freed_ts))) => (zone, max_freed_ts),
            Ok(Err(e)) => {
                if strict {
                    return Err(e);
                }
                tracing::warn!(
                    disk_id = ?disk_id,
                    zone_index = zone_index,
                    error = %e,
                    "zone load failed; falling back to strategy 1 (full scan)"
                );
                // Fallback: strategy 1 full scan. No freed_ts
                // available (strategy 1 doesn't scan free
                // records).
                match full_scan::rebuild_zone_bitmap_full_scan(
                    &self.kv,
                    bind,
                    disk_id,
                    zone_index,
                    group,
                    unit_capacity,
                )
                .await
                {
                    Ok((zone, _stats)) => (zone, 0),
                    Err(e2) => {
                        tracing::error!(
                            disk_id = ?disk_id,
                            zone_index = zone_index,
                            error = %e2,
                            "strategy 1 fallback also failed; keeping disk-group non-writable"
                        );
                        return Err(ZoneLoadError::LoadFailed(format!(
                            "disk {disk_id:?} zone {zone_index}: journal replay: {e}; full scan: {e2}"
                        )));
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    disk_id = ?disk_id,
                    zone_index = zone_index,
                    error = %e,
                    "zone load task failed; keeping disk-group non-writable"
                );
                return Err(ZoneLoadError::LoadFailed(format!(
                    "disk {disk_id:?} zone {zone_index}: task join: {e}"
                )));
            }
        })
    }
}
