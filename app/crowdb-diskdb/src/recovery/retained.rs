// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only recovery for existing media. Uncertain occupancy remains reserved.
use super::ZoneLoadError;
use crate::{
    ddb_kv_client::{Bind, DdbKvClient},
    model::{records::ZoneRecords, zone::DdbZone},
};
use crowdb_protocol::{
    common::DiskId,
    diskdb::rpc::{BlockState, CommitState},
    DiskGroupId,
};
use std::sync::atomic::Ordering;

fn invalid(reason: &str) -> ZoneLoadError {
    ZoneLoadError::LoadFailed(format!("retained metadata: {reason}"))
}

fn checked_range(offset: u64, count: u32, capacity: u32) -> Result<u32, ZoneLoadError> {
    if count == 0
        || offset
            .checked_add(u64::from(count))
            .map_or(true, |end| end > u64::from(capacity))
    {
        return Err(invalid("zero-length or out-of-range allocation"));
    }
    u32::try_from(offset).map_err(|_| invalid("allocation offset overflow"))
}

/// Validate existing records and conservatively reconstruct occupied units.
/// Free facts are checked but never clear bits during this read-only phase.
///
/// # Errors
/// Rejects missing/corrupt baselines, unsupported states, geometry mismatch,
/// overlapping live allocations and invalid timestamps or scan boundaries.
#[allow(clippy::too_many_arguments)]
pub fn validate_retained_zone(
    disk: DiskId,
    zone_index: u32,
    group: DiskGroupId,
    capacity: u32,
    unit_size: u32,
    records: &ZoneRecords,
    cutoff: u64,
) -> Result<(DdbZone, u64), ZoneLoadError> {
    if capacity == 0 || capacity % 64 != 0 || unit_size == 0 {
        return Err(invalid("invalid zone geometry"));
    }
    let snapshot = records
        .zone_value
        .as_ref()
        .ok_or_else(|| invalid("existing snapshot is missing"))?;
    if snapshot.snapshot_slot > cutoff || snapshot.compact_slot > cutoff {
        return Err(invalid("snapshot exceeds scan boundary"));
    }
    let zone = DdbZone::from_zone_value(disk, zone_index, group, capacity, snapshot)
        .ok_or_else(|| invalid("snapshot checksum, encoding or geometry mismatch"))?;
    let mut max_timestamp = snapshot.compact_ts;
    let mut busy: Vec<_> = records.busy.iter().collect();
    busy.sort_unstable_by_key(|record| record.key.unit_offset);
    let mut end = 0;
    for record in busy {
        let key = &record.key;
        let value = &record.value;
        let offset = checked_range(key.unit_offset, value.unit_count, capacity)?;
        if key.disk_id != disk
            || key.zone_index != zone_index
            || record.commit_slot > cutoff
            || key.unit_offset < end
            || value.unit_size != unit_size
            || BlockState::try_from(value.state).is_err()
            || CommitState::try_from(value.commit_state).is_err()
            || value.allocation_ts == 0
        {
            return Err(invalid("invalid or overlapping live allocation"));
        }
        end = key.unit_offset + u64::from(value.unit_count);
        // range_set rolls back on any pre-existing bit. Merge one bit at a time
        // so a partly represented allocation cannot leave its remaining units free.
        for bit in offset..offset + value.unit_count {
            let _ = zone.usage_bits.cas_bit(bit, true);
        }
        max_timestamp = max_timestamp.max(value.allocation_ts);
    }
    for record in &records.free {
        let key = &record.key;
        let value = &record.value;
        checked_range(key.unit_offset, value.unit_count, capacity)?;
        if key.disk_id != disk
            || key.zone_index != zone_index
            || record.commit_slot > cutoff
            || key.allocation_ts != value.pre_allocation_ts
            || value.pre_allocation_ts == 0
            || value.free_ts <= value.pre_allocation_ts
        {
            return Err(invalid("invalid free fact"));
        }
        max_timestamp = max_timestamp.max(value.free_ts);
    }
    if max_timestamp == u64::MAX {
        return Err(invalid("allocation timestamp exhausted"));
    }
    zone.used_count.store(
        u32::try_from(zone.usage_bits.count_set()).unwrap_or(u32::MAX),
        Ordering::Release,
    );
    zone.uncompacted_free_record_count.store(
        u32::try_from(records.free.len()).map_err(|_| invalid("too many free records"))?,
        Ordering::Release,
    );
    Ok((zone, max_timestamp))
}

/// Load a retained zone without writes, fallback snapshots or blank initialization.
///
/// # Errors
/// Propagates bounded observation and strict validation failures.
#[allow(clippy::too_many_arguments)]
pub async fn load_retained_zone(
    kv: &DdbKvClient,
    bind: Bind,
    disk: DiskId,
    zone: u32,
    group: DiskGroupId,
    capacity: u32,
    unit_size: u32,
) -> Result<(DdbZone, u64), ZoneLoadError> {
    let (records, cutoff) = kv.read_retained_zone_records(bind, &disk, zone).await?;
    validate_retained_zone(disk, zone, group, capacity, unit_size, &records, cutoff)
}
