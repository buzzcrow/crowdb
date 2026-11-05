// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded string-safe disk exclusion confirmations from C++.

use super::{crowdb_hw_client_t, crowdb_kv_on_complete, spawn_op, HardwareClient};
use crowdb_protocol::{
    common::DiskId,
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter},
};
use std::{
    ffi::CStr,
    os::raw::{c_char, c_void},
    ptr,
    time::Duration,
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ExclusionWire {
    #[serde(rename = "disk_group_id")]
    group: String,
    #[serde(rename = "disk_id_high")]
    high: String,
    #[serde(rename = "disk_id_low")]
    low: String,
    #[serde(rename = "disk_revision")]
    revision: String,
}

fn parse(
    payload: &str,
    rack_id: u64,
    node_id: u64,
    instance_id: u64,
) -> Result<Vec<DiskExclusionRecord>, String> {
    if payload.len() > 16384 {
        return Err("disk exclusion payload exceeds bound".into());
    }
    let entries: Vec<ExclusionWire> = serde_json::from_str(payload).map_err(|error| error.to_string())?;
    if entries.is_empty() || entries.len() > 16 {
        return Err("disk exclusion batch requires 1..16 entries".into());
    }
    let mut seen = std::collections::HashSet::new();
    entries
        .into_iter()
        .map(|entry| {
            let word = |value: &str| value.parse::<u64>().map_err(|error| error.to_string());
            let record = DiskExclusionRecord {
                version: 1,
                rack_id,
                node_id,
                instance_id,
                disk_group_id: word(&entry.group)?,
                disk_id: DiskId {
                    high: word(&entry.high)?,
                    low: word(&entry.low)?,
                },
                disk_revision: word(&entry.revision)?,
                reporter: DiskExclusionReporter::Diskio,
                owner_revision: None,
            };
            record.validate().map_err(str::to_string)?;
            if !seen.insert((record.disk_id.high, record.disk_id.low)) {
                return Err("duplicate disk exclusion identity".into());
            }
            Ok(record)
        })
        .collect()
}

/// # Errors
/// Returns malformed or out-of-bounds wire input.
#[cfg(feature = "test-util")]
pub fn parse_diskio_exclusions_for_tests(payload: &str) -> Result<Vec<DiskExclusionRecord>, String> {
    parse(payload, 1, 2, 3)
}

/// Confirm a bounded set only after descriptor retirement and all I/O completes.
#[no_mangle]
pub unsafe extern "C" fn crowdb_hw_ack_diskio_exclusions(
    client: crowdb_hw_client_t,
    rack_id: u64,
    node_id: u64,
    instance_id: u64,
    entries_json: *const c_char,
    callback: crowdb_kv_on_complete,
    user_data: *mut c_void,
) {
    if client.is_null() || entries_json.is_null() {
        callback(-1, ptr::null(), user_data);
        return;
    }
    let payload = if let Ok(value) = CStr::from_ptr(entries_json).to_str() {
        value.to_string()
    } else {
        callback(-1, ptr::null(), user_data);
        return;
    };
    let hw = (*(client as *const HardwareClient)).clone();
    spawn_op(callback, user_data, move || async move {
        let records = parse(&payload, rack_id, node_id, instance_id)?;
        tokio::time::timeout(Duration::from_secs(3), async {
            for record in records {
                hw.acknowledge_disk_exclusion(&record)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok::<_, String>("{}".to_string())
        })
        .await
        .map_err(|_| "disk exclusion confirmation timed out; reconcile the same revisions".to_string())?
    });
}
