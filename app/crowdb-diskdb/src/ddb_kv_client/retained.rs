// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Strict, bounded observations for retained disks; never writes recovery data.
use super::{BinaryKey, Bind, BusyBlockKey, DdbKvClient, DiskId, FreeBlockKey, ZoneKey};
use crate::model::records::{BusyRecord, FreeRecord, ZoneRecords};
use bincode::Options;
use crowdb_paxos_kv_client::{Error, Result};
use crowdb_protocol::diskdb::rpc::ZoneValue;
use crowdb_protocol::ZoneValueExt;
use serde::de::DeserializeOwned;

const PAGE_RECORDS: u32 = 128;
const MAX_RECORDS: usize = 131_072;
const MAX_BYTES: usize = 32 * 1024 * 1024;

fn invalid(reason: impl Into<String>) -> Error {
    Error::Server(format!("retained zone validation: {}", reason.into()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_BYTES as u64)
        .reject_trailing_bytes()
        .deserialize(bytes)
        .map_err(|error| invalid(error.to_string()))
}

impl DdbKvClient {
    /// A durable rejoin or Online authorization forbids fresh initialization.
    ///
    /// # Errors
    /// Propagates Group-0 authority or schema failures; callers must fail closed.
    pub async fn disk_requires_retained_validation(
        &self,
        key: &crowdb_protocol::key::DiskKey,
    ) -> Result<bool> {
        let hardware = crowdb_paxos_kv_client::HardwareClient::from_shared(self.kv.clone());
        Ok(hardware.disk_rejoin_receipt(key).await?.is_some()
            || hardware.disk_online_operation(key).await?.is_some())
    }

    /// Observe an existing snapshot and all live facts at one fixed cutoff.
    ///
    /// # Errors
    /// Rejects incomplete/nonadvancing pages, malformed records, missing baselines
    /// and exceeded bounds. No record is skipped or silently treated as empty.
    pub async fn read_retained_zone_records(
        &self,
        bind: Bind,
        disk: &DiskId,
        zone: u32,
    ) -> Result<(ZoneRecords, u64)> {
        let snapshot = ZoneKey {
            disk_id: *disk,
            zone_index: zone,
        }
        .to_bytes();
        let mut records = ZoneRecords::default();
        let mut cutoff = 0;
        let mut count = 0;
        let mut bytes = 0;
        for (kind, prefix) in [
            snapshot,
            BusyBlockKey::prefix_for_zone(disk, zone),
            FreeBlockKey::prefix_for_zone(disk, zone),
        ]
        .into_iter()
        .enumerate()
        {
            let mut start = Vec::new();
            loop {
                let page = self
                    .kv
                    .scan_bounded_at(
                        bind.0,
                        bind.1,
                        &prefix,
                        &start,
                        &[],
                        PAGE_RECORDS,
                        false,
                        None,
                        cutoff,
                    )
                    .await?;
                if page.timed_out
                    || page.items.len() != page.commit_slots.len()
                    || page.items.len() > PAGE_RECORDS as usize
                    || (cutoff != 0 && cutoff != page.scan_cutoff)
                    || (page.truncated && page.items.is_empty())
                {
                    return Err(invalid("incomplete or inconsistent scan page"));
                }
                cutoff = page.scan_cutoff;
                for ((key, value), slot) in page.items.iter().zip(&page.commit_slots) {
                    if !key.starts_with(&prefix) || key.as_ref() <= start.as_slice() || *slot > cutoff {
                        return Err(invalid("scan did not advance within the fixed boundary"));
                    }
                    start = key.to_vec();
                    count += 1;
                    bytes += key.len() + value.len();
                    if count > MAX_RECORDS || bytes > MAX_BYTES {
                        return Err(invalid("record or byte limit exceeded"));
                    }
                    match kind {
                        0 => {
                            if key.as_ref() != prefix || records.zone_value.is_some() {
                                return Err(invalid("unexpected snapshot key"));
                            }
                            let snapshot = ZoneValue::from_bytes(value).map_err(invalid)?;
                            if snapshot.to_bytes().as_slice() != value.as_ref() {
                                return Err(invalid("noncanonical snapshot encoding"));
                            }
                            records.zone_value = Some(snapshot);
                        }
                        1 => records.busy.push(BusyRecord {
                            key: BusyBlockKey::from_bytes(key).map_err(|e| invalid(e.to_string()))?,
                            value: decode(value)?,
                            commit_slot: *slot,
                        }),
                        _ => records.free.push(FreeRecord {
                            key: FreeBlockKey::from_bytes(key).map_err(|e| invalid(e.to_string()))?,
                            value: decode(value)?,
                            commit_slot: *slot,
                        }),
                    }
                }
                if !page.truncated {
                    break;
                }
            }
        }
        if records.zone_value.is_none() {
            return Err(invalid("existing snapshot is missing"));
        }
        Ok((records, cutoff))
    }
}
