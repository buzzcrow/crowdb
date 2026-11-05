// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Independent process renewal remains visible while partitions replay.

use super::{operation_error, wall_time_ms, Group0ControlPlane};
use bytes::Bytes;
use crowdb_protocol::{
    chunk_stream::{StreamProcessLease, StreamPurpose},
    key::{StreamProcessLeaseKey, TextKey},
};
use std::collections::HashSet;

pub(super) async fn live_processes(control: &Group0ControlPlane) -> Result<HashSet<u64>, String> {
    let rows = control
        .scan_all_prefix(Bytes::from(StreamProcessLeaseKey::prefix_all()), 256)
        .await
        .map_err(|error| operation_error(&error))?;
    let now = wall_time_ms();
    let mut live = HashSet::new();
    for row in rows {
        let lease: StreamProcessLease = serde_json::from_slice(&row.value).map_err(|e| e.to_string())?;
        lease.validate().map_err(|e| e.to_string())?;
        let key = StreamProcessLeaseKey {
            purpose: lease.identity.purpose,
            instance_id: lease.identity.instance_id,
        };
        if row.key.as_ref() != key.to_path().as_bytes() {
            return Err("stream process lease differs from its key".into());
        }
        if lease.identity.purpose == StreamPurpose::Wal && lease.expires_at_ms > now {
            live.insert(lease.identity.instance_id);
        }
    }
    Ok(live)
}
