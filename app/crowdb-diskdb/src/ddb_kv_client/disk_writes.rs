// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Retain admission permits across durable metadata writes and unknown outcomes.

use super::{
    BatchOp, BinaryKey, BusyBlockKey, DdbKvClient, DiskId, FreeBlockKey, RecoveryScanProgressKey,
    RelocationJournalKey, Result, ZoneKey,
};
use crate::model::disk::DiskWritePermit;

fn disk_id(operation: &BatchOp) -> Option<DiskId> {
    let (BatchOp::Put { key, .. } | BatchOp::Delete { key }) = operation;
    ZoneKey::decode(key)
        .map(|key| key.disk_id)
        .or_else(|_| BusyBlockKey::decode(key).map(|key| key.disk_id))
        .or_else(|_| FreeBlockKey::decode(key).map(|key| key.disk_id))
        .or_else(|_| RecoveryScanProgressKey::decode(key).map(|key| key.disk_id))
        .or_else(|_| RelocationJournalKey::decode(key).map(|key| key.disk_id))
        .ok()
}

impl DdbKvClient {
    pub(super) fn admit_disk_writes(&self, operations: &[BatchOp]) -> Result<Vec<DiskWritePermit<'_>>> {
        let Some(disks) = &self.disks else {
            return Ok(Vec::new());
        };
        if self.require_fence
            && operations
                .iter()
                .filter_map(disk_id)
                .any(|id| !disks.iter().any(|disk| disk.disk_id == id))
        {
            return Err(crowdb_kv_client::Error::Server(
                "disk is absent from the captured membership".into(),
            ));
        }
        let mut permits: Vec<DiskWritePermit<'_>> = Vec::new();
        for disk in disks.iter() {
            if operations
                .iter()
                .any(|operation| disk_id(operation) == Some(disk.disk_id))
            {
                let Some(permit) = disk.write_gate.enter() else {
                    // No persistence has begun; release earlier reservations as
                    // definite non-writes rather than poisoning unrelated disks.
                    for permit in permits {
                        permit.complete();
                    }
                    return Err(crowdb_kv_client::Error::Server(
                        "disk is administratively excluded; metadata write rejected".into(),
                    ));
                };
                permits.push(permit);
            }
        }
        Ok(permits)
    }
}
