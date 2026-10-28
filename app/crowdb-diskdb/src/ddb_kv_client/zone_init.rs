// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded initial zone publication through the captured ownership fence.

use super::{
    BatchOp, BinaryKey, Bind, BusyBlockKey, Bytes, DdbKvClient, DiskId, FreeBlockKey, ReadMode, Result,
    ZoneKey, ZoneValue, ZoneValueExt,
};

pub const MAX_ZONE_INIT_BATCH: usize = 128;

impl DdbKvClient {
    /// Any snapshot, live allocation or free fact requires ordinary recovery.
    ///
    /// # Errors
    /// Propagates authoritative metadata read failures.
    pub async fn disk_has_records(&self, bind: Bind, disk: &DiskId) -> Result<bool> {
        for prefix in [
            ZoneKey::prefix_for_disk(disk),
            BusyBlockKey::prefix_for_disk(disk),
            FreeBlockKey::prefix_for_disk(disk),
        ] {
            let records = self
                .kv
                .scan(
                    bind.0,
                    bind.1,
                    &prefix,
                    &[],
                    &[],
                    1,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await?;
            if !records.items.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Initialize at most one bounded batch; never bypass ownership fencing.
    ///
    /// # Errors
    /// Rejects an empty or oversized batch and propagates ownership or write failures.
    pub async fn put_zone_batch(&self, bind: Bind, disk: &DiskId, values: &[(u32, ZoneValue)]) -> Result<()> {
        if values.is_empty() || values.len() > MAX_ZONE_INIT_BATCH {
            return Err(crowdb_paxos_kv_client::Error::Server(
                "zone initialization batch must contain 1..=128 records".into(),
            ));
        }
        let ops: Vec<_> = values
            .iter()
            .map(|(zone_index, value)| BatchOp::Put {
                key: Bytes::from(
                    ZoneKey {
                        disk_id: *disk,
                        zone_index: *zone_index,
                    }
                    .to_bytes(),
                ),
                value: Bytes::from(value.to_bytes()),
            })
            .collect();
        self.write_owned(bind, &ops, None).await.map(|_| ())
    }
}
