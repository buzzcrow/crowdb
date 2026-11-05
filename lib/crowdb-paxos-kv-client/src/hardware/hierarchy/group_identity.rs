// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskGroup` allocation addresses must resolve to one physical placement.

use super::{mutation, HardwareClient};
use crate::{Error, GetOutcome, ReadMode, Result};

const GROUP_CONFLICT: &str = "disk group ID belongs to another physical location";
use crowdb_protocol::{
    disk_storage::DiskGroupIdReservation,
    key::{DiskGroupKey, TextKey},
};

impl HardwareClient {
    /// Reserve a global numeric group ID before parent membership changes.
    /// The reservation survives deletion; it never creates effective hardware.
    ///
    /// # Errors
    /// Rejects another physical placement, ambiguous legacy IDs, unsupported
    /// reservations and incomplete bounded legacy observations.
    pub async fn reserve_disk_group_id(&self, key: &DiskGroupKey) -> Result<()> {
        let path = DiskGroupIdReservation::key_for(key.disk_group_id);
        let expected = DiskGroupIdReservation::new(key);
        for _ in 0..10 {
            if let GetOutcome::Found { value, .. } = self
                .kv
                .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                let current: DiskGroupIdReservation = mutation::decode(&path, &value)?;
                if current.validate().map_err(|error| Error::Server(error.into()))? != *key
                    || current != expected
                {
                    return Err(Error::Server(GROUP_CONFLICT.into()));
                }
                return Ok(());
            }
            self.observe_legacy_group_ids(Some(key)).await?;
            let value = serde_json::to_vec(&expected).map_err(|error| Error::Server(error.to_string()))?;
            match self.kv.put_cas(0, 0, path.as_bytes(), &value, 0).await {
                Ok(_) => return Ok(()),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }

    /// Allocate and reserve a fresh, browser-safe numeric group ID.
    /// Interrupted attempts may consume an ID but never create membership.
    ///
    /// # Errors
    /// Rejects incomplete legacy observations, exhausted IDs and unavailable
    /// authority. A concurrent allocator or manual creator cannot share an ID.
    pub async fn allocate_disk_group_id(&self, rack_id: u64, node_id: u64) -> Result<u64> {
        let maximum = self.observe_legacy_group_ids(None).await?;
        let sequence = "/deployment/disk-group-sequence";
        for _ in 0..64 {
            let (previous, revision) = match self
                .kv
                .get(0, 0, sequence.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                GetOutcome::Found { value, revision } => {
                    (mutation::decode::<u64>(sequence, &value)?, revision)
                }
                GetOutcome::NotFound => (0, 0),
            };
            let next = previous
                .max(maximum)
                .checked_add(1)
                .filter(|value| *value < (1_u64 << 53))
                .ok_or_else(|| Error::Server("automatic disk group ID space is exhausted".into()))?;
            match self
                .kv
                .put_cas(
                    0,
                    0,
                    sequence.as_bytes(),
                    &serde_json::to_vec(&next).map_err(|error| Error::Server(error.to_string()))?,
                    revision,
                )
                .await
            {
                Ok(_) => {}
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => continue,
                Err(error) => return Err(error),
            }
            let key = DiskGroupKey {
                rack_id,
                node_id,
                disk_group_id: next,
            };
            match self.reserve_disk_group_id(&key).await {
                Ok(()) => return Ok(next),
                Err(Error::Server(reason)) if reason == GROUP_CONFLICT => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }

    async fn observe_legacy_group_ids(&self, expected: Option<&DiskGroupKey>) -> Result<u64> {
        let prefix = <DiskGroupKey as TextKey>::prefix_all();
        let mut after = Vec::new();
        let mut count = 0;
        let mut maximum = 0;
        loop {
            let page = self
                .kv
                .scan(
                    0,
                    0,
                    prefix.as_bytes(),
                    &after,
                    &[],
                    128,
                    ReadMode::Linearizable,
                    None,
                    true,
                    None,
                )
                .await?;
            if page.timed_out || (page.truncated && page.items.is_empty()) {
                return Err(Error::Server(
                    "legacy group identity observation is incomplete".into(),
                ));
            }
            for (path, _) in page.items {
                if !path.starts_with(prefix.as_bytes()) || path.as_ref() <= after.as_slice() {
                    return Err(Error::Server(
                        "legacy group identity cursor did not advance".into(),
                    ));
                }
                after = path.to_vec();
                count += 1;
                if count > 65_536 {
                    return Err(Error::Server(
                        "legacy group identity observation exceeds its bound".into(),
                    ));
                }
                let path = std::str::from_utf8(&path).map_err(|error| Error::Server(error.to_string()))?;
                let actual =
                    DiskGroupKey::from_path(path).map_err(|error| Error::Server(error.to_string()))?;
                maximum = maximum.max(actual.disk_group_id);
                if expected.is_some_and(|expected| {
                    actual.disk_group_id == expected.disk_group_id && actual != *expected
                }) {
                    return Err(Error::Server(GROUP_CONFLICT.into()));
                }
            }
            if !page.truncated {
                return Ok(maximum);
            }
        }
    }
}
