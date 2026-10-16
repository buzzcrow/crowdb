// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reserve before Rack mutation so concurrent nodes cannot publish one disk ID.
use super::{mutation, HardwareClient};
use crate::{Error, GetOutcome, ReadMode, Result};
use crowdb_protocol::{
    disk_storage::DiskIdReservation,
    key::{DiskKey, TextKey},
};

impl HardwareClient {
    /// Reserve an ID at one immutable location, including compatibility records.
    /// Reservations never authorize membership or release on Remove. A rejected
    /// or interrupted admission may leave only its identity reservation.
    ///
    /// # Errors
    /// Rejects IDs retained at another location, unsupported reservations and
    /// incomplete legacy verification. Same-location retries are read-only.
    pub async fn reserve_disk_id(&self, key: &DiskKey) -> Result<()> {
        let path = DiskIdReservation::key_for(&key.disk_id);
        let expected = DiskIdReservation::new(key);
        for _ in 0..10 {
            if let GetOutcome::Found { value, .. } = self
                .kv
                .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                let current: DiskIdReservation = mutation::decode(&path, &value)?;
                let placement = current.validate().map_err(|error| Error::Server(error.into()))?;
                if current != expected || placement != *key {
                    return Err(Error::Server(
                        "disk ID is already reserved at another physical location".into(),
                    ));
                }
                return Ok(());
            }
            self.verify_legacy_disk_id(key).await?;
            let value = serde_json::to_vec(&expected).map_err(|error| Error::Server(error.to_string()))?;
            match self.kv.put_cas(0, 0, path.as_bytes(), &value, 0).await {
                Ok(_) => return Ok(()),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }

    async fn verify_legacy_disk_id(&self, expected: &DiskKey) -> Result<()> {
        let mut count = 0;
        for prefix in ["/hw/disk/", "/hw/disk-storage/"] {
            let mut after = Vec::new();
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
                if page.timed_out || page.items.len() > 128 || (page.truncated && page.items.is_empty()) {
                    return Err(Error::Server("legacy disk identity scan is incomplete".into()));
                }
                for (key, _) in &page.items {
                    if !key.starts_with(prefix.as_bytes()) || key.as_ref() <= after.as_slice() {
                        return Err(Error::Server(
                            "legacy disk identity cursor did not advance".into(),
                        ));
                    }
                    after = key.to_vec();
                    count += 1;
                    if count > 65_536 {
                        return Err(Error::Server(
                            "legacy disk identity verification exceeds migration bound".into(),
                        ));
                    }
                    let path = std::str::from_utf8(key).map_err(|error| Error::Server(error.to_string()))?;
                    let disk = DiskKey::from_path(&path.replacen("/hw/disk-storage/", "/hw/disk/", 1))
                        .map_err(|error| Error::Server(error.to_string()))?;
                    if disk.disk_id == expected.disk_id && disk != *expected {
                        return Err(Error::Server(
                            "disk ID exists at another legacy or retained location".into(),
                        ));
                    }
                }
                if !page.truncated {
                    break;
                }
            }
        }
        Ok(())
    }
}
