// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Root retention for fixed consumer and reader views.

use crowdb_protocol::chunk_stream::{
    decode_stream_root_pin, StreamDirectoryControl, StreamRootPin, StreamRootPinId, MAX_STREAM_ROOT_PINS,
    MAX_STREAM_ROOT_PIN_LEASE_MS,
};
use crowdb_protocol::key::{BinaryKey, StreamRootPinKey};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{kv_error, DirectoryKeyScan, DirectoryMetadataStore, GetOutcome};
use crate::{Result, StreamError, StreamName, MAX_DIRECTORY_SCAN_KEYS};

mod lifecycle;
mod publication;

/// Metadata proof, not an IO lease. A reader must separately enforce its local
/// conservative deadline. Revisions prevent stale renewal/release, including ABA.
#[derive(Clone, Debug)]
pub struct DirectoryRootPinSnapshot {
    pin: StreamRootPin,
    revision: u64,
    store_id: u64,
}

impl DirectoryRootPinSnapshot {
    #[must_use]
    pub const fn pin(&self) -> &StreamRootPin {
        &self.pin
    }
}

impl DirectoryMetadataStore {
    /// Loads a pin for diagnostics or consumer restoration; a leased pin may
    /// already be expired and this method grants no authority to read data.
    /// # Errors
    /// Rejects malformed records, scope errors or unavailable metadata.
    pub async fn load_directory_pin(
        &self,
        name: StreamName,
        id: StreamRootPinId,
    ) -> Result<Option<DirectoryRootPinSnapshot>> {
        let key = StreamRootPinKey {
            stream_name: name,
            pin_id: id,
        }
        .to_bytes();
        match self.records.get(&key).await.map_err(kv_error)? {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, revision } => {
                let pin = decode_stream_root_pin(&value, name, id)
                    .map_err(|error| StreamError::Corruption(error.to_string()))?;
                if pin.head.metadata_group_id != self.group_id {
                    return Err(StreamError::Corruption(
                        "root pin metadata group is invalid".into(),
                    ));
                }
                Ok(Some(DirectoryRootPinSnapshot {
                    pin,
                    revision,
                    store_id: self.store_id,
                }))
            }
        }
    }

    /// Pins a still-retained published root and its exact read interval. The
    /// control revision orders this operation against root retirement and GC.
    /// # Errors
    /// Rejects unavailable roots, conflicting identities, expired leases or budgets.
    pub async fn pin_directory_root(&self, pin: StreamRootPin) -> Result<DirectoryRootPinSnapshot> {
        pin.validate()
            .map_err(|error| StreamError::InvalidRequest(error.to_string()))?;
        if pin.head.metadata_group_id != self.group_id {
            return Err(StreamError::InvalidRequest(
                "root pin metadata group differs from binding".into(),
            ));
        }
        validate_lease(&pin, now_ms())?;
        let (control, revision) = self.load_directory_control(pin_scope(&pin)).await?;
        if revision == 0 {
            return Err(StreamError::ReadUnavailable(
                "root retention control is missing".into(),
            ));
        }
        if let Some(current) = self.load_directory_pin(pin.head.stream_name, pin.id).await? {
            if current.pin != pin {
                return Err(StreamError::InvalidRequest(
                    "pin identity is already in use".into(),
                ));
            }
            ensure_live(&pin)?;
            return Ok(current);
        }
        let root = self
            .load_directory_root(pin.head.stream_name, pin.head.identity())
            .await?
            .ok_or_else(|| StreamError::ReadUnavailable("requested root has been retired".into()))?;
        if root != pin.head {
            return Err(StreamError::InvalidRequest(
                "pin differs from retained root".into(),
            ));
        }
        self.check_directory_pin_budget(pin.head.stream_name).await?;
        let saved = self
            .write_directory_pin(control, revision, &pin, Some(&pin))
            .await?
            .ok_or_else(|| StreamError::Internal("pin creation returned no record".into()))?;
        ensure_live(saved.pin())?;
        Ok(saved)
    }

    /// Lists pin identities without loading their root descriptors.
    /// # Errors
    /// Rejects invalid bounds, malformed keys and unavailable scans.
    pub async fn scan_directory_pins(
        &self,
        name: StreamName,
        after: Option<StreamRootPinId>,
        limit: usize,
    ) -> Result<DirectoryKeyScan<StreamRootPinId>> {
        let prefix = StreamRootPinKey::stream_prefix(name);
        let after = after.map(|pin_id| {
            StreamRootPinKey {
                stream_name: name,
                pin_id,
            }
            .to_bytes()
        });
        self.scan_directory_ids::<StreamRootPinKey, _>(&prefix, after.as_deref(), limit, |key| {
            if key.stream_name != name || key.pin_id.0 == [0; 16] {
                return Err(StreamError::Corruption("root pin key is invalid".into()));
            }
            Ok(key.pin_id)
        })
        .await
    }

    async fn check_directory_pin_budget(&self, name: StreamName) -> Result<()> {
        let mut count = 0;
        let mut after = None;
        loop {
            let page = self
                .scan_directory_pins(name, after, MAX_DIRECTORY_SCAN_KEYS)
                .await?;
            count += page.entries.len();
            if count >= MAX_STREAM_ROOT_PINS {
                return Err(StreamError::Backpressure);
            }
            let Some(next) = page.next else { return Ok(()) };
            after = Some(next);
        }
    }

    fn validate_pin_snapshot(&self, snapshot: &DirectoryRootPinSnapshot) -> Result<()> {
        if snapshot.store_id != self.store_id || snapshot.pin.head.metadata_group_id != self.group_id {
            return Err(StreamError::InvalidRequest(
                "pin snapshot belongs to another metadata binding".into(),
            ));
        }
        Ok(())
    }
}

fn pin_scope(pin: &StreamRootPin) -> StreamDirectoryControl {
    StreamDirectoryControl {
        stream_name: pin.head.stream_name,
        purpose: pin.head.purpose,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn validate_lease(pin: &StreamRootPin, now: u64) -> Result<()> {
    if let Some(expiry) = pin.expires_at_ms {
        if expiry <= now {
            return Err(StreamError::ReadUnavailable("root pin lease has expired".into()));
        }
        if expiry - now > MAX_STREAM_ROOT_PIN_LEASE_MS {
            return Err(StreamError::InvalidRequest(
                "root pin lease exceeds the allowed lifetime".into(),
            ));
        }
    }
    Ok(())
}

fn ensure_live(pin: &StreamRootPin) -> Result<()> {
    if pin.expires_at_ms.is_some_and(|expiry| expiry <= now_ms()) {
        return Err(StreamError::ReadUnavailable(
            "root pin lease expired during metadata IO".into(),
        ));
    }
    Ok(())
}
