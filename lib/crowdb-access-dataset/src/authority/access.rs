// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode, DatasetAuthority, VersionedRecord};
mod cleanup;
mod lifecycle;
pub use lifecycle::{SnapshotReclaimError, SnapshotReclaimPhase, SnapshotReclaimStatus};
mod reclaim;
use crate::{key, AuthorityError, CasOutcome, DatasetError, DatasetIdentity, OperationId, SnapshotId};
pub use cleanup::MetadataCleanupProgress;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct Access {
    reclaiming: bool,
    legacy: u64,
    deadline: u64,
    leases: Vec<(OperationId, u64)>,
    reclaim_owner: Option<OperationId>,
    retiring: bool,
}
#[derive(Deserialize)]
struct AccessV2 {
    reclaiming: bool,
    legacy: u64,
    deadline: u64,
    leases: Vec<(OperationId, u64)>,
}
fn decode_access(bytes: &[u8]) -> Result<Access, AuthorityError> {
    if bytes.get(..2) == Some(1u16.to_le_bytes().as_slice()) {
        let old: crate::ActiveReadLease = decode(bytes)?;
        return Ok(Access {
            legacy: old.active,
            deadline: old.deadline,
            ..Access::default()
        });
    }
    if bytes.get(..2) == Some(2u16.to_le_bytes().as_slice()) {
        let record: VersionedRecord<AccessV2> =
            bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
        let old = record.payload;
        return Ok(Access {
            reclaiming: old.reclaiming,
            legacy: old.legacy,
            deadline: old.deadline,
            leases: old.leases,
            ..Access::default()
        });
    }
    let record: VersionedRecord<Access> = bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
    if record.version != 3 || (record.payload.retiring && !record.payload.reclaiming) {
        return Err(AuthorityError::Corrupt);
    }
    Ok(record.payload)
}
fn encode_access(record: Access) -> Result<Vec<u8>, AuthorityError> {
    bincode::serialize(&VersionedRecord {
        version: 3,
        payload: record,
    })
    .map_err(|_| AuthorityError::Corrupt)
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
impl DatasetAuthority {
    async fn update_access(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        mut change: impl FnMut(&mut Access) -> Result<(), AuthorityError>,
    ) -> Result<(), AuthorityError> {
        let key = key::read_lease(identity, snapshot)?;
        for _ in 0..64 {
            let old = self.store.get(&key).await?;
            let mut access = old
                .as_ref()
                .map(|v| decode_access(&v.bytes))
                .transpose()?
                .unwrap_or_default();
            change(&mut access)?;
            let bytes = encode_access(access)?;
            if matches!(
                self.store
                    .compare_exchange(&key, old.as_ref().map(|v| v.bytes.as_slice()), &bytes)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(AuthorityError::HeadConflict)
    }
    pub(super) async fn ensure_snapshot_live(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        if let Some(value) = self.store.get(&key::read_lease(identity, snapshot)?).await? {
            if decode_access(&value.bytes)?.reclaiming {
                return Err(AuthorityError::SnapshotProtected);
            }
        }
        Ok(())
    }
    /// Acquires legacy aggregate activity, preserving older persisted records.
    /// # Errors
    /// Returns expired/fenced snapshots, invalid TTL or storage failures.
    pub async fn acquire_snapshot_read(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        time: u64,
        ttl: u64,
    ) -> Result<(), AuthorityError> {
        if ttl == 0 {
            return Err(DatasetError::InvalidManifest.into());
        }
        self.update_access(identity, snapshot, |a| {
            if a.reclaiming {
                return Err(AuthorityError::SnapshotProtected);
            }
            if time >= a.deadline {
                a.legacy = 0;
            }
            a.legacy = a.legacy.checked_add(1).ok_or(AuthorityError::Corrupt)?;
            a.deadline = time.saturating_add(ttl);
            Ok(())
        })
        .await
    }
    /// # Errors
    /// Returns malformed or unavailable storage.
    pub async fn release_snapshot_read(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        self.update_access(identity, snapshot, |a| {
            a.legacy = a.legacy.saturating_sub(1);
            Ok(())
        })
        .await
    }
    /// # Errors
    /// Returns expired activity or unavailable storage.
    pub async fn renew_snapshot_read(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        time: u64,
        ttl: u64,
    ) -> Result<(), AuthorityError> {
        self.update_access(identity, snapshot, |a| {
            if a.reclaiming || a.legacy == 0 || time >= a.deadline {
                return Err(DatasetError::ReadCancelled.into());
            }
            a.deadline = time.saturating_add(ttl);
            Ok(())
        })
        .await
    }
    /// Acquires/renews one independently owned lease; another handle cannot release it.
    /// # Errors
    /// Returns fenced snapshots, exhausted capacity or unavailable storage.
    pub async fn touch_read_handle(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        id: OperationId,
        time: u64,
        ttl: u64,
    ) -> Result<(), AuthorityError> {
        if ttl == 0 {
            return Err(DatasetError::InvalidManifest.into());
        }
        self.update_access(identity, snapshot, |a| {
            if a.reclaiming {
                return Err(AuthorityError::SnapshotProtected);
            }
            a.leases.retain(|(_, deadline)| *deadline > time);
            if let Some((_, deadline)) = a.leases.iter_mut().find(|(lease, _)| *lease == id) {
                *deadline = time.saturating_add(ttl);
            } else {
                if a.leases.len() >= 4096 {
                    return Err(DatasetError::ReadLimitExceeded.into());
                }
                a.leases.push((id, time.saturating_add(ttl)));
            }
            Ok(())
        })
        .await
    }
    /// # Errors
    /// Returns malformed or unavailable storage.
    pub async fn release_read_handle(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        id: OperationId,
    ) -> Result<(), AuthorityError> {
        self.update_access(identity, snapshot, |a| {
            a.leases.retain(|(lease, _)| *lease != id);
            Ok(())
        })
        .await
    }
}

pub(super) fn is_reclaiming(bytes: &[u8]) -> Result<bool, AuthorityError> {
    Ok(decode_access(bytes)?.reclaiming)
}
