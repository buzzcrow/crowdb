// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode, DatasetAuthority, VersionedRecord};
mod cleanup;
mod legacy_progress;
mod lifecycle;
mod observation;
pub use lifecycle::{
    SnapshotReclaimCursor, SnapshotReclaimError, SnapshotReclaimPhase, SnapshotReclaimStatus,
};
pub use observation::SnapshotLifecycle;
mod reclaim;
mod sweep;
use crate::{key, AuthorityError, CasOutcome, DatasetIdentity, OperationId, SnapshotId};
pub use cleanup::MetadataCleanupProgress;
use serde::{Deserialize, Serialize};
pub use sweep::SnapshotReclaimSweep;

/// Default delay between logical deletion and destructive cleanup.
const RECLAIM_GRACE_SECONDS: u64 = 300;

#[derive(Default, Serialize, Deserialize)]
struct Access {
    reclaiming: bool,
    reclaim_owner: Option<OperationId>,
    retiring: bool,
    reclaim_after: u64,
    target: Option<(DatasetIdentity, SnapshotId)>,
    cleanup_complete: bool,
}
#[derive(Deserialize)]
struct AccessV2 {
    reclaiming: bool,
    _legacy: u64,
    _deadline: u64,
    _leases: Vec<(OperationId, u64)>,
}
#[derive(Deserialize)]
struct AccessV3 {
    reclaiming: bool,
    _legacy: u64,
    _deadline: u64,
    _leases: Vec<(OperationId, u64)>,
    reclaim_owner: Option<OperationId>,
    retiring: bool,
}
fn decode_access(bytes: &[u8]) -> Result<Access, AuthorityError> {
    let version = bytes.get(..2).ok_or(AuthorityError::Corrupt)?;
    let access = match u16::from_le_bytes([version[0], version[1]]) {
        1 => {
            let _: crate::ActiveReadLease = decode(bytes)?;
            Access::default()
        }
        2 => {
            let old: VersionedRecord<AccessV2> =
                bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
            Access {
                reclaiming: old.payload.reclaiming,
                ..Access::default()
            }
        }
        3 => {
            let old: VersionedRecord<AccessV3> =
                bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
            Access {
                reclaiming: old.payload.reclaiming,
                reclaim_owner: old.payload.reclaim_owner,
                retiring: old.payload.retiring,
                ..Access::default()
            }
        }
        4 => {
            let record: VersionedRecord<Access> =
                bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
            record.payload
        }
        _ => return Err(AuthorityError::Corrupt),
    };
    if access.retiring && !access.reclaiming {
        return Err(AuthorityError::Corrupt);
    }
    Ok(access)
}
fn encode_access(record: Access) -> Result<Vec<u8>, AuthorityError> {
    bincode::serialize(&VersionedRecord {
        version: 4,
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
        let mut old = self.store.get(&key).await?;
        for _ in 0..64 {
            let mut access = old
                .as_ref()
                .map(|v| decode_access(&v.bytes))
                .transpose()?
                .unwrap_or_default();
            change(&mut access)?;
            access.target = Some((identity.clone(), snapshot));
            let bytes = encode_access(access)?;
            if old.as_ref().is_some_and(|v| v.bytes == bytes) {
                return Ok(());
            }
            match self
                .store
                .compare_exchange(&key, old.as_ref().map(|v| v.bytes.as_slice()), &bytes)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                // Recompute against the sequencer's observation without opening another
                // point-read race against concurrent lifecycle changes.
                CasOutcome::Conflict(observed) => old = observed,
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
    /// Checks authoritative admission once without registering or renewing a reader.
    /// Requests admitted before deletion may continue until storage is reclaimed.
    /// The caller resolves snapshot existence and publication separately.
    /// # Errors
    /// Returns `SnapshotNotFound` for deletion fences, or storage/decoding failures.
    pub async fn check_snapshot_readable(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        if let Some(value) = self.store.get(&key::read_lease(identity, snapshot)?).await? {
            if decode_access(&value.bytes)?.reclaiming {
                return Err(AuthorityError::SnapshotNotFound);
            }
        }
        Ok(())
    }

    /// Returns the durable deadline after a successful deletion mark.
    /// # Errors
    /// Rejects snapshots without a terminal deletion mark or malformed storage.
    pub async fn snapshot_reclaim_after(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<u64, AuthorityError> {
        let value = self
            .store
            .get(&key::read_lease(identity, snapshot)?)
            .await?
            .ok_or(AuthorityError::SnapshotProtected)?;
        let access = decode_access(&value.bytes)?;
        if !access.retiring {
            return Err(AuthorityError::SnapshotProtected);
        }
        Ok(access.reclaim_after)
    }

    pub(crate) async fn require_reclaim_ready(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        let reclaim_after = self.snapshot_reclaim_after(identity, snapshot).await?;
        if self.reclaim_time() < reclaim_after {
            return Err(AuthorityError::ReclaimPending { reclaim_after });
        }
        Ok(())
    }

    /// Already-readable resources wait for deletion grace; sealed incomplete roots have no readers.
    pub(crate) async fn require_legacy_scratch_reclaim_ready(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        cancel: &crate::ReadCancellation,
    ) -> Result<(), AuthorityError> {
        let stored = self
            .store
            .get_bounded(
                &key::read_lease(identity, snapshot)?,
                crate::MetadataScanBudget {
                    items: 1,
                    bytes: 256 * 1024,
                    timeout: std::time::Duration::from_secs(2),
                },
                cancel,
            )
            .await?;
        if let Some(value) = stored {
            let access = decode_access(&value.bytes)?;
            if access.retiring {
                return if self.reclaim_time() < access.reclaim_after {
                    Err(AuthorityError::ReclaimPending {
                        reclaim_after: access.reclaim_after,
                    })
                } else {
                    Ok(())
                };
            }
        }
        if self.seal_incomplete_snapshot(identity, snapshot).await? {
            Err(AuthorityError::SnapshotProtected)
        } else {
            Ok(())
        }
    }

    // The per-authority clock override exists only in test builds.
    #[cfg_attr(not(feature = "test-util"), allow(clippy::unused_self))]
    fn reclaim_time(&self) -> u64 {
        #[cfg(feature = "test-util")]
        if let Some(clock) = &self.reclaim_clock {
            return clock.load(std::sync::atomic::Ordering::Acquire);
        }
        now()
    }
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn with_reclaim_clock_for_tests(
        mut self,
        clock: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        self.reclaim_clock = Some(clock);
        self
    }
}

pub(super) fn is_reclaiming(bytes: &[u8]) -> Result<bool, AuthorityError> {
    Ok(decode_access(bytes)?.reclaiming)
}
