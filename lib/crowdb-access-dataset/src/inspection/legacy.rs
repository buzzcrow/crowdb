// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    index, key,
    preparation::{budget, decode_preparation, summary_progress, PreparationRecord},
    rows::{next_row, IndexPosition},
    InspectionError, InspectionPreparation, InspectionState, PreparationLimits, PreparationState,
};
use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, ManifestRecord, OperationId,
    PublicationState, ReadCancellation, SnapshotId, SnapshotPublication, SnapshotRecord,
};
use bincode::Options;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Deserialize)]
struct LegacyRecord<T> {
    version: u16,
    payload: T,
}

struct LegacyInput {
    publication: SnapshotPublication,
    source_sha256: [u8; 32],
}

impl DatasetAuthority {
    /// Builds a bounded, durable window of inspection metadata for one old Snapshot.
    /// # Errors
    /// Rejects unavailable snapshots, corrupt records, limits and cancellation.
    pub async fn prepare_inspection_page(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        limits: PreparationLimits,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPreparation, InspectionError> {
        limits.validate()?;
        if cancel.is_cancelled() {
            return Err(InspectionError::Cancelled);
        }
        // Old published records gain a small visibility marker only with head/ancestry proof.
        match tokio::time::timeout(
            Duration::from_secs(2),
            self.require_committed_snapshot(identity, snapshot),
        )
        .await
        .map_err(|_| InspectionError::Limit)?
        {
            Ok(()) | Err(AuthorityError::SnapshotNotFound) => {}
            Err(error) => return Err(error.into()),
        }
        match self
            .inspection_preparation_status(identity, snapshot, cancel)
            .await
        {
            Ok(progress) if progress.state != PreparationState::Preparing => return Ok(progress),
            Ok(_) | Err(InspectionError::NotPrepared) => {}
            Err(error) => return Err(error),
        }
        let source = self
            .load_legacy_inspection(identity, snapshot, limits, cancel)
            .await?;
        let lease = OperationId::random();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        tokio::time::timeout(
            Duration::from_secs(2),
            self.touch_read_handle(identity, snapshot, lease, now, 60),
        )
        .await
        .map_err(|_| InspectionError::Limit)??;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            self.prepare_inspection_inner(identity, snapshot, limits, cancel, source),
        )
        .await;
        let released = tokio::time::timeout(
            Duration::from_secs(2),
            self.release_read_handle(identity, snapshot, lease),
        )
        .await;
        let progress = result.map_err(|_| InspectionError::Limit)??;
        released.map_err(|_| InspectionError::Limit)??;
        Ok(progress)
    }

    async fn prepare_inspection_inner(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        limits: PreparationLimits,
        cancel: &ReadCancellation,
        source: LegacyInput,
    ) -> Result<InspectionPreparation, InspectionError> {
        let LegacyInput {
            publication,
            source_sha256,
        } = source;
        let progress_key = key::scope(identity, snapshot, b'P').map_err(AuthorityError::from)?;
        let old = self
            .store
            .get_bounded(&progress_key, budget(16 * 1024), cancel)
            .await?;
        let mut record = if let Some(old) = &old {
            let record = decode_preparation(&old.bytes, snapshot)?;
            if record.source_sha256 != source_sha256 {
                return Err(InspectionError::Corrupt);
            }
            record
        } else {
            PreparationRecord {
                version: 1,
                source_sha256,
                position: IndexPosition::default(),
                progress: InspectionPreparation {
                    snapshot,
                    revision: 0,
                    rows_written: 0,
                    state: PreparationState::Preparing,
                },
            }
        };
        self.advance_legacy_inspection(identity, &publication, &mut record, limits, cancel)
            .await?;
        if cancel.is_cancelled() {
            return Err(InspectionError::Cancelled);
        }
        record.progress.revision = record
            .progress
            .revision
            .checked_add(1)
            .ok_or(InspectionError::Corrupt)?;
        let bytes = serde_json::to_vec(&record).map_err(|_| InspectionError::Corrupt)?;
        match self
            .store
            .compare_exchange(
                &progress_key,
                old.as_ref().map(|value| value.bytes.as_slice()),
                &bytes,
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(record.progress),
            CasOutcome::Conflict(Some(value)) => {
                let winner = decode_preparation(&value.bytes, snapshot)?;
                if winner.source_sha256 != source_sha256 {
                    return Err(InspectionError::Corrupt);
                }
                Ok(winner.progress)
            }
            CasOutcome::Conflict(None) => Err(InspectionError::Corrupt),
        }
    }

    async fn load_legacy_inspection(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        limits: PreparationLimits,
        cancel: &ReadCancellation,
    ) -> Result<LegacyInput, InspectionError> {
        let legacy = self
            .store
            .get_bounded(
                &crate::key::snapshot(identity, snapshot).map_err(AuthorityError::from)?,
                budget(limits.legacy_bytes),
                cancel,
            )
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let source_sha256: [u8; 32] = Sha256::digest(&legacy.bytes).into();
        let snapshot_record: LegacyRecord<SnapshotRecord> =
            decode_legacy(&legacy.bytes, limits.legacy_bytes)?;
        if snapshot_record.version != 1 || snapshot_record.payload.publication.snapshot != snapshot {
            return Err(InspectionError::Corrupt);
        }
        let publication = snapshot_record.payload.publication;
        if publication.state != PublicationState::Published {
            return Err(AuthorityError::SnapshotNotFound.into());
        }
        Ok(LegacyInput {
            publication,
            source_sha256,
        })
    }

    async fn advance_legacy_inspection(
        &self,
        identity: &DatasetIdentity,
        publication: &SnapshotPublication,
        record: &mut PreparationRecord,
        limits: PreparationLimits,
        cancel: &ReadCancellation,
    ) -> Result<(), InspectionError> {
        let snapshot = publication.snapshot;
        let manifest = decode_manifest(&publication.manifest, limits.legacy_bytes);
        match manifest {
            Ok(mut manifest) => {
                manifest.snapshot = snapshot;
                manifest.validate().map_err(AuthorityError::from)?;
                if manifest.samples.is_empty() {
                    return Err(InspectionError::Corrupt);
                }
                record.position.validate(&manifest)?;
                let summary = index::summary(publication.parent, &manifest)?;
                if summary.state == InspectionState::Ready {
                    self.write_inspection_window(identity, &manifest, record, limits, cancel)
                        .await?;
                    if record.progress.state == PreparationState::Ready {
                        self.finish_inspection_index(identity, &summary).await?;
                    }
                } else {
                    record.progress = summary_progress(&summary);
                    self.finish_inspection_index(identity, &summary).await?;
                }
            }
            Err(_) => {
                record.progress.state = PreparationState::Unsupported {
                    reason: "legacy publication does not contain a supported Manifest".into(),
                }
            }
        }
        Ok(())
    }

    async fn write_inspection_window(
        &self,
        identity: &DatasetIdentity,
        manifest: &ManifestRecord,
        record: &mut PreparationRecord,
        limits: PreparationLimits,
        cancel: &ReadCancellation,
    ) -> Result<(), InspectionError> {
        let mut bytes = 0usize;
        for _ in 0..limits.rows {
            if cancel.is_cancelled() {
                return Err(InspectionError::Cancelled);
            }
            let mut next = record.position.clone();
            let Some(row) = next_row(identity, manifest, &mut next)? else {
                record.progress.state = PreparationState::Ready;
                break;
            };
            let size = row.key.len().saturating_add(row.bytes.len());
            if size > limits.bytes {
                return Err(InspectionError::Limit);
            }
            if size > limits.bytes - bytes {
                break;
            }
            self.put_inspection_bytes(row).await?;
            bytes += size;
            record.position = next;
            record.progress.rows_written = record.position.rows;
        }
        Ok(())
    }
}

fn decode_legacy<T: serde::de::DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, InspectionError> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(limit as u64)
        .reject_trailing_bytes()
        .deserialize(bytes)
        .map_err(|_| InspectionError::Corrupt)
}
fn decode_manifest(bytes: &[u8], limit: usize) -> Result<ManifestRecord, InspectionError> {
    if let Ok(manifest) = decode_legacy(bytes, limit) {
        return Ok(manifest);
    }
    let versioned: LegacyRecord<ManifestRecord> = decode_legacy(bytes, limit)?;
    if versioned.version != 1 {
        return Err(InspectionError::Corrupt);
    }
    Ok(versioned.payload)
}
