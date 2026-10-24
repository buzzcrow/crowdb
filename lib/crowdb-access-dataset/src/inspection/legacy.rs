// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    index,
    preparation::{budget, summary_progress},
    InspectionError, InspectionPreparation, InspectionPreparationLimits, InspectionState,
};
use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, ManifestRecord, PublicationState, ReadCancellation,
    SnapshotId, SnapshotRecord,
};
use bincode::Options;
use serde::Deserialize;
use std::time::Duration;

const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;

#[derive(Deserialize)]
struct LegacyRecord<T> {
    version: u16,
    payload: T,
}

impl DatasetAuthority {
    /// Rebuilds a complete immutable projection from its original legacy Manifest.
    /// Failures leave no progress record; a retry repeats final row writes or reuses the ready root.
    /// # Errors
    /// Rejects unavailable sources, unsupported formats, corruption, cancellation and work limits.
    pub async fn prepare_inspection(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        limits: InspectionPreparationLimits,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPreparation, InspectionError> {
        limits.validate()?;
        if cancel.is_cancelled() {
            return Err(InspectionError::Cancelled);
        }
        let cancelled = async {
            while !cancel.is_cancelled() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            result = tokio::time::timeout(Duration::from_secs(30), self.rebuild_inspection(identity, snapshot, limits, cancel)) => result.map_err(|_| InspectionError::Limit)?,
            () = cancelled => Err(InspectionError::Cancelled),
        }
    }

    async fn rebuild_inspection(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        limits: InspectionPreparationLimits,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPreparation, InspectionError> {
        self.check_snapshot_readable(identity, snapshot).await?;
        if let Some(summary) = self.read_inspection_summary(identity, snapshot, cancel).await? {
            return Ok(summary_progress(&summary));
        }
        let (parent, manifest) = self
            .load_legacy_inspection(identity, snapshot, limits, cancel)
            .await?;
        let summary = index::summary(parent, &manifest)?;
        if summary.state == InspectionState::Ready {
            self.persist_inspection_rows(identity, &manifest).await?;
        }
        // Rows and discovery indexes precede this immutable readiness root. Cleanup
        // competes on the same keys and a delayed create cannot replace a retired value.
        self.finish_inspection_index(identity, &summary).await?;
        Ok(summary_progress(&summary))
    }

    async fn load_legacy_inspection(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        limits: InspectionPreparationLimits,
        cancel: &ReadCancellation,
    ) -> Result<(Option<SnapshotId>, ManifestRecord), InspectionError> {
        let legacy = self
            .store
            .get_bounded(
                &crate::key::snapshot(identity, snapshot).map_err(AuthorityError::from)?,
                budget(limits.source_bytes),
                cancel,
            )
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let record: LegacyRecord<SnapshotRecord> = decode_legacy(&legacy.bytes, limits.source_bytes)?;
        if record.version != 1 || record.payload.publication.snapshot != snapshot {
            return Err(InspectionError::Corrupt);
        }
        let publication = record.payload.publication;
        publication.validate().map_err(AuthorityError::from)?;
        if publication.state != PublicationState::Published {
            return Err(AuthorityError::SnapshotNotFound.into());
        }
        if publication.manifest.len() > MAX_MANIFEST_BYTES {
            return Err(InspectionError::Limit);
        }
        let mut manifest = decode_manifest(&publication.manifest, MAX_MANIFEST_BYTES).map_err(|_| {
            InspectionError::Unsupported("legacy publication does not contain a supported Manifest".into())
        })?;
        manifest.snapshot = snapshot;
        manifest.validate().map_err(AuthorityError::from)?;
        if manifest.samples.is_empty() {
            return Err(InspectionError::Corrupt);
        }
        Ok((publication.parent, manifest))
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
