// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, InspectionState, SnapshotSummary};
use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, FieldLocator, ManifestRecord, SnapshotId,
};
use futures::{stream::FuturesUnordered, StreamExt};
use sha2::{Digest, Sha256};

use super::rows::{encode, next_row, EncodedRow, IndexPosition};

const MAX_PENDING_ROWS: usize = 16;
const MAX_PENDING_BYTES: usize = 2 * 1024 * 1024;

impl DatasetAuthority {
    pub(crate) async fn persist_inspection_index(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest: &ManifestRecord,
    ) -> Result<(), AuthorityError> {
        let summary = summary(parent, manifest)?;
        if summary.state == InspectionState::Ready {
            self.persist_inspection_rows(identity, manifest).await?;
        }
        self.finish_inspection_index(identity, &summary).await
    }

    pub(super) async fn finish_inspection_index(
        &self,
        identity: &DatasetIdentity,
        summary: &SnapshotSummary,
    ) -> Result<(), AuthorityError> {
        self.put_inspection_row(&key::inventory(identity, summary.snapshot)?, summary)
            .await?;
        self.put_inspection_row(&key::child(identity, summary.parent, summary.snapshot)?, summary)
            .await?;
        self.put_inspection_row(&key::scope(identity, summary.snapshot, b'H')?, summary)
            .await
    }

    async fn persist_inspection_rows(
        &self,
        identity: &DatasetIdentity,
        manifest: &ManifestRecord,
    ) -> Result<(), AuthorityError> {
        let mut position = IndexPosition::default();
        let mut pending = FuturesUnordered::new();
        let mut pending_bytes = 0;
        while let Some(row) = next_row(identity, manifest, &mut position)? {
            let size = row.key.len() + row.bytes.len();
            while pending.len() >= MAX_PENDING_ROWS || pending_bytes + size > MAX_PENDING_BYTES {
                let (size, result) = pending.next().await.ok_or(AuthorityError::Corrupt)?;
                pending_bytes -= size;
                result?;
            }
            pending_bytes += size;
            pending.push(async move { (size, self.put_inspection_bytes(row).await) });
        }
        while let Some((_, result)) = pending.next().await {
            result?;
        }
        Ok(())
    }

    async fn put_inspection_row(
        &self,
        key: &[u8],
        value: &impl serde::Serialize,
    ) -> Result<(), AuthorityError> {
        self.put_inspection_bytes(encode(key.to_vec(), value)?).await
    }

    pub(super) async fn put_inspection_bytes(&self, row: EncodedRow) -> Result<(), AuthorityError> {
        let EncodedRow { key, bytes } = row;
        match self.store.compare_exchange(&key, None, &bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(existing)) if existing.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
    }
}

pub(super) fn summary(
    parent: Option<SnapshotId>,
    manifest: &ManifestRecord,
) -> Result<SnapshotSummary, AuthorityError> {
    let state = if supported(manifest) {
        InspectionState::Ready
    } else {
        InspectionState::Unsupported {
            reason: "manifest exceeds inspection identity or schema limits".into(),
        }
    };
    let mut digest = ManifestDigest(Sha256::new());
    bincode::serialize_into(&mut digest, manifest).map_err(|_| AuthorityError::Corrupt)?;
    let digest = digest.0.finalize().into();
    let summary = SnapshotSummary {
        version: 1,
        snapshot: manifest.snapshot,
        parent,
        schema_version: manifest.schema.version,
        field_count: u32::try_from(manifest.schema.fields.len()).map_err(|_| AuthorityError::Corrupt)?,
        sample_count: manifest.samples.len() as u64,
        manifest_sha256: digest,
        state,
    };
    Ok(summary)
}

fn supported(manifest: &ManifestRecord) -> bool {
    manifest.schema.fields.len() <= 256
        && manifest
            .schema
            .fields
            .iter()
            .all(|field| field.name.len() <= 1024)
        && manifest.samples.iter().all(|sample| {
            sample.sample_id.len() <= 16 * 1024
                && sample.fields.iter().all(|field| match &field.value {
                    FieldLocator::Chunk { location, .. } => location.len() <= 64 * 1024,
                    _ => true,
                })
        })
}

struct ManifestDigest(Sha256);
impl std::io::Write for ManifestDigest {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
