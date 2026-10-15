// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, FieldMetadata, InspectionState, SnapshotSummary};
use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, FieldLocator, ManifestRecord, SnapshotId,
};
use sha2::{Digest, Sha256};

const MAX_ROW_BYTES: usize = 1024 * 1024;

impl DatasetAuthority {
    pub(crate) async fn persist_inspection_index(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest: &ManifestRecord,
    ) -> Result<(), AuthorityError> {
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
        if summary.state == InspectionState::Ready {
            self.persist_inspection_rows(identity, manifest).await?;
        }
        // The ready binding is installed only after every immutable row exists.
        self.put_inspection_row(&key::scope(identity, manifest.snapshot, b'H')?, &summary)
            .await?;
        self.put_inspection_row(&key::inventory(identity, manifest.snapshot)?, &summary)
            .await?;
        self.put_inspection_row(&key::child(identity, parent, manifest.snapshot)?, &summary)
            .await
    }

    async fn persist_inspection_rows(
        &self,
        identity: &DatasetIdentity,
        manifest: &ManifestRecord,
    ) -> Result<(), AuthorityError> {
        for (index, field) in manifest.schema.fields.iter().enumerate() {
            let mut key = key::scope(identity, manifest.snapshot, b'S')?;
            key.extend_from_slice(
                &u32::try_from(index)
                    .map_err(|_| AuthorityError::Corrupt)?
                    .to_be_bytes(),
            );
            self.put_inspection_row(&key, field).await?;
        }
        for sample in &manifest.samples {
            let mut key = key::scope(identity, manifest.snapshot, b'M')?;
            key::component(&mut key, &sample.sample_id);
            self.put_inspection_row(&key, &sample.sample_id).await?;
            for field in &sample.fields {
                let mut metadata_key = key::scope(identity, manifest.snapshot, b'F')?;
                key::component(&mut metadata_key, &sample.sample_id);
                key::component(&mut metadata_key, field.name.as_bytes());
                self.put_inspection_row(
                    &metadata_key,
                    &FieldMetadata::new(&field.name, &field.value, manifest.snapshot),
                )
                .await?;
                let mut value_key = key::scope(identity, manifest.snapshot, b'V')?;
                key::component(&mut value_key, &sample.sample_id);
                key::component(&mut value_key, field.name.as_bytes());
                self.put_inspection_row(&value_key, &field.value).await?;
            }
        }
        Ok(())
    }

    async fn put_inspection_row(
        &self,
        key: &[u8],
        value: &impl serde::Serialize,
    ) -> Result<(), AuthorityError> {
        let bytes = serde_json::to_vec(value).map_err(|_| AuthorityError::Corrupt)?;
        if bytes.len() > MAX_ROW_BYTES || key.len() >= 64 * 1024 {
            return Err(AuthorityError::Corrupt);
        }
        match self.store.compare_exchange(key, None, &bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(existing)) if existing.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
    }
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
