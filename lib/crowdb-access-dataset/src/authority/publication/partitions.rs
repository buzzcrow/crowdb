// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    authority::encode, key, AuthorityError, CasOutcome, DatasetAuthority, DatasetError, DatasetIdentity,
    ManifestBinding, ManifestRecord, SampleRecord, SchemaRecord, SnapshotId,
};

/// Serializes the established partition layout directly from the validated Manifest.
#[derive(serde::Serialize)]
struct BorrowedPartition<'a> {
    version: u16,
    snapshot: SnapshotId,
    partition: u32,
    schema: &'a SchemaRecord,
    samples: &'a [SampleRecord],
}

impl DatasetAuthority {
    pub(super) async fn persist_manifest_parts(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        manifest: &ManifestRecord,
    ) -> Result<u32, AuthorityError> {
        manifest.validate()?;
        let count = u32::try_from(manifest.samples.len().div_ceil(1024))
            .map_err(|_| DatasetError::InvalidManifest)?;
        if count == 0 {
            return Err(DatasetError::InvalidManifest.into());
        }
        for (index, samples) in manifest.samples.chunks(1024).enumerate() {
            let partition = u32::try_from(index).map_err(|_| DatasetError::InvalidManifest)?;
            let key = key::manifest_partition(identity, snapshot, partition)?;
            let bytes = encode(&BorrowedPartition {
                version: manifest.version,
                snapshot,
                partition,
                schema: &manifest.schema,
                samples,
            })?;
            self.create_manifest_row(&key, &bytes).await?;
        }
        let binding = encode(&ManifestBinding {
            snapshot,
            partitions: count,
        })?;
        self.create_manifest_row(&key::manifest_binding(identity, snapshot)?, &binding)
            .await?;
        Ok(count)
    }

    async fn create_manifest_row(&self, key: &[u8], bytes: &[u8]) -> Result<(), AuthorityError> {
        match self.store.compare_exchange(key, None, bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(existing)) if existing.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
    }
}
