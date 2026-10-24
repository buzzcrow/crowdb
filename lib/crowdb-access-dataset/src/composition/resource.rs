// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Complete caller-owned composition inputs and final immutable resources.

mod input;
mod source;

use super::{CompositionError, CompositionSource, SampleChange};
use crate::{
    split::{DraftSplit, SplitPolicy, SplitPreparation},
    AuthorityError, DatasetAuthority, DatasetIdentity, DatasetUploads, FieldLocator, ManifestRecord,
    OperationId, ReadCancellation, SchemaRecord, SnapshotId, SplitRevisionId, UploadError,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};

const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;

/// One complete computation, with frozen source identities and no editing revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionRequest {
    pub parent: Option<SnapshotId>,
    pub source: CompositionSource,
    pub schema: Option<SchemaRecord>,
    pub samples: Vec<SampleChange>,
    pub splits: Vec<DraftSplit>,
    pub policy: SplitPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompositionResult {
    pub snapshot: SnapshotId,
    pub initial_split: Option<SplitRevisionId>,
    pub sample_count: u64,
}

impl DatasetAuthority {
    /// Recomputes caller-owned inputs and completes the immutable Snapshot after its dependencies.
    /// It writes no draft, edit receipt, evaluation progress or operation-to-Snapshot index.
    /// # Errors
    /// Rejects divergent resource content, unavailable sources, invalid rules, payloads and limits.
    pub async fn prepare_composition_resource(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        request: &CompositionRequest,
        uploads: Option<&DatasetUploads>,
        cancel: &ReadCancellation,
    ) -> Result<CompositionResult, CompositionError> {
        request.validate(identity, snapshot)?;
        tokio::time::timeout(
            Duration::from_secs(30),
            self.compute_composition_resource(identity, snapshot, request, uploads, cancel),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }

    async fn compute_composition_resource(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        request: &CompositionRequest,
        uploads: Option<&DatasetUploads>,
        cancel: &ReadCancellation,
    ) -> Result<CompositionResult, CompositionError> {
        cancelled(cancel)?;
        let schema = self.composition_resource_schema(identity, request).await?;
        self.validate_split_parent_sources(identity, request.parent, &request.splits)
            .await?;
        let manifest = self
            .composition_resource_manifest(identity, snapshot, request, schema, cancel)
            .await?;
        let references = referenced_uploads(identity, &manifest)?;
        validate_uploads(uploads, &references, cancel).await?;
        let operation = OperationId::from_bytes(snapshot.as_bytes()).map_err(AuthorityError::from)?;
        let initial_split = if request.splits.is_empty() {
            None
        } else {
            Some(crate::split::candidate_revision(operation, 1)?)
        };
        let bytes = bincode::serialize(&manifest).map_err(|_| CompositionError::Corrupt)?;
        self.prepare_composition_content(identity, request.parent, bytes, snapshot, initial_split)
            .await?;
        if let Some(revision) = initial_split {
            self.prepare_initial_split_revision(
                identity,
                snapshot,
                &SplitPreparation {
                    revision,
                    policy: request.policy,
                    splits: request.splits.clone(),
                },
                cancel,
            )
            .await?;
        }
        claim_uploads(uploads, &references, operation, cancel).await?;
        cancelled(cancel)?;
        self.complete_snapshot_candidate(identity, snapshot).await?;
        Ok(CompositionResult {
            snapshot,
            initial_split,
            sample_count: u64::try_from(manifest.samples.len()).map_err(|_| CompositionError::Limit)?,
        })
    }

    async fn composition_resource_schema(
        &self,
        identity: &DatasetIdentity,
        request: &CompositionRequest,
    ) -> Result<SchemaRecord, CompositionError> {
        let parent = match request.parent {
            Some(parent) => Some(self.composition_parent_schema(identity, parent).await?),
            None => None,
        };
        let schema = request
            .schema
            .as_ref()
            .or(parent.as_ref())
            .ok_or(CompositionError::Conflict)?;
        super::state::validate_schema(schema)?;
        if let Some(parent) = &parent {
            super::edit::validate_schema_extension(parent, schema)?;
        }
        Ok(schema.clone())
    }
}

struct ReferencedUpload<'a> {
    location: &'a [u8],
    length: u64,
    md5: [u8; 16],
}

fn referenced_uploads<'a>(
    identity: &DatasetIdentity,
    manifest: &'a ManifestRecord,
) -> Result<BTreeMap<OperationId, ReferencedUpload<'a>>, CompositionError> {
    let mut references: BTreeMap<_, ReferencedUpload<'_>> = BTreeMap::new();
    for sample in &manifest.samples {
        for field in &sample.fields {
            if let FieldLocator::Chunk {
                location,
                length,
                md5,
            } = &field.value
            {
                // Opaque legacy locators retain the parent's existing storage contract.
                let Ok(id) = DatasetUploads::upload_id(identity, location) else {
                    continue;
                };
                if let Some(existing) = references.get(&id) {
                    if existing.length != *length || existing.md5 != *md5 {
                        return Err(UploadError::Integrity.into());
                    }
                } else {
                    references.insert(
                        id,
                        ReferencedUpload {
                            location,
                            length: *length,
                            md5: *md5,
                        },
                    );
                }
            }
        }
    }
    Ok(references)
}

async fn validate_uploads(
    uploads: Option<&DatasetUploads>,
    references: &BTreeMap<OperationId, ReferencedUpload<'_>>,
    cancel: &ReadCancellation,
) -> Result<(), CompositionError> {
    for reference in references.values() {
        cancelled(cancel)?;
        let record = uploads
            .ok_or(UploadError::Invalid)?
            .metadata(reference.location)
            .await?;
        if record.length != reference.length || record.md5 != reference.md5 {
            return Err(UploadError::Integrity.into());
        }
    }
    Ok(())
}

async fn claim_uploads(
    uploads: Option<&DatasetUploads>,
    references: &BTreeMap<OperationId, ReferencedUpload<'_>>,
    owner: OperationId,
    cancel: &ReadCancellation,
) -> Result<(), CompositionError> {
    for reference in references.values() {
        cancelled(cancel)?;
        uploads
            .ok_or(UploadError::Invalid)?
            .claim(reference.location, owner, super::state::now())
            .await?;
    }
    Ok(())
}

fn cancelled(cancel: &ReadCancellation) -> Result<(), CompositionError> {
    if cancel.is_cancelled() {
        return Err(crate::inspection::InspectionError::Cancelled.into());
    }
    Ok(())
}
