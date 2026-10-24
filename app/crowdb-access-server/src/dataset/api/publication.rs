// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{conversion, error::ApiError, json, models, operation, read_json, snapshot, ApiResult};
use crate::dataset::{unix_seconds, DatasetHttpService};
use crowdb_access_dataset::{AuthorityError, DatasetIdentity, FieldLocator, ManifestRecord, UploadState};
use hyper::{body::Incoming, Request};
use std::collections::BTreeSet;

impl DatasetHttpService {
    pub(super) async fn publication_route(
        &self,
        identity: &DatasetIdentity,
        action: &str,
        request: Request<Incoming>,
    ) -> ApiResult {
        let publication: models::Publication = read_json(request).await?;
        let operation = operation(&publication.operation)?;
        let parent = publication.parent.as_deref().map(snapshot).transpose()?;
        let mut manifest = conversion::manifest(publication.manifest, identity)?;
        let locations = self.resolve_payloads(&mut manifest).await?;
        let bytes = bincode::serialize(&manifest).map_err(|_| ApiError::integrity())?;
        let authority = self.reads.authority();
        match authority.publication_status(identity, operation).await {
            Ok(record) if record.publication.parent != parent || record.publication.manifest != bytes => {
                return Err(ApiError::conflict())
            }
            Ok(_) => {}
            Err(AuthorityError::SnapshotNotFound) => {
                if authority.latest(identity).await? != parent {
                    return Err(ApiError::conflict());
                }
                if let Some(parent) = parent {
                    authority.get_snapshot(identity, parent).await?;
                }
            }
            Err(error) => return Err(error.into()),
        }
        // The candidate is durable before ownership is acquired, so failed or
        // disconnected requests always have a recoverable publication identity.
        authority
            .prepare_snapshot_with_operation(identity, parent, bytes.clone(), operation)
            .await?;
        if !locations.is_empty() {
            let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
            for location in &locations {
                uploads.claim(location, operation, unix_seconds()).await?;
            }
        }
        let result = if action == "prepare" {
            authority
                .prepare_snapshot_with_operation(identity, parent, bytes, operation)
                .await
        } else {
            authority
                .publish_snapshot_with_operation(identity, parent, bytes, operation)
                .await
        };
        let snapshot = result?;
        authority.complete_snapshot_candidate(identity, snapshot).await?;
        json(&conversion::snapshot(
            authority.publication_status(identity, operation).await?,
            authority.latest(identity).await?,
        ))
    }
    async fn resolve_payloads(&self, manifest: &mut ManifestRecord) -> Result<BTreeSet<Vec<u8>>, ApiError> {
        let mut locations = BTreeSet::new();
        for field in manifest.samples.iter_mut().flat_map(|s| &mut s.fields) {
            if let FieldLocator::Chunk {
                location,
                length,
                md5,
            } = &mut field.value
            {
                let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
                let record = uploads.metadata(location).await?;
                if !matches!(record.state, UploadState::Complete | UploadState::Claimed) {
                    return Err(ApiError::conflict());
                }
                *length = record.length;
                *md5 = record.md5;
                locations.insert(location.clone());
            }
        }
        manifest.validate().map_err(|_| ApiError::invalid())?;
        Ok(locations)
    }
}
