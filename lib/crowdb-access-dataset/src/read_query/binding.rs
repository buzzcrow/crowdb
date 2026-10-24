// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    validate_delivery, AuthorityError, Continuation, DatasetError, DatasetIdentity, DatasetReads,
    ReadBinding, ReadSpec, SnapshotId,
};
use crate::{inspection::work::InspectionWork, split::SplitSource, MetadataScanBudget, ReadCancellation};
use std::time::Duration;

pub(super) fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: Duration::from_secs(2),
    }
}
impl DatasetReads {
    /// Resolves aliases into a portable continuation without registering a reader.
    /// # Errors
    /// Rejects invalid queries, unavailable snapshots/revisions and metadata limits.
    pub async fn bind(
        &self,
        identity: &DatasetIdentity,
        mut spec: ReadSpec,
    ) -> Result<ReadBinding, AuthorityError> {
        validate_delivery(&spec)?;
        let snapshot = self.resolve_snapshot(identity, &spec.snapshot).await?;
        let split = if let Some(request) = &mut spec.split {
            let revision = match request.revision {
                Some(revision) => {
                    self.authority
                        .require_committed_snapshot(identity, snapshot)
                        .await?;
                    revision
                }
                None => self
                    .authority
                    .inspect_split_head(identity, snapshot)
                    .await
                    .map_err(split_error)?
                    .revision
                    .ok_or(AuthorityError::NotFound)?,
            };
            // Both branches admitted this exact Snapshot in this request. Reuse that
            // observation only while resolving its immutable Split resource.
            let cancel = ReadCancellation::new();
            let mut work = InspectionWork::new(self.authority.store.as_ref(), &cancel, budget())
                .map_err(inspection_error)?;
            let record = self
                .authority
                .read_admitted_split_revision(identity, snapshot, revision, &mut work)
                .await
                .map_err(split_error)?;
            if !record.splits.iter().any(|s| s.name == request.name) {
                return Err(AuthorityError::NotFound);
            }
            request.revision = Some(revision);
            Some(SplitSource {
                snapshot,
                revision,
                name: request.name.clone(),
            })
        } else {
            self.authority
                .require_committed_snapshot(identity, snapshot)
                .await?;
            None
        };
        spec.snapshot = snapshot.to_string();
        let cursor = Continuation {
            version: 1,
            identity: identity.key()?,
            spec,
            snapshot,
            split: split.clone(),
            after: None,
            ordinal: 0,
            offset: 0,
        }
        .encode()?;
        Ok(ReadBinding {
            snapshot,
            split,
            cursor,
        })
    }
    pub(super) async fn resolve_snapshot(
        &self,
        identity: &DatasetIdentity,
        value: &str,
    ) -> Result<SnapshotId, AuthorityError> {
        let snapshot = match value {
            "latest" => self
                .authority
                .latest(identity)
                .await?
                .ok_or(AuthorityError::SnapshotNotFound)?,
            "stable" => self
                .authority
                .get_dataset(identity)
                .await?
                .stable
                .ok_or(AuthorityError::SnapshotNotFound)?,
            value => {
                let mut id = [0; 16];
                if value.len() != 32
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(DatasetError::InvalidManifest.into());
                }
                for (i, byte) in id.iter_mut().enumerate() {
                    *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)
                        .map_err(|_| DatasetError::InvalidManifest)?;
                }
                SnapshotId::from_bytes(&id)?
            }
        };
        Ok(snapshot)
    }
}
pub(super) fn inspection_error(error: crate::inspection::InspectionError) -> AuthorityError {
    match error {
        crate::inspection::InspectionError::Authority(error) => error,
        crate::inspection::InspectionError::Store(error) => error.into(),
        crate::inspection::InspectionError::Cancelled => DatasetError::ReadCancelled.into(),
        crate::inspection::InspectionError::Limit
        | crate::inspection::InspectionError::NotPrepared
        | crate::inspection::InspectionError::Unsupported(_) => DatasetError::ReadLimitExceeded.into(),
        crate::inspection::InspectionError::Corrupt => AuthorityError::Corrupt,
    }
}
pub(super) fn split_error(error: crate::split::SplitLifecycleError) -> AuthorityError {
    match error {
        crate::split::SplitLifecycleError::Authority(error) => error,
        crate::split::SplitLifecycleError::Store(error) => error.into(),
        crate::split::SplitLifecycleError::NotFound => AuthorityError::NotFound,
        crate::split::SplitLifecycleError::Inspection(error) => inspection_error(error),
        crate::split::SplitLifecycleError::Limit => DatasetError::ReadLimitExceeded.into(),
        _ => AuthorityError::Corrupt,
    }
}
