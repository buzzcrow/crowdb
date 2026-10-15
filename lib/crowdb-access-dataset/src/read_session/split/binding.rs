// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    state::{budget, decode, encode, key, BoundSplitSpec},
    SessionSpecV2,
};
use crate::{
    split::SplitSource, AuthorityError, DatasetError, DatasetIdentity, DatasetSessions, ReadCancellation,
};

impl DatasetSessions {
    pub(super) async fn bind_split_read_spec(
        &self,
        identity: &DatasetIdentity,
        spec: &SessionSpecV2,
        now: u64,
        cancel: &ReadCancellation,
    ) -> Result<BoundSplitSpec, AuthorityError> {
        self.reserve_session_version(identity, spec.read.id, 2).await?;
        match self.bound_split_read_spec(identity, spec.read.id, cancel).await {
            Ok(bound) => {
                if bound.spec != *spec {
                    return Err(AuthorityError::CursorConflict);
                }
                return Ok(bound);
            }
            Err(AuthorityError::NotFound) => {}
            Err(error) => return Err(error),
        }
        let snapshot = self.resolve_snapshot(identity, &spec.read.snapshot).await?;
        self.authority
            .require_committed_snapshot(identity, snapshot)
            .await?;
        super::super::planning::validate_delivery(&spec.read)?;
        let split = if let Some(split) = &spec.split {
            let revision = match split.revision {
                Some(revision) => revision,
                None => self
                    .authority
                    .inspect_split_head(identity, snapshot)
                    .await
                    .map_err(split_error)?
                    .revision
                    .ok_or(AuthorityError::NotFound)?,
            };
            let record = self
                .authority
                .get_split_revision(identity, snapshot, revision, budget(), cancel)
                .await
                .map_err(split_error)?;
            if !record.splits.iter().any(|s| s.name == split.name) {
                return Err(AuthorityError::NotFound);
            }
            Some(SplitSource {
                snapshot,
                revision,
                name: split.name.clone(),
            })
        } else {
            None
        };
        self.validate_split_read_schema(identity, snapshot, spec, cancel)
            .await?;
        self.authority
            .touch_read_handle(identity, snapshot, spec.read.id, now, super::super::TTL)
            .await?;
        let bound = BoundSplitSpec {
            version: 2,
            spec: spec.clone(),
            snapshot,
            split,
        };
        let stored_key = key(identity, spec.read.id, b"\0SPEC2")?;
        match self
            .immutable_session_metadata(&stored_key, &encode(&bound)?)
            .await
        {
            Ok(()) => Ok(bound),
            Err(AuthorityError::CursorConflict) => {
                let winner = self.bound_split_read_spec(identity, spec.read.id, cancel).await?;
                if winner.snapshot != snapshot {
                    self.authority
                        .release_read_handle(identity, snapshot, spec.read.id)
                        .await?;
                }
                if winner.spec != *spec {
                    return Err(AuthorityError::CursorConflict);
                }
                Ok(winner)
            }
            Err(error) => Err(error),
        }
    }
    pub(super) async fn bound_split_read_spec(
        &self,
        identity: &DatasetIdentity,
        id: crate::OperationId,
        cancel: &ReadCancellation,
    ) -> Result<BoundSplitSpec, AuthorityError> {
        let stored = self
            .authority
            .store
            .get_bounded(&key(identity, id, b"\0SPEC2")?, budget(), cancel)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        let bound: BoundSplitSpec = decode(&stored.bytes)?;
        if bound.version != 2 || bound.spec.read.id != id {
            return Err(AuthorityError::Corrupt);
        }
        match (&bound.spec.split, &bound.split) {
            (None, None) => {}
            (Some(request), Some(source))
                if source.snapshot == bound.snapshot
                    && request.name == source.name
                    && request.revision.map_or(true, |r| r == source.revision) => {}
            _ => return Err(AuthorityError::Corrupt),
        }
        Ok(bound)
    }
    async fn validate_split_read_schema(
        &self,
        identity: &DatasetIdentity,
        snapshot: crate::SnapshotId,
        spec: &SessionSpecV2,
        cancel: &ReadCancellation,
    ) -> Result<(), AuthorityError> {
        let mut after = None;
        let mut fields = Vec::new();
        loop {
            let page = self
                .authority
                .inspection_schema_page(identity, snapshot, after, budget(), cancel)
                .await
                .map_err(inspection_error)?;
            fields.extend(page.entries);
            if fields.len() > 256 {
                return Err(DatasetError::ReadLimitExceeded.into());
            }
            after = page.after;
            if after.is_none() {
                break;
            }
        }
        if spec
            .read
            .fields
            .iter()
            .any(|name| !fields.iter().any(|f| f.name == *name))
        {
            return Err(DatasetError::InvalidManifest.into());
        }
        crate::selection::EffectiveSelection::new(&spec.read.selection, &fields)?;
        Ok(())
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
