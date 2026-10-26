// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::snapshot_resource::decode_snapshot,
    legacy::{anchor_key, decode_anchor},
};
use crate::{
    inspection::work::InspectionWork, AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity,
    MetadataScanBudget, PublicationState, ReadCancellation, SnapshotId,
};
use std::time::Duration;

impl DatasetAuthority {
    pub(in crate::authority) async fn legacy_live_descendant(
        &self,
        identity: &DatasetIdentity,
        target: SnapshotId,
    ) -> Result<bool, AuthorityError> {
        let cancel = ReadCancellation::new();
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            &cancel,
            MetadataScanBudget {
                items: 1,
                bytes: 16 * 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )
        .map_err(map_error)?;
        let Some(value) = work.get(&anchor_key(identity)?, 1024).await.map_err(map_error)? else {
            return Ok(false);
        };
        let anchor = decode_anchor(&value.bytes)?;
        let mut seen = std::collections::HashSet::new();
        for root in [anchor.head.latest, anchor.head.stable].into_iter().flatten() {
            let mut current = Some(root);
            let mut chain = std::collections::HashSet::new();
            while let Some(snapshot) = current {
                if !chain.insert(snapshot) {
                    return Err(AuthorityError::AncestryCycle);
                }
                if !seen.insert(snapshot) {
                    break;
                }
                if seen.len() > 64 {
                    return Err(DatasetError::ReadLimitExceeded.into());
                }
                let value = work
                    .get(&crate::key::snapshot(identity, snapshot)?, 16 * 1024 * 1024)
                    .await
                    .map_err(map_error)?
                    .ok_or(AuthorityError::Corrupt)?;
                let record = decode_snapshot(&value.bytes)?;
                if record.publication.state == PublicationState::Prepared {
                    return Err(AuthorityError::Corrupt);
                }
                let publication = record.publication;
                if publication.parent == Some(target) && publication.state != PublicationState::Aborted {
                    return Ok(true);
                }
                current = publication.parent;
            }
        }
        Ok(false)
    }
}
fn map_error(error: crate::inspection::InspectionError) -> AuthorityError {
    match error {
        crate::inspection::InspectionError::Authority(error) => error,
        crate::inspection::InspectionError::Store(error) => error.into(),
        crate::inspection::InspectionError::Limit => DatasetError::ReadLimitExceeded.into(),
        _ => AuthorityError::Corrupt,
    }
}
