// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, work::InspectionWork, InspectionError, SnapshotSummary};
use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetError, DatasetIdentity, MetadataScanBudget,
    ReadCancellation, SnapshotId,
};
use std::time::Duration;

impl DatasetAuthority {
    /// A temporary reclaim admission fence does not prove a child is retired.
    pub(crate) async fn has_live_snapshot_children(
        &self,
        identity: &DatasetIdentity,
        parent: SnapshotId,
    ) -> Result<bool, AuthorityError> {
        self.inspect_live_children(identity, parent)
            .await
            .map_err(|error| match error {
                InspectionError::Authority(error) => error,
                InspectionError::Store(error) => error.into(),
                InspectionError::Limit => DatasetError::ReadLimitExceeded.into(),
                InspectionError::Cancelled => DatasetError::ReadCancelled.into(),
                _ => AuthorityError::Corrupt,
            })
    }
    async fn inspect_live_children(
        &self,
        identity: &DatasetIdentity,
        parent: SnapshotId,
    ) -> Result<bool, InspectionError> {
        let cancel = ReadCancellation::new();
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            &cancel,
            MetadataScanBudget {
                items: 20,
                bytes: 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )?;
        let mut prefix = key::child(identity, Some(parent), parent).map_err(AuthorityError::from)?;
        prefix.truncate(prefix.len() - 16);
        let mut after = None;
        loop {
            let page = work.scan(prefix.clone(), after, 20).await?;
            for (key, stored) in page.entries {
                let summary: SnapshotSummary =
                    serde_json::from_slice(&stored.bytes).map_err(|_| InspectionError::Corrupt)?;
                if summary.version != 1
                    || summary.parent != Some(parent)
                    || key.strip_prefix(prefix.as_slice()) != Some(summary.snapshot.as_bytes())
                {
                    return Err(InspectionError::Corrupt);
                }
                let retired = work
                    .get(
                        &retired_key(identity, summary.snapshot).map_err(AuthorityError::from)?,
                        1024,
                    )
                    .await?;
                if let Some(retired) = retired {
                    if retired.bytes != [1] {
                        return Err(InspectionError::Corrupt);
                    }
                    // Terminal authority survives a late recreation of this derived index.
                    work.delete_retired_index(&key).await?;
                    continue;
                }
                // The child index precedes its complete root. Protecting it until
                // terminal retirement closes the parent-delete/finalization race.
                return Ok(true);
            }
            after = page.after;
            if after.is_none() {
                return Ok(false);
            }
        }
    }
    pub(crate) async fn inspection_snapshot_retired(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, InspectionError> {
        match work
            .get(
                &retired_key(identity, snapshot).map_err(AuthorityError::from)?,
                1024,
            )
            .await?
        {
            None => Ok(false),
            Some(value) if value.bytes == [1] => Ok(true),
            Some(_) => Err(InspectionError::Corrupt),
        }
    }
    pub(crate) async fn mark_snapshot_retired(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        let key = retired_key(identity, snapshot)?;
        if let Some(value) = self.store.get(&key).await? {
            return if value.bytes == [1] {
                Ok(())
            } else {
                Err(AuthorityError::Corrupt)
            };
        }
        match self.store.compare_exchange(&key, None, &[1]).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == [1] => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::Corrupt),
        }
    }
}
fn retired_key(identity: &DatasetIdentity, snapshot: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    let mut key = b"DS\0RT\0".to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    Ok(key)
}
