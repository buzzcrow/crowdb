// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    head::CommittedSplitHead, key, MembershipPage, SplitHead, SplitLifecycleError, SplitName,
    SplitRevisionRecord,
};
use crate::{
    inspection::{cursor::CursorScope, work::InspectionWork, InspectionPage},
    AuthorityError, CommitTarget, DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId,
    ReadCancellation, SnapshotId, SplitRevisionId, SuccessfulCommit,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitRevisionSummary {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub revision: SplitRevisionId,
    pub operation: OperationId,
    pub split_count: u32,
}
impl DatasetAuthority {
    /// Lists only committed revision summaries; prepared revision indexes remain invisible.
    /// # Errors
    /// Rejects unavailable owners, invalid cursors, corrupt records and bounded-work limits.
    pub async fn split_revision_history(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<SplitRevisionSummary>, SplitLifecycleError> {
        validate_budget(budget)?;
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        self.require_split_reader(identity, snapshot, &mut work).await?;
        let mut prefix = key::revision_summary(identity, snapshot, SplitRevisionId::random())
            .map_err(AuthorityError::from)?;
        prefix.truncate(prefix.len() - 16);
        let cursor = CursorScope::new(&prefix, b"split-history");
        let page = work
            .scan(prefix.clone(), cursor.decode(after)?, budget.items)
            .await?;
        let mut entries = Vec::new();
        for (key, value) in page.entries {
            let summary: SplitRevisionSummary = super::codec::decode(&value.bytes)?;
            if summary.version != 1
                || summary.snapshot != snapshot
                || key.strip_prefix(prefix.as_slice()) != Some(summary.revision.as_bytes())
            {
                return Err(SplitLifecycleError::Corrupt);
            }
            if self
                .split_revision_visible(identity, snapshot, summary.revision, &mut work)
                .await?
            {
                entries.push(summary);
            }
        }
        Ok(InspectionPage {
            entries,
            after: cursor.encode(page.after)?,
        })
    }
    /// Loads immutable definitions/counts/provenance and page bindings for a committed revision.
    /// # Errors
    /// Rejects uncommitted revisions, unavailable owners, corruption and work limits.
    pub async fn get_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<SplitRevisionRecord, SplitLifecycleError> {
        validate_budget(budget)?;
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        self.read_split_revision(identity, snapshot, revision, &mut work)
            .await
    }
    pub(super) async fn read_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        work: &mut InspectionWork<'_>,
    ) -> Result<SplitRevisionRecord, SplitLifecycleError> {
        self.require_split_reader(identity, snapshot, work).await?;
        if !self
            .split_revision_visible(identity, snapshot, revision, work)
            .await?
        {
            return Err(SplitLifecycleError::NotFound);
        }
        let value = work
            .get(
                &key::revision(identity, snapshot, revision).map_err(AuthorityError::from)?,
                1024 * 1024,
            )
            .await?
            .ok_or(SplitLifecycleError::Corrupt)?;
        let record = SplitRevisionRecord::decode(&value.bytes)?;
        if record.snapshot != snapshot || record.revision != revision {
            return Err(SplitLifecycleError::Corrupt);
        }
        Ok(record)
    }
    async fn require_split_reader(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        work: &mut InspectionWork<'_>,
    ) -> Result<(), SplitLifecycleError> {
        if !self
            .inspection_snapshot_committed(identity, snapshot, work)
            .await?
        {
            return Err(SplitLifecycleError::NotFound);
        }
        Ok(())
    }
    pub(super) async fn split_revision_visible(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, SplitLifecycleError> {
        let target = CommitTarget::SplitRevision { snapshot, revision };
        if let Some(value) = work
            .get(
                &key::committed_revision(identity, snapshot, revision).map_err(AuthorityError::from)?,
                8192,
            )
            .await?
        {
            if SuccessfulCommit::decode(&value.bytes)?.attempt.target != target {
                return Err(SplitLifecycleError::Corrupt);
            }
            return Ok(true);
        }
        let Some(value) = work
            .get(
                &key::head(identity, snapshot).map_err(AuthorityError::from)?,
                8192,
            )
            .await?
        else {
            return Ok(false);
        };
        if SplitHead::decode(&value.bytes).is_ok() {
            return Ok(false);
        }
        let head = CommittedSplitHead::decode(&value.bytes)?;
        Ok(head.attempt.target == target)
    }

    /// Returns exact ordered member IDs from checksum-bound immutable pages.
    /// # Errors
    /// Rejects wrong revision/name/cursors, missing pages, corrupt bindings and work limits.
    #[allow(clippy::too_many_arguments)]
    pub async fn split_members_page(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        name: &SplitName,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<Vec<u8>>, SplitLifecycleError> {
        validate_budget(budget)?;
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let record = self
            .read_split_revision(identity, snapshot, revision, &mut work)
            .await?;
        let split = record
            .splits
            .iter()
            .find(|s| &s.name == name)
            .ok_or(SplitLifecycleError::NotFound)?;
        let mut prefix =
            key::membership_page(identity, snapshot, revision, name, 0).map_err(AuthorityError::from)?;
        prefix.truncate(prefix.len() - 4);
        let cursor = CursorScope::new(&prefix, b"members");
        let position = cursor.decode(after)?;
        let (mut page_index, mut offset) = member_position(&prefix, position.as_deref())?;
        if position.is_some() && page_index >= split.pages.len() {
            return Err(cursor_mismatch());
        }
        let mut entries = Vec::new();
        while page_index < split.pages.len() && entries.len() < budget.items {
            let binding = &split.pages[page_index];
            let stored = work
                .get(
                    &key::membership_page(identity, snapshot, revision, name, binding.page)
                        .map_err(AuthorityError::from)?,
                    1024 * 1024,
                )
                .await?
                .ok_or(SplitLifecycleError::Corrupt)?;
            let page = MembershipPage::decode(&stored.bytes)?;
            page.verify_binding(snapshot, revision, name, binding)?;
            if offset >= page.members.len() {
                return Err(cursor_mismatch());
            }
            let end = page.members.len().min(offset + budget.items - entries.len());
            entries.extend_from_slice(&page.members[offset..end]);
            offset = end;
            if offset == page.members.len() {
                page_index += 1;
                offset = 0;
            }
        }
        let next = if page_index < split.pages.len() {
            let mut key = prefix;
            key.extend_from_slice(
                &u32::try_from(page_index)
                    .map_err(|_| SplitLifecycleError::Limit)?
                    .to_be_bytes(),
            );
            key.extend_from_slice(
                &u32::try_from(offset)
                    .map_err(|_| SplitLifecycleError::Limit)?
                    .to_be_bytes(),
            );
            Some(key)
        } else {
            None
        };
        Ok(InspectionPage {
            entries,
            after: cursor.encode(next)?,
        })
    }
}
fn member_position(prefix: &[u8], key: Option<&[u8]>) -> Result<(usize, usize), SplitLifecycleError> {
    let Some(key) = key else {
        return Ok((0, 0));
    };
    let suffix = key.strip_prefix(prefix).ok_or_else(cursor_mismatch)?;
    let position: [u8; 8] = suffix.try_into().map_err(|_| cursor_mismatch())?;
    let page = u32::from_be_bytes(position[..4].try_into().map_err(|_| cursor_mismatch())?);
    let offset = u32::from_be_bytes(position[4..].try_into().map_err(|_| cursor_mismatch())?);
    Ok((page as usize, offset as usize))
}
fn cursor_mismatch() -> SplitLifecycleError {
    AuthorityError::Invalid(crate::DatasetError::CursorMismatch).into()
}
fn validate_budget(budget: MetadataScanBudget) -> Result<(), SplitLifecycleError> {
    budget.validate()?;
    if budget.items > 100 || budget.bytes > 1024 * 1024 {
        return Err(SplitLifecycleError::Limit);
    }
    Ok(())
}
