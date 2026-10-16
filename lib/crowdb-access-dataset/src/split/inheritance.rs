// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key, state::DraftState, MembershipOrigin, MembershipPage, SplitDraftRecord, SplitLifecycleError,
    SplitName, SplitRevisionRecord,
};
use crate::{
    inspection::work::InspectionWork, DatasetAuthority, DatasetIdentity, MetadataScanBudget,
    ReadCancellation, SnapshotId,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub(super) fn source_owners(record: &SplitDraftRecord) -> BTreeSet<SnapshotId> {
    record
        .splits
        .iter()
        .filter_map(|split| match &split.origin {
            MembershipOrigin::Inherited(source) => Some(source.snapshot),
            MembershipOrigin::Evaluated => None,
        })
        .collect()
}
impl DatasetAuthority {
    pub(super) async fn validate_split_sources(
        &self,
        identity: &DatasetIdentity,
        record: &SplitDraftRecord,
    ) -> Result<(), SplitLifecycleError> {
        if source_owners(record).is_empty() {
            return Ok(());
        }
        let budget = MetadataScanBudget {
            items: 1,
            bytes: 1024 * 1024,
            timeout: Duration::from_secs(2),
        };
        let cancel = ReadCancellation::new();
        let owner = self
            .snapshot_inspection_summary(identity, record.snapshot, budget, &cancel)
            .await?;
        let mut work = InspectionWork::new(self.store.as_ref(), &cancel, budget)?;
        let mut revisions = BTreeMap::new();
        for split in &record.splits {
            if let MembershipOrigin::Inherited(source) = &split.origin {
                if owner.parent != Some(source.snapshot) {
                    return Err(SplitLifecycleError::Conflict);
                }
                let key = (source.snapshot, source.revision);
                if let std::collections::btree_map::Entry::Vacant(entry) = revisions.entry(key) {
                    entry.insert(
                        self.read_split_revision(identity, source.snapshot, source.revision, &mut work)
                            .await?,
                    );
                }
                let revision = &revisions[&key];
                if !revision.splits.iter().any(|s| s.name == source.name) {
                    return Err(SplitLifecycleError::NotFound);
                }
            }
        }
        Ok(())
    }
    pub(super) async fn release_split_draft_leases(
        &self,
        identity: &DatasetIdentity,
        state: &DraftState,
    ) -> Result<(), SplitLifecycleError> {
        let mut snapshots: BTreeSet<_> = state.source_leases.iter().copied().collect();
        snapshots.insert(state.record.snapshot);
        for snapshot in snapshots {
            self.release_read_handle(identity, snapshot, state.record.operation)
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn frozen_split_contains(
        &self,
        identity: &DatasetIdentity,
        record: &SplitRevisionRecord,
        name: &SplitName,
        sample: &[u8],
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, SplitLifecycleError> {
        let split = record
            .splits
            .iter()
            .find(|s| &s.name == name)
            .ok_or(SplitLifecycleError::NotFound)?;
        let mut low = 0;
        let mut high = split.pages.len();
        // Pages are ordered by exact Sample bytes; binary search never scans the full membership.
        while low < high {
            let index = low + (high - low) / 2;
            let binding = &split.pages[index];
            let stored = work
                .get(
                    &key::membership_page(identity, record.snapshot, record.revision, name, binding.page)
                        .map_err(crate::AuthorityError::from)?,
                    1024 * 1024,
                )
                .await?
                .ok_or(SplitLifecycleError::Corrupt)?;
            let page = MembershipPage::decode(&stored.bytes)?;
            page.verify_binding(record.snapshot, record.revision, name, binding)?;
            if sample
                < page
                    .members
                    .first()
                    .ok_or(SplitLifecycleError::Corrupt)?
                    .as_slice()
            {
                high = index;
            } else if sample
                > page
                    .members
                    .last()
                    .ok_or(SplitLifecycleError::Corrupt)?
                    .as_slice()
            {
                low = index + 1;
            } else {
                return Ok(page
                    .members
                    .binary_search_by(|id| id.as_slice().cmp(sample))
                    .is_ok());
            }
        }
        Ok(false)
    }
}
