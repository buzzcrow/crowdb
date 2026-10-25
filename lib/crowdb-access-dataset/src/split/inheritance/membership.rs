// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    inspection::work::InspectionWork,
    split::{
        key, MembershipPage, MembershipPageBinding, SplitLifecycleError, SplitName, SplitRevisionRecord,
    },
    AuthorityError, DatasetAuthority, DatasetIdentity,
};

impl DatasetAuthority {
    pub(crate) async fn frozen_split_window_contains(
        &self,
        identity: &DatasetIdentity,
        record: &SplitRevisionRecord,
        name: &SplitName,
        samples: &[Vec<u8>],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<bool>, SplitLifecycleError> {
        if samples.len() > 20 {
            return Err(SplitLifecycleError::Limit);
        }
        let mut members = vec![false; samples.len()];
        if samples.is_empty() {
            return Ok(members);
        }
        let split = record
            .splits
            .iter()
            .find(|split| &split.name == name)
            .ok_or(SplitLifecycleError::NotFound)?;
        let mut bounds = vec![0..split.pages.len(); samples.len()];
        while let Some(pending) = bounds.iter().find(|range| !range.is_empty()) {
            let middle = pending.start + pending.len() / 2;
            let page = self
                .read_frozen_split_page(identity, record, name, &split.pages[middle], work)
                .await?;
            let first = page.members.first().ok_or(SplitLifecycleError::Corrupt)?;
            let last = page.members.last().ok_or(SplitLifecycleError::Corrupt)?;
            // Unresolved ranges are equal or disjoint binary-search subtrees.
            // Every candidate consulting this midpoint advances before the page
            // is dropped, so no candidate can request it again in this window.
            for ((sample, range), member) in samples.iter().zip(&mut bounds).zip(&mut members) {
                if !range.contains(&middle) {
                    continue;
                }
                if sample < first {
                    range.end = middle;
                } else if sample > last {
                    range.start = middle + 1;
                } else {
                    *member = page.members.binary_search(sample).is_ok();
                    range.start = range.end;
                }
            }
        }
        Ok(members)
    }

    pub(super) async fn read_frozen_split_page(
        &self,
        identity: &DatasetIdentity,
        record: &SplitRevisionRecord,
        name: &SplitName,
        binding: &MembershipPageBinding,
        work: &mut InspectionWork<'_>,
    ) -> Result<MembershipPage, SplitLifecycleError> {
        let stored = work
            .get(
                &key::membership_page(identity, record.snapshot, record.revision, name, binding.page)
                    .map_err(AuthorityError::from)?,
                1024 * 1024,
            )
            .await?
            .ok_or(SplitLifecycleError::Corrupt)?;
        let page = MembershipPage::decode(&stored.bytes)?;
        page.verify_binding(record.snapshot, record.revision, name, binding)?;
        Ok(page)
    }
}
