// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key, DraftSplit, MembershipPage, MembershipPageBinding, NamedSplitRecord, SplitLifecycleError,
    SplitPolicy, SplitRevisionRecord, SplitRevisionSummary, SplitValidation, MAX_SPLIT_RECORD_BYTES,
};
use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId, ReadCancellation,
    SnapshotId, SplitRevisionId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Complete caller-owned input for one immutable Split result, without editing or progress state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitPreparation {
    pub revision: SplitRevisionId,
    pub policy: SplitPolicy,
    pub splits: Vec<DraftSplit>,
}

impl DatasetAuthority {
    /// Computes final membership pages and installs their complete root without a draft.
    /// A retry recomputes incomplete work or reuses an identical complete resource.
    /// # Errors
    /// Rejects unavailable sources, conflicting identities, policy violations, cancellation and limits.
    pub async fn prepare_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        input: &SplitPreparation,
        cancel: &ReadCancellation,
    ) -> Result<SplitRevisionRecord, SplitLifecycleError> {
        validate(snapshot, input)?;
        tokio::time::timeout(
            Duration::from_secs(30),
            self.prepare_split_result(identity, snapshot, input, cancel, false),
        )
        .await
        .map_err(|_| SplitLifecycleError::Limit)?
    }

    pub(crate) async fn prepare_initial_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        input: &SplitPreparation,
        cancel: &ReadCancellation,
    ) -> Result<SplitRevisionRecord, SplitLifecycleError> {
        validate(snapshot, input)?;
        self.prepare_split_result(identity, snapshot, input, cancel, true)
            .await
    }

    async fn prepare_split_result(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        input: &SplitPreparation,
        cancel: &ReadCancellation,
        initial: bool,
    ) -> Result<SplitRevisionRecord, SplitLifecycleError> {
        cancelled(cancel)?;
        let parent = self
            .check_preparation_owner(identity, snapshot, input.revision, initial)
            .await?;
        let root = key::revision(identity, snapshot, input.revision).map_err(AuthorityError::from)?;
        if let Some(stored) = self.split_state_get(&root).await? {
            if stored.bytes == super::RETIRED_RECORD {
                return Err(SplitLifecycleError::Frozen);
            }
            let record = SplitRevisionRecord::decode(&stored.bytes)?;
            return if matches_input(&record, snapshot, input) {
                Ok(record)
            } else {
                Err(SplitLifecycleError::Conflict)
            };
        }
        if initial {
            self.validate_split_parent_sources(identity, parent, &input.splits)
                .await?;
        } else {
            self.validate_split_definitions(identity, snapshot, &input.splits)
                .await?;
        }
        let mut record = result(snapshot, input)?;
        let encoded_size = record.encode()?.len();
        let summary = SplitRevisionSummary {
            version: 1,
            snapshot,
            revision: input.revision,
            operation: record.operation,
            split_count: u32::try_from(record.splits.len()).map_err(|_| SplitLifecycleError::Limit)?,
        };
        // Resource enumeration precedes its member pages, including failed preparations.
        self.put_split_state(
            &key::revision_summary(identity, snapshot, input.revision).map_err(AuthorityError::from)?,
            &super::codec::encode(&summary)?,
        )
        .await?;
        self.compute_split_members(identity, input, &mut record, encoded_size, cancel, initial)
            .await?;
        cancelled(cancel)?;
        self.check_preparation_owner(identity, snapshot, input.revision, initial)
            .await?;
        self.put_split_state(&root, &record.encode()?).await?;
        Ok(record)
    }

    async fn check_preparation_owner(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        initial: bool,
    ) -> Result<Option<SnapshotId>, SplitLifecycleError> {
        if !initial {
            self.require_committed_snapshot(identity, snapshot).await?;
            return Ok(None);
        }
        self.require_initial_split_owner(identity, snapshot, revision)
            .await
    }

    async fn compute_split_members(
        &self,
        identity: &DatasetIdentity,
        input: &SplitPreparation,
        record: &mut SplitRevisionRecord,
        mut encoded_size: usize,
        cancel: &ReadCancellation,
        initial: bool,
    ) -> Result<(), SplitLifecycleError> {
        let mut after = None;
        let mut validation = SplitValidation::default();
        loop {
            cancelled(cancel)?;
            let window = self
                .evaluate_split_window(
                    identity,
                    record.snapshot,
                    &input.splits,
                    after,
                    MetadataScanBudget {
                        items: 20,
                        bytes: 1024 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                    cancel,
                    initial,
                )
                .await?;
            validation.merge(&window.validation)?;
            if validation.scanned > window.universe_count
                || (window.after.is_none() && validation.scanned != window.universe_count)
            {
                return Err(SplitLifecycleError::Corrupt);
            }
            self.write_split_member_window(identity, record, window.members, &mut encoded_size, cancel)
                .await?;
            after = window.after;
            if after.is_none() {
                if !validation.satisfies(input.policy) {
                    return Err(SplitLifecycleError::PolicyViolation);
                }
                return Ok(());
            }
        }
    }

    async fn write_split_member_window(
        &self,
        identity: &DatasetIdentity,
        record: &mut SplitRevisionRecord,
        members: Vec<Vec<Vec<u8>>>,
        encoded_size: &mut usize,
        cancel: &ReadCancellation,
    ) -> Result<(), SplitLifecycleError> {
        for (split, members) in record.splits.iter_mut().zip(members) {
            if members.is_empty() {
                continue;
            }
            cancelled(cancel)?;
            let page = MembershipPage {
                version: 1,
                snapshot: record.snapshot,
                revision: record.revision,
                name: split.name.clone(),
                page: u32::try_from(split.pages.len()).map_err(|_| SplitLifecycleError::Limit)?,
                members,
            };
            let bytes = page.encode()?;
            let binding = MembershipPageBinding {
                page: page.page,
                count: u32::try_from(page.members.len()).map_err(|_| SplitLifecycleError::Limit)?,
                sha256: Sha256::digest(&bytes).into(),
            };
            let count = split
                .count
                .checked_add(u64::try_from(page.members.len()).map_err(|_| SplitLifecycleError::Limit)?)
                .ok_or(SplitLifecycleError::Limit)?;
            let added = super::codec::encode(&binding)?.len()
                + usize::from(!split.pages.is_empty())
                + decimal_digits(count)
                - decimal_digits(split.count);
            *encoded_size = encoded_size
                .checked_add(added)
                .ok_or(SplitLifecycleError::Limit)?;
            if u64::try_from(*encoded_size).map_err(|_| SplitLifecycleError::Limit)? > MAX_SPLIT_RECORD_BYTES
            {
                return Err(SplitLifecycleError::Limit);
            }
            split.count = count;
            split.pages.push(binding);
            self.put_split_state(
                &key::membership_page(identity, record.snapshot, record.revision, &page.name, page.page)
                    .map_err(AuthorityError::from)?,
                &bytes,
            )
            .await?;
        }
        Ok(())
    }
}

fn validate(snapshot: SnapshotId, input: &SplitPreparation) -> Result<(), SplitLifecycleError> {
    SnapshotId::from_bytes(snapshot.as_bytes()).map_err(AuthorityError::from)?;
    SplitRevisionId::from_bytes(input.revision.as_bytes()).map_err(AuthorityError::from)?;
    super::draft::validate_definitions(snapshot, &input.splits)?;
    super::codec::encode(input)?;
    Ok(())
}

fn result(
    snapshot: SnapshotId,
    input: &SplitPreparation,
) -> Result<SplitRevisionRecord, SplitLifecycleError> {
    Ok(SplitRevisionRecord {
        version: 1,
        snapshot,
        revision: input.revision,
        operation: OperationId::from_bytes(input.revision.as_bytes()).map_err(AuthorityError::from)?,
        evaluator_version: 1,
        policy: input.policy,
        splits: input
            .splits
            .iter()
            .map(|split| NamedSplitRecord {
                name: split.name.clone(),
                definition: split.definition.clone(),
                origin: split.origin.clone(),
                count: 0,
                pages: Vec::new(),
            })
            .collect(),
    })
}
const fn decimal_digits(mut value: u64) -> usize {
    let mut count = 1;
    while value >= 10 {
        value /= 10;
        count += 1;
    }
    count
}
fn matches_input(record: &SplitRevisionRecord, snapshot: SnapshotId, input: &SplitPreparation) -> bool {
    record.snapshot == snapshot
        && record.revision == input.revision
        && record.operation.as_bytes() == input.revision.as_bytes()
        && record.policy == input.policy
        && record.splits.len() == input.splits.len()
        && record
            .splits
            .iter()
            .zip(&input.splits)
            .all(|(stored, requested)| {
                stored.name == requested.name
                    && stored.definition == requested.definition
                    && stored.origin == requested.origin
            })
}
fn cancelled(cancel: &ReadCancellation) -> Result<(), SplitLifecycleError> {
    if cancel.is_cancelled() {
        return Err(AuthorityError::Invalid(crate::DatasetError::ReadCancelled).into());
    }
    Ok(())
}
