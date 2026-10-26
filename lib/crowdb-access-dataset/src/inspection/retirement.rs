// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    index::summary,
    key,
    rows::{next_row, IndexPosition},
    work::InspectionWork,
    InspectionError, InspectionState,
};
use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, ManifestRecord, MetadataScanBudget,
    ReadCancellation, SnapshotId, SnapshotRecord,
};
use std::time::Duration;

pub(crate) const RETIRED_RECORD: &[u8] = b"snapshot-metadata-retired-v1";
#[derive(Clone, Debug, Default)]
pub(crate) struct CandidateRetirementCursor {
    partition: u32,
    rows: IndexPosition,
    complete: bool,
    target: Option<(DatasetIdentity, SnapshotId)>,
}
impl DatasetAuthority {
    /// Seals all possible immutable writes, including keys not reached by a failed preparation.
    pub(crate) async fn retire_candidate_metadata_step(
        &self,
        identity: &DatasetIdentity,
        record: &SnapshotRecord,
        cursor: &mut CandidateRetirementCursor,
        cancel: &ReadCancellation,
    ) -> Result<bool, InspectionError> {
        if record.publication.state != crate::PublicationState::Aborted
            || record.publication.manifest.len() > 8 * 1024 * 1024
        {
            return Err(InspectionError::Corrupt);
        }
        let mut manifest: ManifestRecord =
            bincode::deserialize(&record.publication.manifest).map_err(|_| InspectionError::Corrupt)?;
        manifest.snapshot = record.publication.snapshot;
        manifest.validate().map_err(AuthorityError::from)?;
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 20,
                bytes: 9 * 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )?;
        let target = (identity.clone(), manifest.snapshot);
        if cursor.target.as_ref().is_some_and(|bound| bound != &target) {
            return Err(InspectionError::Corrupt);
        }
        cursor.target = Some(target);
        cursor.rows.validate(&manifest)?;
        let partitions =
            u32::try_from(manifest.samples.len().div_ceil(1024)).map_err(|_| InspectionError::Limit)?;
        if cursor.partition > partitions {
            return Err(InspectionError::Corrupt);
        }
        if cursor.complete {
            return Ok(true);
        }
        let mut left = 20;
        while left > 0 && cursor.partition < partitions {
            self.seal_candidate_key(
                &crate::key::manifest_partition(identity, manifest.snapshot, cursor.partition)
                    .map_err(AuthorityError::from)?,
                RETIRED_RECORD,
                &mut work,
            )
            .await?;
            cursor.partition += 1;
            left -= 1;
        }
        while left > 0 {
            let mut next = cursor.rows.clone();
            let Some(row) = next_row(identity, &manifest, &mut next)? else {
                self.finish_candidate_retirement(identity, record, &manifest, &mut work)
                    .await?;
                cursor.rows = next;
                cursor.complete = true;
                break;
            };
            self.seal_candidate_key(&row.key, RETIRED_RECORD, &mut work)
                .await?;
            cursor.rows = next;
            left -= 1;
        }
        Ok(cursor.complete)
    }
    async fn finish_candidate_retirement(
        &self,
        identity: &DatasetIdentity,
        record: &SnapshotRecord,
        manifest: &ManifestRecord,
        work: &mut InspectionWork<'_>,
    ) -> Result<(), InspectionError> {
        self.seal_candidate_key(
            &crate::key::manifest_binding(identity, manifest.snapshot).map_err(AuthorityError::from)?,
            RETIRED_RECORD,
            work,
        )
        .await?;
        let mut retired = summary(record.publication.parent, manifest)?;
        retired.state = InspectionState::Unsupported {
            reason: "snapshot is retired".into(),
        };
        let bytes = serde_json::to_vec(&retired).map_err(|_| InspectionError::Corrupt)?;
        for key in [
            key::inventory(identity, manifest.snapshot),
            key::child(identity, record.publication.parent, manifest.snapshot),
            key::scope(identity, manifest.snapshot, b'H'),
        ] {
            self.seal_candidate_key(&key.map_err(AuthorityError::from)?, &bytes, work)
                .await?;
        }
        Ok(())
    }
    async fn seal_candidate_key(
        &self,
        key: &[u8],
        bytes: &[u8],
        work: &mut InspectionWork<'_>,
    ) -> Result<(), InspectionError> {
        let stored = work.get(key, 9 * 1024 * 1024).await?;
        if stored.as_ref().is_some_and(|v| v.bytes == bytes) {
            return Ok(());
        }
        match self
            .store
            .compare_exchange(key, stored.as_ref().map(|v| v.bytes.as_slice()), bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::HeadConflict.into()),
        }
    }
}
