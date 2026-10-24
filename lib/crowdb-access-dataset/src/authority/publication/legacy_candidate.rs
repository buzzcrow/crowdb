// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::candidate::CandidateBinding;
use crate::{
    authority::VersionedRecord,
    inspection::{work::InspectionWork, InspectionError},
    key,
    split::SplitRevisionRecord,
    AuthorityError, DatasetAuthority, DatasetIdentity, PublicationState, SnapshotRecord,
};
use bincode::Options;
use sha2::{Digest, Sha256};

impl DatasetAuthority {
    pub(super) async fn legacy_candidate_ready(
        &self,
        identity: &DatasetIdentity,
        candidate: &CandidateBinding,
    ) -> Result<bool, AuthorityError> {
        if candidate.version != 1 {
            return Ok(false);
        }
        let cancel = crate::ReadCancellation::new();
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            &cancel,
            crate::MetadataScanBudget {
                items: 1,
                bytes: 16 * 1024 * 1024,
                timeout: std::time::Duration::from_secs(2),
            },
        )
        .map_err(super::legacy::legacy_read_error)?;
        self.legacy_candidate_available(identity, candidate, &mut work)
            .await
            .map_err(super::legacy::legacy_read_error)
    }
    /// Old publishers marked content Published only after its initial Split and owners were ready.
    /// New staged bindings use another version and cannot take this compatibility path.
    pub(crate) async fn legacy_candidate_available(
        &self,
        identity: &DatasetIdentity,
        candidate: &CandidateBinding,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, InspectionError> {
        if candidate.version != 1 {
            return Ok(false);
        }
        let Some(value) = work
            .get(
                &key::snapshot(identity, candidate.snapshot).map_err(AuthorityError::from)?,
                9 * 1024 * 1024,
            )
            .await?
        else {
            return Ok(false);
        };
        let stored: VersionedRecord<SnapshotRecord> = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(value.bytes.len() as u64)
            .reject_trailing_bytes()
            .deserialize(&value.bytes)
            .map_err(|_| InspectionError::Corrupt)?;
        let record = stored.payload.publication;
        let digest: [u8; 32] = Sha256::digest(&record.manifest).into();
        if stored.version != 1
            || record.snapshot != candidate.snapshot
            || record.parent != candidate.parent
            || record.operation != candidate.operation
            || digest != candidate.manifest_sha256
        {
            return Err(InspectionError::Corrupt);
        }
        if record.state != PublicationState::Published {
            return Ok(false);
        }
        if let Some(revision) = candidate.initial_split {
            let Some(value) = work
                .get(
                    &crate::split::key::revision(identity, candidate.snapshot, revision)
                        .map_err(AuthorityError::from)?,
                    1024 * 1024,
                )
                .await?
            else {
                return Ok(false);
            };
            let record = SplitRevisionRecord::decode(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
            if record.snapshot != candidate.snapshot || record.revision != revision {
                return Err(InspectionError::Corrupt);
            }
        }
        Ok(true)
    }
}
