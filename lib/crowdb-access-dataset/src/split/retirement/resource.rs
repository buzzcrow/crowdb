// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{SplitError, SplitLifecycleError, SplitRevisionRecord, MAX_SPLIT_RECORD_BYTES};
use crate::{SnapshotId, SplitRevisionId};
use serde::{Deserialize, Serialize};

const MAGIC: &[u8] = b"split-deletion-v1\0";
pub(in crate::split) const RESOURCE_CAP: usize = 1024 * 1024 + 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::split) enum DeletionPhase {
    Closing,
    Retiring,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::split) struct RetiringRevision {
    pub phase: DeletionPhase,
    pub reclaim_after: u64,
    pub record: SplitRevisionRecord,
}

impl RetiringRevision {
    pub fn encode(&self) -> Result<Vec<u8>, SplitLifecycleError> {
        let size = validate_record(&self.record)?;
        let mut bytes = Vec::with_capacity(size + 1024);
        bytes.extend_from_slice(MAGIC);
        serde_json::to_writer(&mut bytes, self).map_err(|_| SplitLifecycleError::Corrupt)?;
        if bytes.len() > RESOURCE_CAP {
            return Err(SplitLifecycleError::Limit);
        }
        Ok(bytes)
    }
    pub fn result(&self) -> super::SplitRevisionDeletion {
        super::SplitRevisionDeletion {
            snapshot: self.record.snapshot,
            revision: self.record.revision,
            reclaim_after: Some(self.reclaim_after),
            complete: false,
        }
    }
}

pub(in crate::split) enum RevisionResource {
    Complete(SplitRevisionRecord),
    Deleting(RetiringRevision),
    Sealed,
}

impl RevisionResource {
    pub fn decode(
        bytes: &[u8],
        snapshot: SnapshotId,
        revision: SplitRevisionId,
    ) -> Result<Self, SplitLifecycleError> {
        if bytes == super::super::RETIRED_RECORD {
            return Ok(Self::Sealed);
        }
        if bytes.len() > RESOURCE_CAP {
            return Err(SplitLifecycleError::Limit);
        }
        let resource = if let Some(body) = bytes.strip_prefix(MAGIC) {
            let value: RetiringRevision =
                serde_json::from_slice(body).map_err(|_| SplitLifecycleError::Corrupt)?;
            validate_record(&value.record)?;
            Self::Deleting(value)
        } else {
            Self::Complete(SplitRevisionRecord::decode(bytes)?)
        };
        let record = match &resource {
            Self::Complete(record) => record,
            Self::Deleting(value) => &value.record,
            Self::Sealed => return Ok(resource),
        };
        if record.snapshot != snapshot || record.revision != revision {
            return Err(SplitLifecycleError::Corrupt);
        }
        Ok(resource)
    }
}

fn validate_record(record: &SplitRevisionRecord) -> Result<usize, SplitError> {
    record.validate()?;
    let mut size = RecordSize {
        bytes: 0,
        limit: usize::try_from(MAX_SPLIT_RECORD_BYTES).map_err(|_| SplitError::Limit)?,
    };
    serde_json::to_writer(&mut size, record).map_err(|_| SplitError::Limit)?;
    Ok(size.bytes)
}

struct RecordSize {
    bytes: usize,
    limit: usize,
}
impl std::io::Write for RecordSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes {
            return Err(std::io::Error::other("Split metadata limit"));
        }
        self.bytes += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(in crate::split) fn is_closed(bytes: &[u8]) -> bool {
    bytes == super::super::RETIRED_RECORD || bytes.starts_with(MAGIC)
}
