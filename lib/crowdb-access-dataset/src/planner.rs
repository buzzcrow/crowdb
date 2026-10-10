use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

use crate::{DatasetError, ManifestRecord, SampleRecord, SnapshotId};

pub const MAX_BATCH_SIZE: usize = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadFailure {
    Transient,
    Checksum,
    NotFound,
    Cancelled,
}

impl ReadFailure {
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Transient)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: usize,
}

impl RetryPolicy {
    /// # Errors
    /// Rejects a policy that would never attempt a read.
    pub fn validate(self) -> Result<(), DatasetError> {
        if self.max_attempts == 0 {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(())
    }

    #[must_use]
    pub const fn should_retry(self, failure: ReadFailure, attempt: usize) -> bool {
        failure.retryable() && attempt < self.max_attempts
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadLimits {
    pub max_samples: usize,
    pub max_metadata_bytes: usize,
    pub max_batches: usize,
    pub prefetch: usize,
    pub in_flight: usize,
}

impl ReadLimits {
    /// # Errors
    /// Rejects zero limits and a prefetch window larger than in-flight capacity.
    pub fn validate(self) -> Result<(), DatasetError> {
        if self.max_samples == 0
            || self.max_metadata_bytes == 0
            || self.max_batches == 0
            || self.prefetch == 0
            || self.in_flight == 0
            || self.prefetch > self.in_flight
        {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Selection {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
    Explicit(Vec<Vec<u8>>),
    Keyword(Vec<u8>),
    FieldMatch { field: String, value: Vec<u8> },
    And(Vec<Selection>),
    Or(Vec<Selection>),
    Not(Box<Selection>),
}

impl Selection {
    /// # Errors
    /// Rejects empty ranges, duplicate explicit IDs, and empty compositions.
    pub fn validate(&self) -> Result<(), DatasetError> {
        match self {
            Self::Prefix(prefix) | Self::Keyword(prefix) if prefix.is_empty() => {
                Err(DatasetError::InvalidManifest)
            }
            Self::Range { start, end } if start.is_empty() || start >= end => {
                Err(DatasetError::InvalidManifest)
            }
            Self::Explicit(ids) => {
                let mut seen = std::collections::BTreeSet::new();
                if ids.iter().any(|id| id.is_empty() || !seen.insert(id)) {
                    return Err(DatasetError::InvalidManifest);
                }
                Ok(())
            }
            Self::And(items) | Self::Or(items) if items.is_empty() => Err(DatasetError::InvalidManifest),
            Self::And(items) | Self::Or(items) => {
                for item in items {
                    item.validate()?;
                }
                Ok(())
            }
            Self::Not(item) => item.validate(),
            Self::FieldMatch { field, value } if field.is_empty() || value.is_empty() => {
                Err(DatasetError::InvalidManifest)
            }
            _ => Ok(()),
        }
    }

    #[must_use]
    pub fn matches(&self, sample: &SampleRecord) -> bool {
        match self {
            Self::Prefix(prefix) => sample.sample_id.starts_with(prefix),
            Self::Range { start, end } => sample.sample_id >= *start && sample.sample_id < *end,
            Self::Explicit(ids) => ids.iter().any(|id| id == &sample.sample_id),
            Self::Keyword(keyword) => sample
                .fields
                .iter()
                .any(|field| matches!(&field.value, crate::FieldLocator::Inline { value, .. } if value.windows(keyword.len()).any(|window| window == keyword))),
            Self::FieldMatch { field, value } => sample.fields.iter().any(|item| {
                item.name == *field
                    && matches!(&item.value, crate::FieldLocator::Inline { value: actual, .. } if actual == value)
            }),
            Self::And(items) => items.iter().all(|item| item.matches(sample)),
            Self::Or(items) => items.iter().any(|item| item.matches(sample)),
            Self::Not(item) => !item.matches(sample),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Ordering {
    SampleId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadPlan {
    pub snapshot: SnapshotId,
    pub selection: Selection,
    pub ordering: Ordering,
    pub projection: Vec<String>,
    pub batch_size: usize,
}

impl ReadPlan {
    /// # Errors
    /// Rejects invalid selection, duplicate projection fields, or unbounded batches.
    pub fn validate(&self) -> Result<(), DatasetError> {
        self.selection.validate()?;
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(DatasetError::InvalidManifest);
        }
        let mut fields = std::collections::BTreeSet::new();
        if self
            .projection
            .iter()
            .any(|field| field.is_empty() || !fields.insert(field))
        {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(())
    }

    /// Evaluates and deterministically orders one manifest without reading payloads.
    ///
    /// # Errors
    /// Propagates plan or manifest validation failures.
    pub fn scan_ids(&self, manifest: &ManifestRecord) -> Result<Vec<Vec<u8>>, DatasetError> {
        self.validate()?;
        manifest.validate()?;
        if manifest.snapshot != self.snapshot {
            return Err(DatasetError::InvalidManifest);
        }
        let mut ids: Vec<_> = manifest
            .samples
            .iter()
            .filter(|sample| self.selection.matches(sample))
            .map(|sample| sample.sample_id.clone())
            .collect();
        ids.sort();
        Ok(ids)
    }

    /// Splits a deterministic scan into bounded batches without reading field
    /// payloads.
    ///
    /// # Errors
    /// Propagates plan and manifest validation failures.
    pub fn scan_batches(&self, manifest: &ManifestRecord) -> Result<Vec<Vec<Vec<u8>>>, DatasetError> {
        let ids = self.scan_ids(manifest)?;
        Ok(ids.chunks(self.batch_size).map(<[Vec<u8>]>::to_vec).collect())
    }

    /// Returns a stable identity for retries, cursors, and surface adapters.
    /// The identity covers every plan field and is independent of execution
    /// limits, which are admission policy rather than logical query state.
    ///
    /// # Panics
    /// Panics only if the derived serializable plan cannot be encoded, which
    /// cannot occur for the in-memory plan fields.
    #[must_use]
    pub fn identity(&self) -> [u8; 16] {
        let bytes = bincode::serialize(self).expect("ReadPlan serialization is infallible");
        let mut digest = Md5::new();
        digest.update(bytes);
        digest.finalize().into()
    }

    /// Evaluates a plan and rejects it before payload reads when any bounded
    /// delivery limit would be exceeded.
    ///
    /// # Errors
    /// Propagates plan or manifest validation failures and rejects a plan that
    /// exceeds sample, metadata-byte, batch, prefetch, or in-flight limits.
    pub fn scan_batches_with_limits(
        &self,
        manifest: &ManifestRecord,
        limits: ReadLimits,
    ) -> Result<Vec<Vec<Vec<u8>>>, DatasetError> {
        limits.validate()?;
        let batches = self.scan_batches(manifest)?;
        if batches.len() > limits.max_batches {
            return Err(DatasetError::ReadLimitExceeded);
        }
        let sample_count = batches.iter().map(Vec::len).sum::<usize>();
        if sample_count > limits.max_samples {
            return Err(DatasetError::ReadLimitExceeded);
        }
        if self.batch_size > limits.in_flight.saturating_mul(MAX_BATCH_SIZE) {
            return Err(DatasetError::ReadLimitExceeded);
        }
        let metadata_bytes = manifest
            .samples
            .iter()
            .filter(|sample| batches.iter().flatten().any(|id| id == &sample.sample_id))
            .map(|sample| {
                sample
                    .fields
                    .iter()
                    .map(|field| match &field.value {
                        crate::FieldLocator::Inline { value, .. } => value.len(),
                        crate::FieldLocator::Chunk { location, .. } => location.len(),
                        crate::FieldLocator::Tombstone => 0,
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();
        if metadata_bytes > limits.max_metadata_bytes {
            return Err(DatasetError::ReadLimitExceeded);
        }
        Ok(batches)
    }
}
