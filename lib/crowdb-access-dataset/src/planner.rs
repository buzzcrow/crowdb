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
        let mut ids: Vec<_> = self
            .selected_samples(manifest)?
            .map(|sample| sample.sample_id.clone())
            .collect();
        ids.sort();
        Ok(ids)
    }

    pub(crate) fn selected_samples<'a>(
        &'a self,
        manifest: &'a ManifestRecord,
    ) -> Result<impl Iterator<Item = &'a SampleRecord> + 'a, DatasetError> {
        self.validate()?;
        manifest.validate()?;
        if manifest.snapshot != self.snapshot {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(manifest
            .samples
            .iter()
            .filter(move |sample| self.selection.matches(sample)))
    }

    /// Splits a deterministic scan into bounded batches without reading field
    /// payloads.
    ///
    /// # Errors
    /// Propagates plan and manifest validation failures.
    pub fn scan_batches(&self, manifest: &ManifestRecord) -> Result<Vec<Vec<Vec<u8>>>, DatasetError> {
        let ids = self.scan_ids(manifest)?;
        Ok(into_batches(ids, self.batch_size))
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
        let mut ids = Vec::new();
        let mut metadata_bytes = 0_usize;
        for sample in self.selected_samples(manifest)? {
            if ids.len() >= limits.max_samples {
                return Err(DatasetError::ReadLimitExceeded);
            }
            for field in &sample.fields {
                let size = match &field.value {
                    crate::FieldLocator::Inline { value, .. } => value.len(),
                    crate::FieldLocator::Chunk { location, .. } => location.len(),
                    crate::FieldLocator::Tombstone => 0,
                };
                metadata_bytes = metadata_bytes.saturating_add(size);
                if metadata_bytes > limits.max_metadata_bytes {
                    return Err(DatasetError::ReadLimitExceeded);
                }
            }
            ids.push(sample.sample_id.clone());
        }
        if ids.len().div_ceil(self.batch_size) > limits.max_batches {
            return Err(DatasetError::ReadLimitExceeded);
        }
        if self.batch_size > limits.in_flight.saturating_mul(MAX_BATCH_SIZE) {
            return Err(DatasetError::ReadLimitExceeded);
        }
        ids.sort();
        Ok(into_batches(ids, self.batch_size))
    }
}

pub(crate) fn into_batches(ids: Vec<Vec<u8>>, batch_size: usize) -> Vec<Vec<Vec<u8>>> {
    let mut batches = Vec::with_capacity(ids.len().div_ceil(batch_size));
    let mut remaining = ids.into_iter();
    loop {
        let batch: Vec<_> = remaining.by_ref().take(batch_size).collect();
        if batch.is_empty() {
            return batches;
        }
        batches.push(batch);
    }
}

pub(crate) fn batch_range(
    total: usize,
    batch_size: usize,
    group: u32,
    offset: u32,
) -> Result<std::ops::Range<usize>, DatasetError> {
    let group = usize::try_from(group).map_err(|_| DatasetError::CursorMismatch)?;
    let offset = usize::try_from(offset).map_err(|_| DatasetError::CursorMismatch)?;
    let windows = total.div_ceil(batch_size);
    if group > windows || (group == windows && offset != 0) {
        return Err(DatasetError::CursorMismatch);
    }
    if group == windows {
        return Ok(total..total);
    }
    let start = group
        .checked_mul(batch_size)
        .ok_or(DatasetError::CursorMismatch)?;
    let end = start.saturating_add(batch_size).min(total);
    if offset > end - start {
        return Err(DatasetError::CursorMismatch);
    }
    Ok(start + offset..end)
}

/// Select an already-validated ordinal range without sorting discarded IDs.
pub(crate) fn sorted_window<T: Ord>(mut values: Vec<T>, range: std::ops::Range<usize>) -> Vec<T> {
    if range.is_empty() {
        return Vec::new();
    }
    if range.end < values.len() {
        values.select_nth_unstable(range.end);
        values.truncate(range.end);
    }
    if range.start > 0 {
        values.select_nth_unstable(range.start);
    }
    let mut selected: Vec<_> = values.drain(range).collect();
    selected.sort_unstable();
    selected
}
