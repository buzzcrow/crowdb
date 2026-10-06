use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

use crate::{DatasetError, ManifestRecord, ReadCursor, ReadPlan};

pub const DEFAULT_MAX_SHUFFLE_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_MAX_SHUFFLE_OBJECTS: usize = 100_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShuffleSpec {
    pub seed: u64,
    pub epoch: u64,
    pub max_shuffle_bytes: usize,
    pub max_shuffle_objects: usize,
}

impl Default for ShuffleSpec {
    fn default() -> Self {
        Self {
            seed: 0,
            epoch: 0,
            max_shuffle_bytes: DEFAULT_MAX_SHUFFLE_BYTES,
            max_shuffle_objects: DEFAULT_MAX_SHUFFLE_OBJECTS,
        }
    }
}

impl ShuffleSpec {
    /// # Errors
    /// Rejects zero or otherwise unusable shuffle limits.
    pub fn validate(&self) -> Result<(), DatasetError> {
        if self.max_shuffle_bytes == 0 || self.max_shuffle_objects == 0 {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GroupShuffle {
    Prefix(Vec<u8>),
    MetadataKey(String),
}

impl GroupShuffle {
    /// # Errors
    /// Rejects empty grouping keys and unsupported metadata grouping.
    pub fn validate(&self) -> Result<(), DatasetError> {
        match self {
            Self::Prefix(prefix) if prefix.is_empty() => Err(DatasetError::InvalidManifest),
            Self::MetadataKey(key) if key.is_empty() => Err(DatasetError::InvalidManifest),
            Self::MetadataKey(_) => Err(DatasetError::UnsupportedGrouping),
            Self::Prefix(_) => Ok(()),
        }
    }
}

pub struct SampleShuffle;

impl SampleShuffle {
    /// Deterministically shuffles only selected IDs and locator metadata.
    ///
    /// # Errors
    /// Rejects invalid plans, snapshot mismatches, and shuffle admission limits.
    pub fn execute(
        plan: &ReadPlan,
        manifest: &ManifestRecord,
        spec: &ShuffleSpec,
    ) -> Result<Vec<Vec<u8>>, DatasetError> {
        spec.validate()?;
        let ids = plan.scan_ids(manifest)?;
        if ids.len() > spec.max_shuffle_objects {
            return Err(DatasetError::ReadLimitExceeded);
        }
        let metadata_bytes = manifest
            .samples
            .iter()
            .filter(|sample| ids.iter().any(|id| id == &sample.sample_id))
            .map(|sample| {
                sample.sample_id.len()
                    + sample
                        .fields
                        .iter()
                        .map(|field| field.name.len() + locator_size(&field.value))
                        .sum::<usize>()
            })
            .sum::<usize>();
        if metadata_bytes > spec.max_shuffle_bytes {
            return Err(DatasetError::ReadLimitExceeded);
        }
        let mut keyed = ids
            .into_iter()
            .map(|id| (shuffle_key(spec, plan, &id), id))
            .collect::<Vec<_>>();
        keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        Ok(keyed.into_iter().map(|(_, id)| id).collect())
    }

    /// Produces stable bounded windows from one globally shuffled sequence.
    /// Every window contains only IDs and lightweight locator metadata remains
    /// bounded by the configured object limit.
    ///
    /// # Errors
    /// Propagates the same admission and validation errors as [`Self::execute`].
    pub fn execute_windows(
        plan: &ReadPlan,
        manifest: &ManifestRecord,
        spec: &ShuffleSpec,
    ) -> Result<Vec<Vec<Vec<u8>>>, DatasetError> {
        let ids = Self::execute(plan, manifest, spec)?;
        Ok(ids.chunks(plan.batch_size).map(<[Vec<u8>]>::to_vec).collect())
    }

    /// Groups selected IDs using the indexed prefix grouping rule.
    ///
    /// # Errors
    /// Rejects unsupported grouping rules or a selection that cannot be
    /// represented exactly by the indexed groups.
    pub fn execute_groups(
        plan: &ReadPlan,
        manifest: &ManifestRecord,
        spec: &ShuffleSpec,
        grouping: &GroupShuffle,
    ) -> Result<Vec<Vec<Vec<u8>>>, DatasetError> {
        grouping.validate()?;
        let ids = Self::execute(plan, manifest, spec)?;
        let GroupShuffle::Prefix(prefix) = grouping else {
            return Err(DatasetError::UnsupportedGrouping);
        };
        let mut groups = std::collections::BTreeMap::<Vec<u8>, Vec<Vec<u8>>>::new();
        for id in ids {
            if !id.starts_with(prefix) {
                return Err(DatasetError::ShuffleIntegrity);
            }
            let key = id
                .iter()
                .position(|byte| *byte == b'-')
                .map_or_else(|| id.clone(), |end| id[..end].to_vec());
            groups.entry(key).or_default().push(id);
        }
        Ok(groups.into_values().collect())
    }

    /// Resumes at the last confirmed group and offset. The unconfirmed group
    /// is replayed in full, so a worker interruption cannot silently skip IDs.
    ///
    /// # Errors
    /// Rejects cursors bound to another plan or invalid group offsets.
    pub fn resume(
        plan: &ReadPlan,
        manifest: &ManifestRecord,
        spec: &ShuffleSpec,
        cursor: &ReadCursor,
    ) -> Result<Vec<Vec<u8>>, DatasetError> {
        cursor.validate_for(plan)?;
        let windows = Self::execute_windows(plan, manifest, spec)?;
        let group = usize::try_from(cursor.group).map_err(|_| DatasetError::CursorMismatch)?;
        if group > windows.len() {
            return Err(DatasetError::CursorMismatch);
        }
        if group == windows.len() {
            if cursor.offset != 0 {
                return Err(DatasetError::CursorMismatch);
            }
            return Ok(Vec::new());
        }
        let offset = usize::try_from(cursor.offset).map_err(|_| DatasetError::CursorMismatch)?;
        if offset > windows[group].len() {
            return Err(DatasetError::CursorMismatch);
        }
        let mut result = windows[group][offset..].to_vec();
        result.extend(windows.iter().skip(group + 1).flatten().cloned());
        Ok(result)
    }
}

fn locator_size(locator: &crate::FieldLocator) -> usize {
    match locator {
        crate::FieldLocator::Inline { value, .. } => value.len(),
        crate::FieldLocator::Chunk { location, .. } => location.len(),
        crate::FieldLocator::Tombstone => 0,
    }
}

fn shuffle_key(spec: &ShuffleSpec, plan: &ReadPlan, id: &[u8]) -> [u8; 16] {
    let mut digest = Md5::new();
    digest.update(spec.seed.to_le_bytes());
    digest.update(spec.epoch.to_le_bytes());
    digest.update(plan.identity());
    digest.update(id);
    digest.finalize().into()
}
