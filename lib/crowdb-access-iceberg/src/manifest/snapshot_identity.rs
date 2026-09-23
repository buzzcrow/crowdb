use std::collections::{BTreeMap, BTreeSet};

use crate::file::{ContentFormat, FileLocation, TableLocation};

use super::{EntryStatus, FileContentKind, ManifestScalarEntry};

#[derive(Clone, Copy, Debug)]
pub struct SnapshotIdentityLimits {
    pub keys: usize,
    pub key_bytes: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotIdentityError {
    #[error("snapshot identity index limit exceeded")]
    Bounds,
    #[error("snapshot contains duplicate live file, manifest or deletion-vector target")]
    Duplicate,
    #[error("snapshot file declarations disagree or escape their selected scope")]
    Binding,
    #[error("snapshot deletion-vector spans overlap or escape their containing file")]
    Span,
    #[error("snapshot identity index has already failed")]
    Failed,
}

struct FileState {
    length: u64,
    vector: bool,
}

pub struct SnapshotIdentityIndex {
    table: TableLocation,
    limits: SnapshotIdentityLimits,
    keys: usize,
    key_bytes: usize,
    manifests: BTreeSet<String>,
    files: BTreeMap<String, FileState>,
    vectors: BTreeMap<(String, u64), u64>,
    targets: BTreeSet<String>,
    failed: bool,
}

impl SnapshotIdentityIndex {
    /// Exact transient membership checks with independent node and retained-key byte caps.
    /// No entries, schemas, metrics or bitmap contents are retained. Exceeding the caps
    /// fails validation; entries are never evicted to claim a partial uniqueness proof.
    /// # Errors
    /// Rejects zero or unbounded index limits.
    pub fn new(table: TableLocation, limits: SnapshotIdentityLimits) -> Result<Self, SnapshotIdentityError> {
        if limits.keys == 0
            || limits.keys > 1_000_000
            || limits.key_bytes == 0
            || limits.key_bytes > 64 * 1024 * 1024
        {
            return Err(SnapshotIdentityError::Bounds);
        }
        Ok(Self {
            table,
            limits,
            keys: 0,
            key_bytes: 0,
            manifests: BTreeSet::new(),
            files: BTreeMap::new(),
            vectors: BTreeMap::new(),
            targets: BTreeSet::new(),
            failed: false,
        })
    }

    /// # Errors
    /// Rejects a repeated or foreign manifest. Any error permanently poisons this index.
    pub fn observe_manifest(&mut self, location: &FileLocation) -> Result<(), SnapshotIdentityError> {
        self.begin()?;
        self.check_table(location)?;
        let key = location.relative_key();
        if self.manifests.contains(key) {
            return Err(SnapshotIdentityError::Duplicate);
        }
        self.reserve(key.len())?;
        self.manifests.insert(key.to_owned());
        self.failed = false;
        Ok(())
    }

    /// Checks one semantically validated manifest entry; deleted entries do not count
    /// as live references. This incremental index is not an enumeration/EOF proof.
    /// # Errors
    /// Rejects duplicate live paths, inconsistent Puffin sizes, repeated targets and
    /// overlapping DV spans. Distinct DVs may share one Puffin file.
    pub fn observe_entry(&mut self, entry: &ManifestScalarEntry) -> Result<(), SnapshotIdentityError> {
        self.begin()?;
        self.check_table(&entry.file.location)?;
        if entry.entry.status != EntryStatus::Deleted {
            self.observe_live(entry)?;
        }
        self.failed = false;
        Ok(())
    }

    fn observe_live(&mut self, entry: &ManifestScalarEntry) -> Result<(), SnapshotIdentityError> {
        let key = entry.file.location.relative_key();
        let vector = entry.file.deletion_vector.is_some();
        if let Some(previous) = self.files.get(key) {
            if !previous.vector || !vector {
                return Err(SnapshotIdentityError::Duplicate);
            }
            if previous.length != entry.file.length {
                return Err(SnapshotIdentityError::Binding);
            }
        } else {
            self.reserve(key.len())?;
            self.files.insert(
                key.to_owned(),
                FileState {
                    length: entry.file.length,
                    vector,
                },
            );
        }
        if vector {
            self.observe_vector(entry)?;
        } else if entry.file.format == ContentFormat::Puffin {
            return Err(SnapshotIdentityError::Binding);
        }
        Ok(())
    }

    fn observe_vector(&mut self, entry: &ManifestScalarEntry) -> Result<(), SnapshotIdentityError> {
        if entry.file.format != ContentFormat::Puffin
            || entry.entry.content != FileContentKind::PositionDeletes
        {
            return Err(SnapshotIdentityError::Binding);
        }
        let target = entry
            .file
            .referenced_data_file
            .as_ref()
            .ok_or(SnapshotIdentityError::Binding)?;
        self.check_table(target)?;
        if self.targets.contains(target.relative_key()) {
            return Err(SnapshotIdentityError::Duplicate);
        }
        let span = entry.file.deletion_vector.ok_or(SnapshotIdentityError::Binding)?;
        let end = span
            .offset
            .checked_add(span.length)
            .filter(|end| span.length > 0 && *end <= entry.file.length)
            .ok_or(SnapshotIdentityError::Span)?;
        let key = (entry.file.location.relative_key().to_owned(), span.offset);
        if self
            .vectors
            .range(..=key.clone())
            .next_back()
            .is_some_and(|((path, _), previous_end)| path == &key.0 && *previous_end > span.offset)
            || self
                .vectors
                .range(key.clone()..)
                .next()
                .is_some_and(|((path, offset), _)| path == &key.0 && *offset < end)
        {
            return Err(SnapshotIdentityError::Span);
        }
        self.reserve(key.0.len())?;
        self.reserve(target.relative_key().len())?;
        self.vectors.insert(key, end);
        self.targets.insert(target.relative_key().to_owned());
        Ok(())
    }

    fn begin(&mut self) -> Result<(), SnapshotIdentityError> {
        if self.failed {
            return Err(SnapshotIdentityError::Failed);
        }
        self.failed = true;
        Ok(())
    }

    fn check_table(&self, location: &FileLocation) -> Result<(), SnapshotIdentityError> {
        if location.table() != self.table {
            return Err(SnapshotIdentityError::Binding);
        }
        Ok(())
    }

    fn reserve(&mut self, bytes: usize) -> Result<(), SnapshotIdentityError> {
        let keys = self
            .keys
            .checked_add(1)
            .filter(|keys| *keys <= self.limits.keys)
            .ok_or(SnapshotIdentityError::Bounds)?;
        let key_bytes = self
            .key_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.limits.key_bytes)
            .ok_or(SnapshotIdentityError::Bounds)?;
        self.keys = keys;
        self.key_bytes = key_bytes;
        Ok(())
    }
}
