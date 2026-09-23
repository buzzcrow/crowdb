use std::sync::Arc;

use super::{ManifestContext, ManifestScalarEntry};
use crate::catalog::CatalogContext;
use crate::file::{
    validate_deletion_vector, DeletionVectorError, DeletionVectorLimits, DeletionVectorStats, FileBlockStore,
    FileLocation, FileRecord, TableLocation,
};
use crate::key::FileId;

mod binding;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotDvScope {
    pub context: CatalogContext,
    pub table: TableLocation,
    pub snapshot_id: i64,
    pub sequence: i64,
    pub manifest_list: FileId,
}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotDvLimits {
    pub vectors: u64,
    pub blob_bytes: u64,
    pub vector: DeletionVectorLimits,
}

#[derive(Clone, Copy)]
pub struct SnapshotFile<'entry> {
    pub entry: &'entry ManifestScalarEntry,
    pub record: &'entry FileRecord,
    pub context: &'entry ManifestContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotDvSummary {
    pub scope: SnapshotDvScope,
    pub vectors: u64,
    pub blob_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotDvError {
    #[error("deletion vector and data file do not belong to the selected snapshot scope")]
    Binding,
    #[error("snapshot deletion vector references are duplicated or out of order")]
    Order,
    #[error("snapshot deletion vector validation limit exceeded")]
    Bounds,
    #[error("deletion vector references a row outside its data file")]
    Position,
    #[error("snapshot deletion vector validation failed, was cancelled or is incomplete")]
    Incomplete,
    #[error(transparent)]
    Vector(#[from] DeletionVectorError),
}

pub struct SnapshotDvValidator {
    store: Arc<dyn FileBlockStore>,
    scope: SnapshotDvScope,
    expected: u64,
    limits: SnapshotDvLimits,
    previous: Option<FileLocation>,
    checked: u64,
    bytes: u64,
    failed: bool,
}

impl SnapshotDvValidator {
    /// Checks a complete stream sorted by referenced canonical key, retaining only one key.
    /// The caller supplies the trusted live-DV count from fully scanned manifests, fully
    /// validated live entries, and their historical contexts. Commit publication must separately
    /// fence this scope and prove that the enumerator covers the entire candidate snapshot.
    /// # Errors
    /// Rejects invalid scope or independent count, byte and per-vector work limits.
    pub fn new(
        store: Arc<dyn FileBlockStore>,
        scope: SnapshotDvScope,
        expected: u64,
        limits: SnapshotDvLimits,
    ) -> Result<Self, SnapshotDvError> {
        if scope.context.validate().is_err()
            || scope.context.catalog != scope.table.catalog
            || scope.snapshot_id <= 0
            || scope.sequence < 0
        {
            return Err(SnapshotDvError::Binding);
        }
        if limits.vectors == 0
            || limits.vectors > 1_000_000
            || expected > limits.vectors
            || limits.blob_bytes == 0
            || limits.blob_bytes > u64::MAX / 8
            || limits.vector.blob_bytes < 20
            || limits.vector.blob_bytes > u64::from(u32::MAX) + 8
            || limits.vector.bitmaps == 0
            || limits.vector.bitmaps > 1_000_000
        {
            return Err(SnapshotDvError::Bounds);
        }
        Ok(Self {
            store,
            scope,
            expected,
            limits,
            previous: None,
            checked: 0,
            bytes: 0,
            failed: false,
        })
    }

    #[must_use]
    pub fn checked(&self) -> u64 {
        self.checked
    }

    /// Binds one live DV to its canonical Puffin descriptor and same-snapshot live data file.
    /// # Errors
    /// Rejects scope, partition, sequence, descriptor, bitmap or row-range mismatches.
    /// Any error or cancelled read poisons the stream; only successful checks advance progress.
    pub async fn check(
        &mut self,
        scope: SnapshotDvScope,
        vector: SnapshotFile<'_>,
        data: SnapshotFile<'_>,
    ) -> Result<DeletionVectorStats, SnapshotDvError> {
        if self.failed {
            return Err(SnapshotDvError::Incomplete);
        }
        self.failed = true;
        if scope != self.scope {
            return Err(SnapshotDvError::Binding);
        }
        let reference = binding::reference(self.scope, vector, data)?;
        if self
            .previous
            .as_ref()
            .is_some_and(|previous| previous.relative_key() >= reference.referenced.relative_key())
        {
            return Err(SnapshotDvError::Order);
        }
        let bytes = self
            .bytes
            .checked_add(reference.span.length)
            .ok_or(SnapshotDvError::Bounds)?;
        if self.checked >= self.expected || bytes > self.limits.blob_bytes {
            return Err(SnapshotDvError::Bounds);
        }
        let rows = u64::try_from(data.entry.entry.record_count).map_err(|_| SnapshotDvError::Binding)?;
        if reference.cardinality > rows {
            return Err(SnapshotDvError::Position);
        }
        let result =
            validate_deletion_vector(self.store.clone(), vector.record, &reference, self.limits.vector)
                .await?;
        if result.maximum_position.is_some_and(|position| position >= rows) {
            return Err(SnapshotDvError::Position);
        }
        self.previous = Some(reference.referenced);
        self.checked += 1;
        self.bytes = bytes;
        self.failed = false;
        Ok(result)
    }

    /// Finishes only after exactly the trusted number of live vectors has been validated.
    /// # Errors
    /// Rejects early EOF, any previous failure, or a cancelled read.
    pub fn finish(self) -> Result<SnapshotDvSummary, SnapshotDvError> {
        if self.failed || self.checked != self.expected {
            return Err(SnapshotDvError::Incomplete);
        }
        Ok(SnapshotDvSummary {
            scope: self.scope,
            vectors: self.checked,
            blob_bytes: self.bytes,
        })
    }
}
