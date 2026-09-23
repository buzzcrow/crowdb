use std::sync::Arc;

use async_trait::async_trait;

use crate::file::{AvroDatumLimits, AvroLimits, FileBlockStore, FileLocation, FileRecord};

use super::{
    ManifestContext, ManifestEntryError, ManifestListError, ManifestListReader, ManifestListSelection,
    ManifestReader, ManifestScalarEntry,
};

#[derive(Clone, Copy, Debug)]
pub struct SnapshotManifestLimits {
    pub framing: AvroLimits,
    pub datum: AvroDatumLimits,
    pub decoded_bytes: usize,
    pub manifests: u64,
    pub entries: u64,
    pub manifest_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotManifestSummary {
    pub manifests: u64,
    pub entries: u64,
    pub manifest_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotManifestError {
    #[error(transparent)]
    List(#[from] ManifestListError),
    #[error(transparent)]
    Manifest(#[from] ManifestEntryError),
    #[error("snapshot manifest enumeration work limit exceeded")]
    Bounds,
    #[error("snapshot manifest row-ID assignments overlap or escape the allocated range")]
    RowIds,
    #[error("snapshot manifest enumeration failed, was cancelled or is incomplete")]
    Incomplete,
    #[error("selected manifest authority or historical context is unavailable")]
    Unavailable,
    #[error("selected manifest lookup failed: {0}")]
    Source(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait SnapshotManifestSource: Send + Sync {
    /// Resolves canonical authority and trusted historical schema/spec context for one
    /// selected reference. Implementations must fence the candidate table generation.
    async fn resolve(
        &self,
        location: &FileLocation,
    ) -> Result<(FileRecord, ManifestContext), SnapshotManifestError>;
}

pub struct SnapshotManifestReader {
    store: Arc<dyn FileBlockStore>,
    source: Arc<dyn SnapshotManifestSource>,
    list: ManifestListReader,
    manifest: Option<ManifestReader>,
    limits: SnapshotManifestLimits,
    summary: SnapshotManifestSummary,
    failed: bool,
    complete: bool,
    rows: super::snapshot_rows::SnapshotRowAssignments,
}

impl SnapshotManifestReader {
    /// Enumerates every entry of every selected manifest, including deleted entries.
    /// Retains one list block and one manifest reader, never a snapshot-sized vector.
    /// # Errors
    /// Rejects invalid budgets, snapshot selection or canonical manifest-list bytes.
    pub async fn open(
        store: Arc<dyn FileBlockStore>,
        source: Arc<dyn SnapshotManifestSource>,
        record: FileRecord,
        selection: ManifestListSelection,
        limits: SnapshotManifestLimits,
    ) -> Result<Self, SnapshotManifestError> {
        if limits.manifests == 0 || limits.entries == 0 || limits.manifest_bytes == 0 {
            return Err(SnapshotManifestError::Bounds);
        }
        let rows = super::snapshot_rows::SnapshotRowAssignments::new(&selection)?;
        let list = ManifestListReader::open_selected(
            store.clone(),
            record,
            selection,
            limits.framing,
            limits.datum,
            limits.decoded_bytes,
        )
        .await?;
        Ok(Self {
            store,
            source,
            list,
            manifest: None,
            limits,
            summary: SnapshotManifestSummary::default(),
            failed: false,
            complete: false,
            rows,
        })
    }

    #[must_use]
    pub fn current_manifest(&self) -> Option<(&FileLocation, &ManifestContext)> {
        self.manifest.as_ref().map(ManifestReader::selection)
    }

    /// Returns enumeration totals only after both the list and every manifest reached EOF.
    /// This is not a data-file, cross-manifest uniqueness or commit-publication proof.
    /// # Errors
    /// Rejects partial, failed or cancelled enumeration.
    pub fn finish(&self) -> Result<SnapshotManifestSummary, SnapshotManifestError> {
        if self.failed || !self.complete {
            return Err(SnapshotManifestError::Incomplete);
        }
        Ok(self.summary)
    }

    /// # Errors
    /// Any error or cancellation permanently poisons the entire enumeration. A failed
    /// manifest is never skipped; its EOF totals must pass before the next reference.
    pub async fn next_entry(&mut self) -> Result<Option<ManifestScalarEntry>, SnapshotManifestError> {
        if self.failed {
            return Err(SnapshotManifestError::Incomplete);
        }
        if self.complete {
            return Ok(None);
        }
        self.failed = true;
        loop {
            if let Some(manifest) = &mut self.manifest {
                if let Some(entry) = manifest.next_entry().await? {
                    self.rows.check(manifest.next_row_id())?;
                    self.summary.entries = bounded_add(self.summary.entries, 1, self.limits.entries)?;
                    self.failed = false;
                    return Ok(Some(entry));
                }
                self.rows.finish_manifest(manifest.next_row_id())?;
                self.manifest = None;
            }
            let Some(reference) = self.list.next_entry().await? else {
                self.complete = true;
                self.failed = false;
                return Ok(None);
            };
            let manifests = bounded_add(self.summary.manifests, 1, self.limits.manifests)?;
            self.rows.begin(&reference)?;
            let bytes = bounded_add(
                self.summary.manifest_bytes,
                reference.length,
                self.limits.manifest_bytes,
            )?;
            let (record, context) = self.source.resolve(&reference.location).await?;
            self.manifest = Some(
                ManifestReader::open(
                    self.store.clone(),
                    record,
                    reference,
                    context,
                    self.limits.framing,
                    self.limits.datum,
                    self.limits.decoded_bytes,
                )
                .await?,
            );
            self.summary.manifests = manifests;
            self.summary.manifest_bytes = bytes;
        }
    }
}

fn bounded_add(current: u64, added: u64, limit: u64) -> Result<u64, SnapshotManifestError> {
    current
        .checked_add(added)
        .filter(|value| *value <= limit)
        .ok_or(SnapshotManifestError::Bounds)
}
