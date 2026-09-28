use std::sync::Arc;

use async_trait::async_trait;

use crate::file::{ContentFormat, FileBlockStore, FileLocation, FileRecord};

use super::{
    EntryStatus, FileContentKind, ManifestListSelection, ParquetFieldMapping, PositionDeleteLimits,
    SelectedParquetError, SnapshotDvError, SnapshotDvLimits, SnapshotDvScope, SnapshotManifestError,
    SnapshotManifestLimits, SnapshotManifestReader, SnapshotManifestSource, SnapshotManifestSummary,
};

mod index;
mod preservation;
mod selected;

pub use preservation::validate_snapshot_delete_preservation;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotValidationError {
    #[error(transparent)]
    Manifest(#[from] SnapshotManifestError),
    #[error(transparent)]
    Parquet(#[from] SelectedParquetError),
    #[error(transparent)]
    Vector(#[from] SnapshotDvError),
    #[error("selected snapshot file binding mismatch")]
    Binding,
    #[error("selected snapshot file validation resource limit exceeded")]
    Bounds,
    #[error("selected snapshot file format is not supported")]
    Unsupported,
    #[error("selected snapshot file authority is unavailable")]
    Unavailable,
    #[error("selected snapshot file lookup failed: {0}")]
    Source(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait SnapshotFileSource: Send + Sync {
    async fn resolve(&self, location: &FileLocation) -> Result<FileRecord, SnapshotValidationError>;
}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotFileLimits {
    pub manifests: SnapshotManifestLimits,
    pub data_files: usize,
    pub index_bytes: usize,
    pub position_deletes: PositionDeleteLimits,
    pub vectors: SnapshotDvLimits,
    pub delete_rows: u64,
}

pub struct SnapshotValidationInput {
    pub scope: SnapshotDvScope,
    pub list: FileRecord,
    pub selection: ManifestListSelection,
    pub manifests: Arc<dyn SnapshotManifestSource>,
    pub files: Arc<dyn SnapshotFileSource>,
    pub mapping: Option<ParquetFieldMapping>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotFileSummary {
    pub scope: SnapshotDvScope,
    pub manifests: SnapshotManifestSummary,
    pub data_files: u64,
    pub data_rows: u64,
    pub delete_rows: u64,
    pub equality_files: u64,
    pub position_files: u64,
    pub position_rows: u64,
    pub applicable_position_rows: u64,
    pub vectors: u64,
    pub vector_bytes: u64,
}

/// Enumerates canonical manifests for data rows, then DVs, then other deletes.
/// Only returns after every manifest EOF and supported selected-file check succeeds.
/// This validates Parquet schemas/footer counts, position pairs and DV payloads, not
/// arbitrary data/equality values, prior-snapshot DV preservation or table head CAS.
/// Sources must belong to one trusted candidate metadata generation; publication
/// must independently fence that generation and the catalog context.
/// # Errors
/// Rejects unsupported formats, inconsistent scopes, missing files and exceeded budgets.
pub async fn validate_snapshot_files(
    store: Arc<dyn FileBlockStore>,
    input: SnapshotValidationInput,
    limits: SnapshotFileLimits,
) -> Result<SnapshotFileSummary, SnapshotValidationError> {
    Ok(validate(store, &input, limits).await?.1)
}

async fn validate(
    store: Arc<dyn FileBlockStore>,
    input: &SnapshotValidationInput,
    limits: SnapshotFileLimits,
) -> Result<(index::DataIndex, SnapshotFileSummary), SnapshotValidationError> {
    validate_input(input, limits)?;
    let mut index = index::DataIndex::new(limits);
    let mut summary = SnapshotFileSummary {
        scope: input.scope,
        manifests: SnapshotManifestSummary::default(),
        data_files: 0,
        data_rows: 0,
        delete_rows: 0,
        equality_files: 0,
        position_files: 0,
        position_rows: 0,
        applicable_position_rows: 0,
        vectors: 0,
        vector_bytes: 0,
    };
    let mut reader = open(store.clone(), input, limits).await?;
    while let Some(entry) = reader.next_entry().await? {
        if entry.entry.status == EntryStatus::Deleted {
            continue;
        }
        if entry.entry.content == FileContentKind::Data {
            let (manifest, context) = reader
                .current_manifest()
                .ok_or(SnapshotValidationError::Binding)?;
            index
                .data(store.clone(), input, entry, manifest, context, &mut summary)
                .await?;
        }
    }
    summary.manifests = reader.finish()?;
    drop(reader);
    let mut reader = open(store.clone(), input, limits).await?;
    while let Some(entry) = reader.next_entry().await? {
        if entry.entry.status == EntryStatus::Deleted || entry.file.format != ContentFormat::Puffin {
            continue;
        }
        let (_, context) = reader
            .current_manifest()
            .ok_or(SnapshotValidationError::Binding)?;
        selected::vector(
            store.clone(),
            input,
            &mut index,
            &entry,
            context,
            &mut summary,
            limits,
        )
        .await?;
    }
    if reader.finish()? != summary.manifests {
        return Err(SnapshotValidationError::Binding);
    }
    drop(reader);
    let mut reader = open(store.clone(), input, limits).await?;
    while let Some(entry) = reader.next_entry().await? {
        if entry.entry.status == EntryStatus::Deleted
            || entry.entry.content == FileContentKind::Data
            || entry.file.format == ContentFormat::Puffin
        {
            continue;
        }
        let (_, context) = reader
            .current_manifest()
            .ok_or(SnapshotValidationError::Binding)?;
        selected::delete(
            store.clone(),
            input,
            &index,
            &entry,
            context,
            &mut summary,
            limits,
        )
        .await?;
    }
    if reader.finish()? != summary.manifests {
        return Err(SnapshotValidationError::Binding);
    }
    Ok((index, summary))
}

async fn open(
    store: Arc<dyn FileBlockStore>,
    input: &SnapshotValidationInput,
    limits: SnapshotFileLimits,
) -> Result<SnapshotManifestReader, SnapshotValidationError> {
    Ok(SnapshotManifestReader::open(
        store,
        input.manifests.clone(),
        input.list.clone(),
        input.selection.clone(),
        limits.manifests,
    )
    .await?)
}

fn validate_input(
    input: &SnapshotValidationInput,
    limits: SnapshotFileLimits,
) -> Result<(), SnapshotValidationError> {
    if input.scope.context.validate().is_err()
        || input.scope.context.catalog != input.scope.table.catalog
        || input.scope.table != input.selection.location.table()
        || input.scope.manifest_list != input.list.file
        || input.scope.snapshot_id != input.selection.snapshot_id
        || input.scope.sequence != input.selection.sequence
    {
        return Err(SnapshotValidationError::Binding);
    }
    if limits.data_files == 0
        || limits.data_files > 1_000_000
        || limits.index_bytes == 0
        || limits.index_bytes > 256 * 1024 * 1024
        || limits.manifests.manifests > 4096
        || limits.delete_rows == 0
        || limits.vectors.vectors == 0
        || limits.vectors.vectors > 1_000_000
        || limits.vectors.blob_bytes == 0
    {
        return Err(SnapshotValidationError::Bounds);
    }
    Ok(())
}
