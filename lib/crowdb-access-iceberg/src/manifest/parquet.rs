use std::sync::Arc;

use super::{EntryStatus, FileContentKind, ManifestScalarEntry};
use crate::file::{
    read_parquet_metadata, ContentFormat, FileBlockStore, FileKind, FileRecord, ParquetMetadata,
    ParquetMetadataError, ParquetMetadataLimits, TableLocation,
};

#[derive(Debug, thiserror::Error)]
pub enum SelectedParquetError {
    #[error("selected manifest descriptor does not match the Parquet file")]
    Binding,
    #[error("Parquet footer row count disagrees with the manifest")]
    Rows,
    #[error(transparent)]
    Metadata(#[from] ParquetMetadataError),
}

/// Reads footer metadata for an already validated live manifest entry.
/// Binds table, location, format, length and selected content kind before I/O,
/// without modifying the immutable upload. Checks declared rows against the footer,
/// not against decoded pages. This is not schema compatibility, delete semantics,
/// snapshot membership or commit validation.
/// # Errors
/// Rejects mismatched descriptors, invalid footer metadata and conflicting row counts.
pub async fn read_selected_parquet_metadata(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    entry: &ManifestScalarEntry,
    table: TableLocation,
    limits: ParquetMetadataLimits,
) -> Result<ParquetMetadata, SelectedParquetError> {
    let binding = SelectedParquetError::Binding;
    if record.location.table() != table
        || record.location != entry.file.location
        || record.length != entry.file.length
        || record.format != ContentFormat::Parquet
        || entry.file.format != ContentFormat::Parquet
        || entry.entry.status == EntryStatus::Deleted
        || entry.entry.record_count < 0
        || entry.file.deletion_vector.is_some()
    {
        return Err(binding);
    }
    let kind = match entry.entry.content {
        FileContentKind::Data => FileKind::Data,
        FileContentKind::PositionDeletes => FileKind::PositionDelete,
        FileContentKind::EqualityDeletes => FileKind::EqualityDelete,
    };
    let bound = record.bind_kind(kind).map_err(|_| binding)?;
    let metadata = read_parquet_metadata(store, &bound, limits).await?;
    if metadata.rows != u64::try_from(entry.entry.record_count).map_err(|_| SelectedParquetError::Rows)? {
        return Err(SelectedParquetError::Rows);
    }
    Ok(metadata)
}
