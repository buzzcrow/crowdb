use std::sync::Arc;

use async_trait::async_trait;

use crate::file::{
    validate_deletion_vector, ContentFormat, DeletionVectorReference, FileBlockStore, FileKind, FileLocation,
};
use crate::manifest::{
    read_parquet_selection, validate_parquet_position_deletes, FileContentKind, ManifestContext,
    ManifestScalarEntry, ParquetSelection, PositionDeleteTargets, SelectedParquetError, SnapshotDvError,
    SnapshotFile,
};

use super::{
    index::DataIndex, SnapshotFileLimits, SnapshotFileSummary, SnapshotValidationError as Error,
    SnapshotValidationInput,
};

pub(super) async fn delete(
    store: Arc<dyn FileBlockStore>,
    input: &SnapshotValidationInput,
    index: &DataIndex,
    entry: &ManifestScalarEntry,
    context: &ManifestContext,
    summary: &mut SnapshotFileSummary,
    limits: SnapshotFileLimits,
) -> Result<(), Error> {
    let rows = u64::try_from(entry.entry.record_count).map_err(|_| Error::Binding)?;
    let processed = summary.delete_rows.checked_add(rows).ok_or(Error::Bounds)?;
    if processed > limits.delete_rows {
        return Err(Error::Bounds);
    }
    let record = input.files.resolve(&entry.file.location).await?;
    let selection = ParquetSelection {
        entry,
        context,
        table: input.scope.table,
        mapping: input.mapping.as_ref(),
    };
    match (entry.file.format, entry.entry.content) {
        (ContentFormat::Parquet, FileContentKind::EqualityDeletes) => {
            read_parquet_selection(store, &record, &selection, limits.position_deletes.metadata).await?;
            summary.equality_files += 1;
        }
        (ContentFormat::Parquet, FileContentKind::PositionDeletes) => {
            let targets = Targets {
                index,
                delete: SnapshotFile {
                    entry,
                    record: &record,
                    context,
                },
            };
            let checked = validate_parquet_position_deletes(
                store,
                &record,
                selection,
                &targets,
                limits.position_deletes,
            )
            .await?;
            summary.position_files += 1;
            summary.position_rows = summary.position_rows.checked_add(rows).ok_or(Error::Bounds)?;
            summary.applicable_position_rows = summary
                .applicable_position_rows
                .checked_add(checked.applicable_rows)
                .ok_or(Error::Bounds)?;
        }
        _ => return Err(Error::Unsupported),
    }
    summary.delete_rows = processed;
    Ok(())
}

pub(super) async fn vector(
    store: Arc<dyn FileBlockStore>,
    input: &SnapshotValidationInput,
    index: &mut DataIndex,
    entry: &ManifestScalarEntry,
    context: &ManifestContext,
    summary: &mut SnapshotFileSummary,
    limits: SnapshotFileLimits,
) -> Result<(), Error> {
    let target = entry.file.referenced_data_file.as_ref().ok_or(Error::Binding)?;
    let span = entry.file.deletion_vector.ok_or(Error::Binding)?;
    let cardinality = u64::try_from(entry.entry.record_count).map_err(|_| Error::Binding)?;
    let bytes = summary
        .vector_bytes
        .checked_add(span.length)
        .ok_or(Error::Bounds)?;
    let rows = summary
        .delete_rows
        .checked_add(cardinality)
        .ok_or(Error::Bounds)?;
    if bytes > limits.vectors.blob_bytes
        || summary.vectors >= limits.vectors.vectors
        || rows > limits.delete_rows
    {
        return Err(Error::Bounds);
    }
    let record = input
        .files
        .resolve(&entry.file.location)
        .await?
        .bind_kind(FileKind::DeletionVector)
        .map_err(|_| Error::Binding)?;
    let selected = SnapshotFile {
        entry,
        record: &record,
        context,
    };
    crate::manifest::deletion_vectors::validate_file(input.scope, selected)?;
    let checked = validate_deletion_vector(
        store,
        &record,
        &DeletionVectorReference {
            referenced: target.clone(),
            span,
            cardinality,
        },
        limits.vectors.vector,
    )
    .await
    .map_err(SnapshotDvError::from)?;
    if let Some(data) = index.files.get(target.relative_key()) {
        if data.entry.inherited.data_sequence <= entry.inherited.data_sequence
            && crate::manifest::deletion_vectors::partitions(selected, data.selected()).is_ok()
        {
            let data_rows = u64::try_from(data.entry.entry.record_count).map_err(|_| Error::Binding)?;
            if cardinality > data_rows
                || checked
                    .maximum_position
                    .is_some_and(|position| position >= data_rows)
            {
                return Err(SnapshotDvError::Position.into());
            }
            index.vector(entry)?;
        }
    }
    summary.vectors += 1;
    summary.vector_bytes = bytes;
    summary.delete_rows = rows;
    Ok(())
}

struct Targets<'selected> {
    index: &'selected DataIndex,
    delete: SnapshotFile<'selected>,
}

#[async_trait]
impl PositionDeleteTargets for Targets<'_> {
    async fn rows(&self, location: &FileLocation) -> Result<Option<u64>, SelectedParquetError> {
        let Some(data) = self.index.files.get(location.relative_key()) else {
            return Ok(None);
        };
        if self.index.vectors.contains(location.relative_key())
            || data.entry.inherited.data_sequence > self.delete.entry.inherited.data_sequence
            || crate::manifest::deletion_vectors::partitions(self.delete, data.selected()).is_err()
        {
            return Ok(None);
        }
        Ok(Some(
            u64::try_from(data.entry.entry.record_count).map_err(|_| SelectedParquetError::Rows)?,
        ))
    }
}
