use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;

use super::{
    index::DataIndex, SnapshotFileLimits, SnapshotFileSummary, SnapshotValidationError as Error,
    SnapshotValidationInput,
};
use crate::file::{
    read_deletion_vector_positions, ContentFormat, DeletionVectorPositions, DeletionVectorReference,
    FileBlockStore, FileKind, FileLocation,
};
use crate::manifest::{
    validate_parquet_position_deletes, EntryStatus, FileContentKind, ManifestScalarEntry, ParquetSelection,
    PositionDeleteTargets, SelectedParquetError, SnapshotDvError, SnapshotFile,
};

/// Validates both snapshots and checks DV replacements for surviving immutable data files.
/// The candidate must be a direct child of the supplied prior snapshot. Sources and parent
/// reachability must be established by the caller from one retained metadata generation.
/// This is not a metadata publication proof and does not validate equality-delete rewrites
/// or position-delete removal when no replacement DV is present.
/// # Errors
/// Rejects lost prior DVs, incomplete DV replacements, changed data identities and exceeded budgets.
pub async fn validate_snapshot_delete_preservation(
    store: Arc<dyn FileBlockStore>,
    prior: &SnapshotValidationInput,
    candidate: &SnapshotValidationInput,
    limits: SnapshotFileLimits,
    ranges: usize,
) -> Result<SnapshotFileSummary, Error> {
    if prior.scope.context != candidate.scope.context
        || prior.scope.table != candidate.scope.table
        || candidate.selection.parent_snapshot_id != Some(prior.scope.snapshot_id)
        || candidate.scope.snapshot_id == prior.scope.snapshot_id
        || candidate.scope.sequence <= prior.scope.sequence
    {
        return Err(Error::Binding);
    }
    if !(1..=1_000_000).contains(&ranges) {
        return Err(Error::Bounds);
    }
    let (prior_index, prior_summary) = Box::pin(super::validate(store.clone(), prior, limits)).await?;
    let (candidate_index, summary) = Box::pin(super::validate(store.clone(), candidate, limits)).await?;
    surviving_files(&prior_index, &candidate_index)?;
    if summary.vectors == 0 {
        if prior_index
            .vectors
            .iter()
            .any(|path| candidate_index.files.contains_key(path))
        {
            return Err(Error::Binding);
        }
        return Ok(summary);
    }
    let vectors = collect(store.clone(), candidate, &candidate_index, limits, ranges).await?;
    let mut reader = super::open(store.clone(), prior, limits).await?;
    while let Some(entry) = reader.next_entry().await? {
        if entry.entry.status == EntryStatus::Deleted
            || entry.entry.content != FileContentKind::PositionDeletes
        {
            continue;
        }
        let (_, context) = reader.current_manifest().ok_or(Error::Binding)?;
        if entry.file.format == ContentFormat::Puffin {
            let target = entry.file.referenced_data_file.as_ref().ok_or(Error::Binding)?;
            if !prior_index.vectors.contains(target.relative_key()) {
                continue;
            }
            preserve_vector(
                store.clone(),
                prior,
                &candidate_index,
                &vectors,
                &entry,
                limits,
                ranges,
            )
            .await?;
        } else {
            let record = prior.files.resolve(&entry.file.location).await?;
            let targets = Targets {
                prior: &prior_index,
                candidate: &candidate_index,
                vectors: &vectors,
                delete: SnapshotFile {
                    entry: &entry,
                    record: &record,
                    context,
                },
            };
            validate_parquet_position_deletes(
                store.clone(),
                &record,
                ParquetSelection {
                    entry: &entry,
                    context,
                    table: prior.scope.table,
                    mapping: prior.mapping.as_ref(),
                },
                &targets,
                limits.position_deletes,
            )
            .await?;
        }
    }
    if reader.finish()? != prior_summary.manifests {
        return Err(Error::Binding);
    }
    Ok(summary)
}

fn surviving_files(prior: &DataIndex, candidate: &DataIndex) -> Result<(), Error> {
    for (path, previous) in &prior.files {
        let Some(current) = candidate.files.get(path) else {
            continue;
        };
        if previous.record != current.record
            || previous.entry.inherited.data_sequence != current.entry.inherited.data_sequence
            || previous.entry.inherited.file_sequence != current.entry.inherited.file_sequence
            || previous.entry.entry.record_count != current.entry.entry.record_count
            || crate::manifest::deletion_vectors::partitions(previous.selected(), current.selected()).is_err()
        {
            return Err(Error::Binding);
        }
    }
    Ok(())
}

async fn collect(
    store: Arc<dyn FileBlockStore>,
    input: &SnapshotValidationInput,
    index: &DataIndex,
    limits: SnapshotFileLimits,
    mut ranges: usize,
) -> Result<BTreeMap<String, DeletionVectorPositions>, Error> {
    let mut result = BTreeMap::new();
    let mut reader = super::open(store.clone(), input, limits).await?;
    while let Some(entry) = reader.next_entry().await? {
        if entry.entry.status == EntryStatus::Deleted || entry.file.format != ContentFormat::Puffin {
            continue;
        }
        let target = entry.file.referenced_data_file.as_ref().ok_or(Error::Binding)?;
        if !index.vectors.contains(target.relative_key()) {
            return Err(Error::Binding);
        }
        if ranges == 0 && entry.entry.record_count != 0 {
            return Err(Error::Bounds);
        }
        let positions = positions(store.clone(), input, &entry, limits, ranges.max(1)).await?;
        ranges = ranges.checked_sub(positions.range_count()).ok_or(Error::Bounds)?;
        if result
            .insert(target.relative_key().to_owned(), positions)
            .is_some()
        {
            return Err(Error::Binding);
        }
    }
    reader.finish()?;
    Ok(result)
}

async fn positions(
    store: Arc<dyn FileBlockStore>,
    input: &SnapshotValidationInput,
    entry: &ManifestScalarEntry,
    limits: SnapshotFileLimits,
    ranges: usize,
) -> Result<DeletionVectorPositions, Error> {
    let record = input
        .files
        .resolve(&entry.file.location)
        .await?
        .bind_kind(FileKind::DeletionVector)
        .map_err(|_| Error::Binding)?;
    Ok(read_deletion_vector_positions(
        store,
        &record,
        &DeletionVectorReference {
            referenced: entry.file.referenced_data_file.clone().ok_or(Error::Binding)?,
            span: entry.file.deletion_vector.ok_or(Error::Binding)?,
            cardinality: u64::try_from(entry.entry.record_count).map_err(|_| Error::Binding)?,
        },
        limits.vectors.vector,
        ranges,
    )
    .await
    .map_err(SnapshotDvError::from)?)
}

async fn preserve_vector(
    store: Arc<dyn FileBlockStore>,
    prior: &SnapshotValidationInput,
    candidate: &DataIndex,
    vectors: &BTreeMap<String, DeletionVectorPositions>,
    entry: &ManifestScalarEntry,
    limits: SnapshotFileLimits,
    ranges: usize,
) -> Result<(), Error> {
    let target = entry.file.referenced_data_file.as_ref().ok_or(Error::Binding)?;
    if !candidate.files.contains_key(target.relative_key()) {
        return Ok(());
    }
    let replacement = vectors.get(target.relative_key()).ok_or(Error::Binding)?;
    let previous = positions(store, prior, entry, limits, ranges).await?;
    if !replacement.covers(&previous) {
        return Err(Error::Binding);
    }
    Ok(())
}

struct Targets<'selected> {
    prior: &'selected DataIndex,
    candidate: &'selected DataIndex,
    vectors: &'selected BTreeMap<String, DeletionVectorPositions>,
    delete: SnapshotFile<'selected>,
}

#[async_trait]
impl PositionDeleteTargets for Targets<'_> {
    async fn rows(&self, location: &FileLocation) -> Result<Option<u64>, SelectedParquetError> {
        let Some(data) = self.prior.files.get(location.relative_key()) else {
            return Ok(None);
        };
        if !self.candidate.files.contains_key(location.relative_key())
            || !self.vectors.contains_key(location.relative_key())
            || self.prior.vectors.contains(location.relative_key())
            || data.entry.inherited.data_sequence > self.delete.entry.inherited.data_sequence
            || crate::manifest::deletion_vectors::partitions(self.delete, data.selected()).is_err()
        {
            return Ok(None);
        }
        Ok(Some(
            u64::try_from(data.entry.entry.record_count).map_err(|_| SelectedParquetError::Rows)?,
        ))
    }

    async fn position(&self, location: &FileLocation, position: u64) -> Result<(), SelectedParquetError> {
        if !self
            .vectors
            .get(location.relative_key())
            .is_some_and(|vector| vector.contains(location, position))
        {
            return Err(SelectedParquetError::Delete);
        }
        Ok(())
    }
}
