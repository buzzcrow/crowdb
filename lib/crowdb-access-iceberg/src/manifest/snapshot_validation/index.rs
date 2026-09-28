use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use super::{
    SnapshotFileLimits, SnapshotFileSummary, SnapshotValidationError as Error, SnapshotValidationInput,
};
use crate::file::{ContentFormat, FileBlockStore, FileKind, FileLocation, FileRecord};
use crate::manifest::{
    read_parquet_selection, ManifestContext, ManifestMetrics, ManifestScalarEntry, ParquetSelection,
    PartitionValue, SnapshotFile,
};

pub(super) struct DataFile {
    pub entry: ManifestScalarEntry,
    pub record: FileRecord,
    pub context: Arc<ManifestContext>,
}

impl DataFile {
    pub fn selected(&self) -> SnapshotFile<'_> {
        SnapshotFile {
            entry: &self.entry,
            record: &self.record,
            context: &self.context,
        }
    }
}

pub(super) struct DataIndex {
    pub files: BTreeMap<String, DataFile>,
    pub vectors: BTreeSet<String>,
    limits: SnapshotFileLimits,
    bytes: usize,
    context: Option<(FileLocation, Arc<ManifestContext>)>,
}

impl DataIndex {
    pub fn new(limits: SnapshotFileLimits) -> Self {
        Self {
            files: BTreeMap::new(),
            vectors: BTreeSet::new(),
            limits,
            bytes: 0,
            context: None,
        }
    }

    pub fn vector_count(&self) -> u64 {
        self.vectors.len() as u64
    }

    pub fn vector(&mut self, entry: &ManifestScalarEntry) -> Result<(), Error> {
        let target = entry.file.referenced_data_file.as_ref().ok_or(Error::Binding)?;
        if self.vector_count() >= self.limits.vectors.vectors {
            return Err(Error::Bounds);
        }
        self.reserve(std::mem::size_of::<FileLocation>() + target.relative_key().len() + 128)?;
        if !self.vectors.insert(target.relative_key().to_owned()) {
            return Err(Error::Binding);
        }
        Ok(())
    }

    pub async fn data(
        &mut self,
        store: Arc<dyn FileBlockStore>,
        input: &SnapshotValidationInput,
        mut entry: ManifestScalarEntry,
        manifest: &FileLocation,
        context: &ManifestContext,
        summary: &mut SnapshotFileSummary,
    ) -> Result<(), Error> {
        if entry.file.format != ContentFormat::Parquet {
            return Err(Error::Unsupported);
        }
        if self.files.len() >= self.limits.data_files {
            return Err(Error::Bounds);
        }
        self.reserve(entry_bytes(&entry))?;
        if self.context.as_ref().map(|(location, _)| location) != Some(manifest) {
            self.reserve(context.retained_bytes() + manifest.relative_key().len() + 128)?;
            self.context = Some((manifest.clone(), Arc::new(context.clone())));
        }
        let record = input.files.resolve(&entry.file.location).await?;
        let selection = ParquetSelection {
            entry: &entry,
            context,
            table: input.scope.table,
            mapping: input.mapping.as_ref(),
        };
        let (metadata, _) =
            read_parquet_selection(store, &record, &selection, self.limits.position_deletes.metadata).await?;
        summary.data_rows = summary
            .data_rows
            .checked_add(metadata.rows)
            .ok_or(Error::Bounds)?;
        summary.data_files += 1;
        entry.file.metrics = ManifestMetrics::default();
        let record = record.bind_kind(FileKind::Data).map_err(|_| Error::Binding)?;
        let context = self.context.as_ref().ok_or(Error::Binding)?.1.clone();
        if self
            .files
            .insert(
                entry.file.location.relative_key().to_owned(),
                DataFile {
                    entry,
                    record,
                    context,
                },
            )
            .is_some()
        {
            return Err(Error::Binding);
        }
        Ok(())
    }

    fn reserve(&mut self, bytes: usize) -> Result<(), Error> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.limits.index_bytes)
            .ok_or(Error::Bounds)?;
        Ok(())
    }
}

fn entry_bytes(entry: &ManifestScalarEntry) -> usize {
    let partition = entry.file.partition.as_ref().map_or(0, |values| {
        values.capacity() * std::mem::size_of::<(i32, PartitionValue)>()
            + values
                .iter()
                .map(|(_, value)| match value {
                    PartitionValue::String(value) => value.capacity(),
                    PartitionValue::Bytes(value) | PartitionValue::Opaque(value) => value.capacity(),
                    _ => 0,
                })
                .sum::<usize>()
    });
    std::mem::size_of::<DataFile>()
        + std::mem::size_of::<FileLocation>()
        + 256
        + entry.file.location.relative_key().len() * 3
        + partition
}
