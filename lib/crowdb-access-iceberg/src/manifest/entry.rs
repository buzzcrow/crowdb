use crate::file::{
    AvroContainerError, AvroDatumLimits, AvroFieldPath, AvroProjectedRecords, AvroProjection, AvroScalarType,
    AvroSchema, ContentFormat, FileLocation, FormatHint, TableLocation,
};

use super::{
    InheritedEntry, ManifestContent, ManifestEntry, ManifestInheritance, ManifestInheritanceError,
    ManifestListEntry, ManifestMetadata, ManifestVersion,
};

mod decode;

const PATHS: [&[i32]; 16] = [
    &[0],
    &[1],
    &[3],
    &[4],
    &[2, 134],
    &[2, 100],
    &[2, 101],
    &[2, 103],
    &[2, 104],
    &[2, 140],
    &[2, 142],
    &[2, 143],
    &[2, 144],
    &[2, 145],
    &[2, 105],
    &[2, 135],
];

#[derive(Debug, thiserror::Error)]
pub enum ManifestEntryError {
    #[error(transparent)]
    Avro(#[from] AvroContainerError),
    #[error(transparent)]
    Inheritance(#[from] ManifestInheritanceError),
    #[error("invalid manifest scalar field, descriptor or decoding context")]
    Field,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ManifestFileFields {
    pub location: FileLocation,
    pub format: ContentFormat,
    pub length: u64,
    pub sort_order_id: Option<i32>,
    pub referenced_data_file: Option<FileLocation>,
    pub deletion_vector: Option<FormatHint>,
    pub equality_ids: Option<Vec<i32>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ManifestScalarEntry {
    pub entry: ManifestEntry,
    pub file: ManifestFileFields,
    pub inherited: InheritedEntry,
}

pub struct ManifestEntryState {
    version: ManifestVersion,
    table: TableLocation,
    inheritance: ManifestInheritance,
}

impl ManifestEntryState {
    /// Binds manifest header properties to the containing manifest-list entry.
    /// # Errors
    /// Rejects content, partition-spec or table mismatches before reading entries.
    pub fn from_list(
        metadata: ManifestMetadata<'_>,
        list: &ManifestListEntry,
        table: TableLocation,
    ) -> Result<Self, ManifestEntryError> {
        if list.location.table() != table
            || list.content != metadata.content
            || metadata
                .partition_spec_id
                .is_some_and(|id| id != list.partition_spec_id)
        {
            return Err(ManifestEntryError::Field);
        }
        Self::new(
            metadata.version,
            table,
            metadata.content,
            list.added_snapshot_id,
            list.sequence,
            list.first_row_id,
        )
    }

    /// Keeps inheritance across decoded blocks; version belongs to the manifest writer.
    /// # Errors
    /// Rejects invalid inheritance sources or content/version combinations.
    pub fn new(
        version: ManifestVersion,
        table: TableLocation,
        content: ManifestContent,
        snapshot_id: i64,
        sequence: i64,
        first_row_id: Option<i64>,
    ) -> Result<Self, ManifestEntryError> {
        Ok(Self {
            version,
            table,
            inheritance: ManifestInheritance::new(version, content, snapshot_id, sequence, first_row_id)?,
        })
    }

    #[must_use]
    pub fn next_row_id(&self) -> Option<i64> {
        self.inheritance.next_row_id()
    }
}

pub struct ManifestEntryProjection<'schema> {
    projection: AvroProjection<'schema>,
    version: ManifestVersion,
    table: TableLocation,
}

pub struct ManifestEntryRecords<'projection, 'schema, 'data, 'state> {
    records: AvroProjectedRecords<'projection, 'schema, 'data>,
    state: &'state mut ManifestEntryState,
    failed: bool,
}

impl<'schema> ManifestEntryProjection<'schema> {
    /// Compiles scalar entry fields; partition/metrics/equality-ID semantics require separate checks.
    /// # Errors
    /// Rejects missing required scalar fields, bad IDs and incompatible writer types.
    pub fn new(
        schema: &'schema AvroSchema,
        version: ManifestVersion,
        table: TableLocation,
    ) -> Result<Self, ManifestEntryError> {
        use AvroScalarType::{Int, Long, String};

        let paths: Vec<_> = PATHS
            .iter()
            .enumerate()
            .map(|(index, ids)| AvroFieldPath {
                ids,
                required: matches!(index, 0 | 5..=8)
                    || (version == ManifestVersion::V1 && matches!(index, 1 | 14))
                    || (version != ManifestVersion::V1 && index == 4),
            })
            .collect();
        let projection = AvroProjection::paths(schema, &paths)?;
        let types = [
            Int,
            Long,
            Long,
            Long,
            Int,
            String,
            String,
            Long,
            Long,
            Int,
            Long,
            String,
            Long,
            Long,
            Long,
            AvroScalarType::IntList,
        ];
        if projection
            .field_types()
            .iter()
            .zip(types)
            .any(|(actual, expected)| actual.is_some_and(|actual| actual != expected))
        {
            return Err(ManifestEntryError::Field);
        }
        if projection.field_types()[15].is_some() && projection.element_ids()[15] != Some(136) {
            return Err(ManifestEntryError::Field);
        }
        Ok(Self {
            projection,
            version,
            table,
        })
    }

    /// Borrows shared inheritance state for one bounded decoded block.
    /// # Errors
    /// Rejects mixed writer-version/table contexts and invalid block bounds.
    pub fn records<'projection, 'data, 'state>(
        &'projection self,
        bytes: &'data [u8],
        count: u64,
        limits: AvroDatumLimits,
        state: &'state mut ManifestEntryState,
    ) -> Result<ManifestEntryRecords<'projection, 'schema, 'data, 'state>, ManifestEntryError> {
        if state.version != self.version || state.table != self.table {
            return Err(ManifestEntryError::Field);
        }
        Ok(ManifestEntryRecords {
            records: self.projection.records(bytes, count, limits)?,
            state,
            failed: false,
        })
    }
}

impl ManifestEntryRecords<'_, '_, '_, '_> {
    /// Resolves one scalar entry only after every selected and skipped binary field is valid.
    /// This is not full manifest acceptance: collection and cross-file semantics remain separate.
    /// # Errors
    /// Poisons the block cursor on error without advancing inheritance for the failed entry.
    pub fn next_entry(&mut self) -> Result<Option<ManifestScalarEntry>, ManifestEntryError> {
        if self.failed {
            return Err(AvroContainerError::Failed.into());
        }
        self.failed = true;
        let result = if let Some(values) = self.records.next_record()? {
            let (entry, file) = decode::entry(&values, self.state.version, self.state.table)?;
            let inherited = self.state.inheritance.resolve(entry)?;
            Some(ManifestScalarEntry {
                entry,
                file,
                inherited,
            })
        } else {
            None
        };
        self.failed = false;
        Ok(result)
    }
}
