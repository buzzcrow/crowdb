use crate::file::{
    AvroContainerError, AvroDatumLimits, AvroFieldPath, AvroProjectedRecords, AvroProjection, AvroScalarType,
    AvroSchema, ContentFormat, FileLocation, FormatHint, TableLocation,
};

use super::{
    InheritedEntry, ManifestContent, ManifestEntry, ManifestInheritance, ManifestInheritanceError,
    ManifestListEntry, ManifestMetadata, ManifestVersion,
};

mod bounds;
mod decode;
mod metrics;
mod partition;
mod semantic;
pub use metrics::ManifestMetrics;
pub use partition::PartitionValue;

const PATHS: [&[i32]; 22] = [
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
    &[2, 108],
    &[2, 109],
    &[2, 110],
    &[2, 137],
    &[2, 125],
    &[2, 128],
];

#[derive(Debug, thiserror::Error)]
pub enum ManifestEntryError {
    #[error(transparent)]
    Avro(#[from] AvroContainerError),
    #[error(transparent)]
    Inheritance(#[from] ManifestInheritanceError),
    #[error("invalid manifest scalar field, descriptor or decoding context")]
    Field,
    #[error(transparent)]
    Context(#[from] super::ManifestContextError),
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
    pub metrics: ManifestMetrics,
    pub partition: Option<Vec<(i32, PartitionValue)>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ManifestScalarEntry {
    pub entry: ManifestEntry,
    pub file: ManifestFileFields,
    pub inherited: InheritedEntry,
}

#[derive(Clone)]
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
    context: Option<&'schema super::ManifestContext>,
    partition: Option<partition::PartitionProjection<'schema>>,
}

pub struct ManifestEntryRecords<'projection, 'schema, 'data, 'state> {
    records: AvroProjectedRecords<'projection, 'schema, 'data>,
    state: &'state mut ManifestEntryState,
    failed: bool,
    projection: &'projection ManifestEntryProjection<'schema>,
    limits: AvroDatumLimits,
}

impl<'schema> ManifestEntryProjection<'schema> {
    /// Compiles entry fields and bounded metrics; typed table/partition semantics require context.
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
            AvroScalarType::LongMap,
            AvroScalarType::LongMap,
            AvroScalarType::LongMap,
            AvroScalarType::LongMap,
            AvroScalarType::BytesMap,
            AvroScalarType::BytesMap,
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
        for (slot, ids) in [
            (117, 118),
            (119, 120),
            (121, 122),
            (138, 139),
            (126, 127),
            (129, 130),
        ]
        .into_iter()
        .enumerate()
        {
            if projection.field_types()[16 + slot].is_some() && projection.map_ids()[16 + slot] != Some(ids) {
                return Err(ManifestEntryError::Field);
            }
        }
        Ok(Self {
            projection,
            version,
            table,
            context: None,
            partition: None,
        })
    }

    /// Compiles partition and metric semantics against a historical table context.
    /// # Errors
    /// Rejects incompatible partition field IDs, writer layouts and logical types.
    pub fn with_context(
        schema: &'schema AvroSchema,
        version: ManifestVersion,
        table: TableLocation,
        context: &'schema super::ManifestContext,
    ) -> Result<Self, ManifestEntryError> {
        let mut projection = Self::new(schema, version, table)?;
        projection.partition = Some(partition::PartitionProjection::new(schema, context)?);
        projection.context = Some(context);
        Ok(projection)
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
            projection: self,
            limits,
        })
    }
}

impl ManifestEntryRecords<'_, '_, '_, '_> {
    #[must_use]
    pub fn last_record_length(&self) -> usize {
        self.records.last_record_bytes().len()
    }
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
            let (entry, mut file) = decode::entry(&values, self.state.version, self.state.table)?;
            if let Some(context) = self.projection.context {
                semantic::validate(context, entry.content, &file)?;
                file.partition = Some(
                    self.projection
                        .partition
                        .as_ref()
                        .ok_or(ManifestEntryError::Field)?
                        .read(self.records.last_record_bytes(), self.limits, context)?,
                );
            }
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
