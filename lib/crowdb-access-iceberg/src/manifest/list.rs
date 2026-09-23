use crate::file::{
    AvroContainerError, AvroDatumLimits, AvroProjectedRecords, AvroProjection, AvroScalar, AvroScalarType,
    AvroSchema, FileLocation, TableLocation,
};

use super::{ManifestContent, ManifestVersion};

const FIELDS: [i32; 14] = [
    500, 501, 502, 503, 517, 515, 516, 504, 505, 506, 512, 513, 514, 520,
];

#[derive(Debug, thiserror::Error)]
pub enum ManifestListError {
    #[error(transparent)]
    Avro(#[from] AvroContainerError),
    #[error("invalid manifest list field type, value or table location")]
    Field,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ManifestListEntry {
    pub location: FileLocation,
    pub length: u64,
    pub partition_spec_id: i32,
    pub added_snapshot_id: i64,
    pub content: ManifestContent,
    pub sequence: i64,
    pub min_sequence: i64,
    pub file_counts: [Option<i32>; 3],
    pub row_counts: [Option<i64>; 3],
    pub first_row_id: Option<i64>,
}

pub struct ManifestListProjection<'schema> {
    projection: AvroProjection<'schema>,
    version: ManifestVersion,
    table: TableLocation,
}

pub struct ManifestListRecords<'projection, 'schema, 'data> {
    records: AvroProjectedRecords<'projection, 'schema, 'data>,
    version: ManifestVersion,
    table: TableLocation,
    failed: bool,
}

impl<'schema> ManifestListProjection<'schema> {
    /// The version describes this manifest list, not the current table or contained manifests.
    /// # Errors
    /// Rejects missing required fields, malformed IDs and unsupported selected writer layouts.
    pub fn new(
        schema: &'schema AvroSchema,
        version: ManifestVersion,
        table: TableLocation,
    ) -> Result<Self, ManifestListError> {
        use AvroScalarType::{Int, Long, String};

        let required = if version == ManifestVersion::V1 { 4 } else { 13 };
        let projection = AvroProjection::with_optional(schema, &FIELDS[..required], &FIELDS[required..])?;
        let expected = [
            String, Long, Int, Long, Int, Long, Long, Int, Int, Int, Long, Long, Long, Long,
        ];
        if projection
            .field_types()
            .iter()
            .zip(expected)
            .any(|(actual, expected)| actual.is_some_and(|actual| actual != expected))
        {
            return Err(ManifestListError::Field);
        }
        Ok(Self {
            projection,
            version,
            table,
        })
    }

    /// Opens one decoded Avro block without retaining a manifest entry vector.
    /// # Errors
    /// Rejects invalid block bounds or record counts.
    pub fn records<'projection, 'data>(
        &'projection self,
        bytes: &'data [u8],
        count: u64,
        limits: AvroDatumLimits,
    ) -> Result<ManifestListRecords<'projection, 'schema, 'data>, ManifestListError> {
        Ok(ManifestListRecords {
            records: self.projection.records(bytes, count, limits)?,
            version: self.version,
            table: self.table,
            failed: false,
        })
    }
}

impl ManifestListRecords<'_, '_, '_> {
    /// Checks primitive semantics and binds each manifest location to the expected native table.
    /// # Errors
    /// Permanently stops on bad fields, negative counts, invalid sequences or foreign locations.
    pub fn next_entry(&mut self) -> Result<Option<ManifestListEntry>, ManifestListError> {
        if self.failed {
            return Err(AvroContainerError::Failed.into());
        }
        self.failed = true;
        let result = self
            .records
            .next_record()?
            .map(|values| decode(&values, self.version, self.table))
            .transpose()?;
        self.failed = false;
        Ok(result)
    }
}

fn decode(
    values: &[AvroScalar<'_>],
    version: ManifestVersion,
    table: TableLocation,
) -> Result<ManifestListEntry, ManifestListError> {
    let AvroScalar::String(path) = values[0] else {
        return Err(ManifestListError::Field);
    };
    let location = path
        .parse::<FileLocation>()
        .map_err(|_| ManifestListError::Field)?;
    let length = long(values[1])?;
    let partition_spec_id = integer(values[2])?;
    if location.table() != table || length <= 0 || partition_spec_id < 0 {
        return Err(ManifestListError::Field);
    }
    let (content, sequence, min_sequence) = if version == ManifestVersion::V1 {
        (ManifestContent::Data, 0, 0)
    } else {
        let content = match integer(values[4])? {
            0 => ManifestContent::Data,
            1 => ManifestContent::Deletes,
            _ => return Err(ManifestListError::Field),
        };
        (content, long(values[5])?, long(values[6])?)
    };
    if sequence < 0 || min_sequence < 0 || min_sequence > sequence {
        return Err(ManifestListError::Field);
    }
    let required = version != ManifestVersion::V1;
    let mut file_counts = [None; 3];
    let mut row_counts = [None; 3];
    for index in 0..3 {
        file_counts[index] = optional(values[7 + index], required, integer)?;
        row_counts[index] = optional(values[10 + index], required, long)?;
        if file_counts[index].is_some_and(|count| count < 0)
            || row_counts[index].is_some_and(|count| count < 0)
        {
            return Err(ManifestListError::Field);
        }
    }
    let first_row_id = if version == ManifestVersion::V3 {
        optional(values[13], false, long)?
    } else {
        None
    };
    if first_row_id.is_some_and(|value| value < 0)
        || (content == ManifestContent::Deletes && first_row_id.is_some())
    {
        return Err(ManifestListError::Field);
    }
    Ok(ManifestListEntry {
        location,
        length: u64::try_from(length).map_err(|_| ManifestListError::Field)?,
        partition_spec_id,
        added_snapshot_id: long(values[3])?,
        content,
        sequence,
        min_sequence,
        file_counts,
        row_counts,
        first_row_id,
    })
}

fn integer(value: AvroScalar<'_>) -> Result<i32, ManifestListError> {
    if let AvroScalar::Int(value) = value {
        Ok(value)
    } else {
        Err(ManifestListError::Field)
    }
}

fn long(value: AvroScalar<'_>) -> Result<i64, ManifestListError> {
    if let AvroScalar::Long(value) = value {
        Ok(value)
    } else {
        Err(ManifestListError::Field)
    }
}

fn optional<Value>(
    value: AvroScalar<'_>,
    required: bool,
    read: impl FnOnce(AvroScalar<'_>) -> Result<Value, ManifestListError>,
) -> Result<Option<Value>, ManifestListError> {
    if value == AvroScalar::Null && !required {
        Ok(None)
    } else {
        read(value).map(Some)
    }
}
