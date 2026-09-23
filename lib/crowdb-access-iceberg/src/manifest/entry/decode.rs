use crate::file::{AvroScalar, ContentFormat, FileLocation, FormatHint, TableLocation};
use crate::manifest::{EntryStatus, FileContentKind, ManifestEntry, ManifestVersion};
use std::collections::BTreeSet;

use super::{ManifestEntryError, ManifestFileFields};

pub(super) fn entry(
    values: &[AvroScalar<'_>],
    version: ManifestVersion,
    table: TableLocation,
) -> Result<(ManifestEntry, ManifestFileFields), ManifestEntryError> {
    let status = match integer(values[0])? {
        0 => EntryStatus::Existing,
        1 => EntryStatus::Added,
        2 => EntryStatus::Deleted,
        _ => return Err(ManifestEntryError::Field),
    };
    let content = if version == ManifestVersion::V1 {
        FileContentKind::Data
    } else {
        match integer(values[4])? {
            0 => FileContentKind::Data,
            1 => FileContentKind::PositionDeletes,
            2 => FileContentKind::EqualityDeletes,
            _ => return Err(ManifestEntryError::Field),
        }
    };
    let file = file(values, version, table, content)?;
    let snapshot_id = optional(values[1], long)?;
    if version == ManifestVersion::V1 && (snapshot_id.is_none() || long(values[14])? < 0) {
        return Err(ManifestEntryError::Field);
    }
    let (data_sequence, file_sequence) = if version == ManifestVersion::V1 {
        (None, None)
    } else {
        (optional(values[2], long)?, optional(values[3], long)?)
    };
    let first_row_id = if version == ManifestVersion::V3 {
        optional(values[10], long)?
    } else {
        None
    };
    let entry = ManifestEntry {
        status,
        content,
        snapshot_id,
        data_sequence,
        file_sequence,
        first_row_id,
        record_count: long(values[7])?,
    };
    Ok((entry, file))
}

fn file(
    values: &[AvroScalar<'_>],
    version: ManifestVersion,
    table: TableLocation,
    content: FileContentKind,
) -> Result<ManifestFileFields, ManifestEntryError> {
    let location = parse_location(values[5], table)?;
    let format = format(values[6])?;
    let length = u64::try_from(long(values[8])?).map_err(|_| ManifestEntryError::Field)?;
    if length == 0 {
        return Err(ManifestEntryError::Field);
    }
    let sort_order_id = if content == FileContentKind::PositionDeletes {
        None
    } else {
        optional(values[9], integer)?
    };
    if sort_order_id.is_some_and(|value| value < 0) {
        return Err(ManifestEntryError::Field);
    }
    let referenced_data_file = if version == ManifestVersion::V1 {
        None
    } else {
        optional(values[11], |value| parse_location(value, table))?
    };
    if referenced_data_file.is_some() && content != FileContentKind::PositionDeletes {
        return Err(ManifestEntryError::Field);
    }
    let offset = if version == ManifestVersion::V3 {
        optional(values[12], long)?
    } else {
        None
    };
    let size = if version == ManifestVersion::V3 {
        optional(values[13], long)?
    } else {
        None
    };
    let deletion_vector = if format == ContentFormat::Puffin {
        if version != ManifestVersion::V3
            || content != FileContentKind::PositionDeletes
            || referenced_data_file.is_none()
        {
            return Err(ManifestEntryError::Field);
        }
        let offset = offset
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(ManifestEntryError::Field)?;
        let size = size
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(ManifestEntryError::Field)?;
        if offset < 4 || size < 20 || !offset.checked_add(size).is_some_and(|end| end <= length) {
            return Err(ManifestEntryError::Field);
        }
        Some(FormatHint { offset, length: size })
    } else {
        if offset.is_some() || size.is_some() {
            return Err(ManifestEntryError::Field);
        }
        None
    };
    let equality_ids = match (content, values[15]) {
        (FileContentKind::EqualityDeletes, AvroScalar::IntList(list)) => {
            let ids = list.values(4096)?;
            let mut unique = BTreeSet::new();
            if ids.is_empty() || ids.iter().any(|id| *id <= 0 || !unique.insert(*id)) {
                return Err(ManifestEntryError::Field);
            }
            Some(ids)
        }
        (FileContentKind::EqualityDeletes, _) => return Err(ManifestEntryError::Field),
        (_, AvroScalar::Null) => None,
        _ => return Err(ManifestEntryError::Field),
    };
    Ok(ManifestFileFields {
        location,
        format,
        length,
        sort_order_id,
        referenced_data_file,
        deletion_vector,
        equality_ids,
        metrics: super::metrics::decode(&values[16..22])?,
    })
}

fn parse_location(value: AvroScalar<'_>, table: TableLocation) -> Result<FileLocation, ManifestEntryError> {
    let AvroScalar::String(path) = value else {
        return Err(ManifestEntryError::Field);
    };
    let location = path
        .parse::<FileLocation>()
        .map_err(|_| ManifestEntryError::Field)?;
    if location.table() != table {
        return Err(ManifestEntryError::Field);
    }
    Ok(location)
}

fn format(value: AvroScalar<'_>) -> Result<ContentFormat, ManifestEntryError> {
    let AvroScalar::String(format) = value else {
        return Err(ManifestEntryError::Field);
    };
    if format.eq_ignore_ascii_case("parquet") {
        Ok(ContentFormat::Parquet)
    } else if format.eq_ignore_ascii_case("orc") {
        Ok(ContentFormat::Orc)
    } else if format.eq_ignore_ascii_case("avro") {
        Ok(ContentFormat::Avro)
    } else if format.eq_ignore_ascii_case("puffin") {
        Ok(ContentFormat::Puffin)
    } else {
        Err(ManifestEntryError::Field)
    }
}

fn integer(value: AvroScalar<'_>) -> Result<i32, ManifestEntryError> {
    if let AvroScalar::Int(value) = value {
        Ok(value)
    } else {
        Err(ManifestEntryError::Field)
    }
}

fn long(value: AvroScalar<'_>) -> Result<i64, ManifestEntryError> {
    if let AvroScalar::Long(value) = value {
        Ok(value)
    } else {
        Err(ManifestEntryError::Field)
    }
}

fn optional<Value>(
    value: AvroScalar<'_>,
    read: impl FnOnce(AvroScalar<'_>) -> Result<Value, ManifestEntryError>,
) -> Result<Option<Value>, ManifestEntryError> {
    if value == AvroScalar::Null {
        Ok(None)
    } else {
        read(value).map(Some)
    }
}
