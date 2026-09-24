use std::collections::{BTreeMap, BTreeSet};

use super::{primitive, SelectedParquetError as Error};
use crate::{
    file::{ParquetMetadata, ParquetMetadataError, ParquetSchemaElement},
    table::TableMetadataDocument,
};

mod inventory;
mod projection;
mod rows;
mod value;
pub use inventory::validate_partition_statistics_inventory;
pub use rows::{validate_partition_statistics_rows, PartitionStatisticsRowLimits};

/// Validates partition-statistics field IDs, requiredness and the unified partition type.
/// This validates schema only, not page values, tuple ordering or statistics counts.
/// # Errors
/// Rejects missing active fields, incompatible historical specs and exhausted work.
pub fn validate_partition_statistics_schema(
    metadata: &ParquetMetadata,
    document: &TableMetadataDocument,
    work: &mut usize,
) -> Result<(), Error> {
    validated_projection(metadata, document, work).map(|_| ())
}

fn validated_projection(
    metadata: &ParquetMetadata,
    document: &TableMetadataDocument,
    work: &mut usize,
) -> Result<BTreeMap<i32, projection::PartitionField>, Error> {
    if *work == 0 || *work > 1_000_000 {
        return Err(ParquetMetadataError::Bounds.into());
    }
    let fields = projection::project(document, work)?;
    let schema = &metadata.schema;
    let root = schema.first().ok_or(Error::Schema)?;
    if root.physical_type.is_some()
        || primitive::annotation(root)?.is_some()
        || root.repetition.is_some_and(|value| value != 0)
    {
        return Err(Error::Schema);
    }
    let mut seen = BTreeSet::new();
    let mut index = 1;
    let version = document.selected_head().format_version;
    for _ in 0..root.children {
        charge(work, 1)?;
        let field = schema.get(index).ok_or(Error::Schema)?;
        let id = field.field_id.ok_or(Error::Schema)?;
        if !seen.insert(id) {
            return Err(Error::Schema);
        }
        index += 1;
        if id == 1 {
            if field.physical_type.is_some()
                || field.repetition != Some(0)
                || primitive::annotation(field)?.is_some()
            {
                return Err(Error::Schema);
            }
            let end = index.checked_add(field.children).ok_or(Error::Schema)?;
            partition(schema.get(index..end).ok_or(Error::Schema)?, &fields, work)?;
            index = end;
        } else {
            statistic(field, id, version)?;
        }
    }
    if index != schema.len()
        || (1..=5).any(|id| !seen.contains(&id))
        || (version == 3 && [6, 7, 8, 9, 13].iter().any(|id| !seen.contains(id)))
    {
        return Err(Error::Schema);
    }
    Ok(fields)
}

fn partition(
    fields: &[ParquetSchemaElement],
    expected: &std::collections::BTreeMap<i32, projection::PartitionField>,
    work: &mut usize,
) -> Result<(), Error> {
    let mut previous = None;
    let mut present = BTreeSet::new();
    for field in fields {
        charge(work, 1)?;
        let id = field.field_id.ok_or(Error::Schema)?;
        if previous.is_some_and(|previous| id <= previous)
            || field.children != 0
            || field.repetition != Some(1)
        {
            return Err(Error::Schema);
        }
        let expected = expected.get(&id).ok_or(Error::Schema)?;
        primitive::validate(field, expected.result.as_ref().ok_or(Error::Unsupported)?)?;
        previous = Some(id);
        present.insert(id);
    }
    for (id, field) in expected {
        charge(work, 1)?;
        if !field.may_omit && !present.contains(id) {
            return Err(Error::Schema);
        }
    }
    Ok(())
}

fn statistic(field: &ParquetSchemaElement, id: i32, version: u8) -> Result<(), Error> {
    let physical = match id {
        2 | 4 | 7 | 9 | 13 => 1,
        3 | 5 | 6 | 8 | 10..=12 => 2,
        _ => return Err(Error::Schema),
    };
    let required = id <= 5 || (version == 3 && matches!(id, 6..=9 | 13));
    if (id == 13 && version < 3)
        || field.children != 0
        || field.physical_type != Some(physical)
        || !matches!(field.repetition, Some(0 | 1))
        || (required && field.repetition != Some(0))
        || primitive::annotation(field)?.is_some()
    {
        return Err(Error::Schema);
    }
    Ok(())
}

fn charge(work: &mut usize, amount: usize) -> Result<(), Error> {
    *work = work.checked_sub(amount).ok_or(ParquetMetadataError::Bounds)?;
    Ok(())
}
