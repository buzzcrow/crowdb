use super::{SnapshotDvError as Error, SnapshotDvScope, SnapshotFile};
use crate::file::{ContentFormat, DeletionVectorReference, FileKind};
use crate::manifest::{entry::bounds, EntryStatus, FileContentKind, PartitionValue, PrimitiveType};

pub(super) fn reference(
    scope: SnapshotDvScope,
    vector: SnapshotFile<'_>,
    data: SnapshotFile<'_>,
) -> Result<DeletionVectorReference, Error> {
    validate_file(scope, vector)?;
    validate_file(scope, data)?;
    if vector.entry.entry.content != FileContentKind::PositionDeletes
        || vector.record.kind != FileKind::DeletionVector
        || vector.record.format != ContentFormat::Puffin
        || data.entry.entry.content != FileContentKind::Data
        || data.record.kind != FileKind::Data
        || vector.entry.file.referenced_data_file.as_ref() != Some(&data.record.location)
        || vector.entry.inherited.data_sequence < data.entry.inherited.data_sequence
        || data.entry.file.referenced_data_file.is_some()
        || data.entry.file.deletion_vector.is_some()
        || vector.entry.file.equality_ids.is_some()
        || data.entry.file.equality_ids.is_some()
        || vector.entry.inherited.first_row_id.is_some()
    {
        return Err(Error::Binding);
    }
    partitions(vector, data)?;
    Ok(DeletionVectorReference {
        referenced: data.record.location.clone(),
        span: vector.entry.file.deletion_vector.ok_or(Error::Binding)?,
        cardinality: u64::try_from(vector.entry.entry.record_count).map_err(|_| Error::Binding)?,
    })
}

fn validate_file(scope: SnapshotDvScope, file: SnapshotFile<'_>) -> Result<(), Error> {
    let raw = &file.entry.entry;
    let inherited = &file.entry.inherited;
    if file.record.validate().is_err()
        || file.record.location.table() != scope.table
        || file.entry.file.location != file.record.location
        || file.entry.file.length != file.record.length
        || file.entry.file.format != file.record.format
        || raw.status == EntryStatus::Deleted
        || raw.record_count < 0
        || inherited.data_sequence < 0
        || inherited.file_sequence < 0
        || inherited.data_sequence > scope.sequence
        || inherited.file_sequence > scope.sequence
        || raw
            .data_sequence
            .is_some_and(|value| value != inherited.data_sequence)
        || raw
            .file_sequence
            .is_some_and(|value| value != inherited.file_sequence)
        || raw
            .snapshot_id
            .is_some_and(|value| value != inherited.snapshot_id)
        || raw
            .first_row_id
            .is_some_and(|value| Some(value) != inherited.first_row_id)
    {
        return Err(Error::Binding);
    }
    Ok(())
}

fn partitions(vector: SnapshotFile<'_>, data: SnapshotFile<'_>) -> Result<(), Error> {
    let left = vector.entry.file.partition.as_ref().ok_or(Error::Binding)?;
    let right = data.entry.file.partition.as_ref().ok_or(Error::Binding)?;
    let left_spec = vector.context.partitions();
    let right_spec = data.context.partitions();
    if vector.context.spec_id() != data.context.spec_id()
        || left.len() != right.len()
        || left.len() != left_spec.len()
        || right.len() != right_spec.len()
    {
        return Err(Error::Binding);
    }
    for (((left_id, lower), left_field), ((right_id, upper), right_field)) in
        left.iter().zip(left_spec).zip(right.iter().zip(right_spec))
    {
        if left_id != right_id
            || *left_id != left_field.id
            || *right_id != right_field.id
            || left_field.sources != right_field.sources
            || left_field.transform != right_field.transform
            || !equal(
                lower,
                upper,
                left_field.result.as_ref(),
                right_field.result.as_ref(),
            )?
        {
            return Err(Error::Binding);
        }
    }
    Ok(())
}

fn equal(
    left: &PartitionValue,
    right: &PartitionValue,
    left_type: Option<&PrimitiveType>,
    right_type: Option<&PrimitiveType>,
) -> Result<bool, Error> {
    if let (
        Some(PrimitiveType::Decimal { precision, scale }),
        Some(PrimitiveType::Decimal {
            precision: other_precision,
            scale: other_scale,
        }),
    ) = (left_type, right_type)
    {
        if scale != other_scale {
            return Ok(false);
        }
        if let (PartitionValue::Bytes(left), PartitionValue::Bytes(right)) = (left, right) {
            return Ok(bounds::decimal(left, *precision).map_err(|_| Error::Binding)?
                == bounds::decimal(right, *other_precision).map_err(|_| Error::Binding)?);
        }
    } else if left_type != right_type
        && !matches!(
            (left_type, right_type),
            (Some(PrimitiveType::Int), Some(PrimitiveType::Long))
                | (Some(PrimitiveType::Long), Some(PrimitiveType::Int))
                | (Some(PrimitiveType::Float), Some(PrimitiveType::Double))
                | (Some(PrimitiveType::Double), Some(PrimitiveType::Float))
        )
    {
        return Ok(false);
    }
    Ok(match (left, right) {
        (PartitionValue::Int(left), PartitionValue::Long(right)) => i64::from(*left) == *right,
        (PartitionValue::Long(left), PartitionValue::Int(right)) => *left == i64::from(*right),
        (PartitionValue::Float(left), PartitionValue::Float(right)) => {
            normalized(f64::from(f32::from_bits(*left))) == normalized(f64::from(f32::from_bits(*right)))
        }
        (PartitionValue::Double(left), PartitionValue::Double(right)) => {
            normalized(f64::from_bits(*left)) == normalized(f64::from_bits(*right))
        }
        (PartitionValue::Float(left), PartitionValue::Double(right)) => {
            normalized(f64::from(f32::from_bits(*left))) == normalized(f64::from_bits(*right))
        }
        (PartitionValue::Double(left), PartitionValue::Float(right)) => {
            normalized(f64::from_bits(*left)) == normalized(f64::from(f32::from_bits(*right)))
        }
        _ => left == right,
    })
}

fn normalized(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}
