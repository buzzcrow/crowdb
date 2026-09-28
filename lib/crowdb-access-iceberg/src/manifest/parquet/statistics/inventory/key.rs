use std::collections::BTreeMap;

use super::{charge, projection, value::Value, Error};
use crate::{
    file::ParquetMetadataError,
    manifest::{ManifestContext, ManifestScalarEntry, PartitionValue, PrimitiveType},
};

pub(super) fn row(spec: i32, tuple: &BTreeMap<i32, Value>, work: &mut usize) -> Result<Vec<u8>, Error> {
    let mut key = spec.to_le_bytes().to_vec();
    for (id, value) in tuple {
        append(&mut key, *id, value, work)?;
    }
    Ok(key)
}

pub(super) fn manifest(
    entry: &ManifestScalarEntry,
    context: &ManifestContext,
    fields: &BTreeMap<i32, projection::PartitionField>,
    present: &[i32],
    work: &mut usize,
) -> Result<Vec<u8>, Error> {
    let mut key = context.spec_id().to_le_bytes().to_vec();
    let tuple = entry.file.partition.as_ref().ok_or(Error::Schema)?;
    for id in present {
        charge(work, tuple.len() + context.partitions().len() + 1)?;
        let value = if let Some((_, value)) = tuple.iter().find(|(field, _)| field == id) {
            let writer = context
                .partitions()
                .iter()
                .find(|field| field.id == *id)
                .and_then(|field| field.result.as_ref())
                .ok_or(Error::Schema)?;
            let target = fields
                .get(id)
                .and_then(|field| field.result.as_ref())
                .ok_or(Error::Schema)?;
            let bytes = match value {
                PartitionValue::String(value) => value.len(),
                PartitionValue::Bytes(value) | PartitionValue::Opaque(value) => value.len(),
                _ => 16,
            };
            charge(work, bytes)?;
            normalize(value, writer, target)?
        } else {
            Value::Null
        };
        append(&mut key, *id, &value, work)?;
    }
    Ok(key)
}

fn append(key: &mut Vec<u8>, id: i32, value: &Value, work: &mut usize) -> Result<(), Error> {
    charge(work, value.bytes() + 16)?;
    key.extend(id.to_le_bytes());
    match value {
        Value::Null => key.push(0),
        Value::Integer(value) => {
            key.push(1);
            key.extend(value.to_le_bytes());
        }
        Value::Real(value) => {
            key.push(2);
            key.extend(value.to_bits().to_le_bytes());
        }
        Value::Bytes(bytes) => {
            key.push(3);
            key.extend(
                u64::try_from(bytes.len())
                    .map_err(|_| ParquetMetadataError::Bounds)?
                    .to_le_bytes(),
            );
            key.extend(bytes);
        }
        Value::Uuid(high, low) => {
            key.push(4);
            key.extend(high.to_le_bytes());
            key.extend(low.to_le_bytes());
        }
    }
    Ok(())
}

fn normalize(value: &PartitionValue, writer: &PrimitiveType, target: &PrimitiveType) -> Result<Value, Error> {
    use PartitionValue as Raw;
    use PrimitiveType as Kind;
    Ok(match (target, value) {
        (_, Raw::Null) => Value::Null,
        (Kind::Boolean, Raw::Boolean(value)) => Value::Integer(i128::from(*value)),
        (Kind::Int | Kind::Long | Kind::Date, Raw::Int(value)) => Value::Integer(i128::from(*value)),
        (Kind::Long, Raw::Long(value)) => Value::Integer(i128::from(*value)),
        (Kind::Float | Kind::Double, Raw::Float(value)) => real(f64::from(f32::from_bits(*value))),
        (Kind::Double, Raw::Double(value)) => real(f64::from_bits(*value)),
        (Kind::String, Raw::String(value)) => Value::Bytes(value.as_bytes().to_vec()),
        (Kind::Binary | Kind::Fixed(_), Raw::Bytes(value)) => Value::Bytes(value.clone()),
        (Kind::Uuid, Raw::Bytes(value)) if value.len() == 16 => Value::Uuid(
            i64::from_be_bytes(value[..8].try_into().map_err(|_| Error::Schema)?),
            i64::from_be_bytes(value[8..].try_into().map_err(|_| Error::Schema)?),
        ),
        (Kind::Decimal { precision, .. }, Raw::Bytes(value)) => Value::Integer(
            crate::manifest::entry::bounds::decimal(value, *precision).map_err(|_| Error::Schema)?,
        ),
        (
            Kind::Time | Kind::Timestamp | Kind::Timestamptz | Kind::TimestampNs | Kind::TimestamptzNs,
            Raw::Long(value),
        ) => Value::Integer(
            i128::from(*value)
                * match writer {
                    Kind::Time | Kind::Timestamp | Kind::Timestamptz => 1000,
                    Kind::TimestampNs | Kind::TimestamptzNs => 1,
                    _ => return Err(Error::Schema),
                },
        ),
        (Kind::Timestamp | Kind::TimestampNs, Raw::Int(value)) if *writer == Kind::Date => {
            Value::Integer(i128::from(*value) * 86_400_000_000_000)
        }
        _ => return Err(Error::Unsupported),
    })
}

fn real(value: f64) -> Value {
    Value::Real(if value.is_nan() { f64::NAN } else { value })
}
