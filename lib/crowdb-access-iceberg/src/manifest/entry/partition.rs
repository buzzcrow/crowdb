use super::ManifestEntryError as Error;
use crate::file::{AvroDatumLimits, AvroScalar, AvroSchema, AvroTuple, AvroTupleField};
use crate::manifest::{ManifestContext, PartitionTransform, PrimitiveType};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitionValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    String(String),
    Bytes(Vec<u8>),
    Opaque(Vec<u8>),
}

pub(super) struct PartitionProjection<'schema> {
    tuple: AvroTuple<'schema>,
}

impl<'schema> PartitionProjection<'schema> {
    pub(super) fn new(schema: &'schema AvroSchema, context: &ManifestContext) -> Result<Self, Error> {
        let tuple = AvroTuple::new(schema, &[2, 102])?;
        if tuple.fields().len() != context.partitions().len() {
            return Err(Error::Field);
        }
        for field in context.partitions() {
            let writer = tuple
                .fields()
                .iter()
                .find(|writer| writer.id == field.id)
                .ok_or(Error::Field)?;
            if let Some(result) = &field.result {
                if !(compatible(result, writer)
                    || field.transform == PartitionTransform::Void && writer.physical == "int"
                    || field.transform == PartitionTransform::Day
                        && writer.physical == "int"
                        && writer
                            .annotation
                            .as_ref()
                            .is_some_and(|value| value["logicalType"] == "date"))
                {
                    return Err(Error::Field);
                }
            }
        }
        Ok(Self { tuple })
    }

    pub(super) fn read(
        &self,
        bytes: &[u8],
        limits: AvroDatumLimits,
        context: &ManifestContext,
    ) -> Result<Vec<(i32, PartitionValue)>, Error> {
        let values = self.tuple.read(bytes, limits)?;
        let mut result = Vec::new();
        let mut remaining = 1024 * 1024_usize;
        for field in context.partitions() {
            let index = self
                .tuple
                .fields()
                .iter()
                .position(|writer| writer.id == field.id)
                .ok_or(Error::Field)?;
            let value = values[index];
            if field.transform == PartitionTransform::Void && value != AvroScalar::Null {
                return Err(Error::Field);
            }
            if let PartitionTransform::Bucket(buckets) = field.transform {
                if value != AvroScalar::Null
                    && !matches!(value, AvroScalar::Int(value) if value >= 0 && value < buckets)
                {
                    return Err(Error::Field);
                }
            }
            if let PartitionTransform::Truncate(width) = field.transform {
                let valid = match value {
                    AvroScalar::Null => true,
                    AvroScalar::Int(value) => value.rem_euclid(width) == 0,
                    AvroScalar::Long(value) => value.rem_euclid(i64::from(width)) == 0,
                    AvroScalar::String(value) => {
                        value.chars().count() <= usize::try_from(width).map_err(|_| Error::Field)?
                    }
                    AvroScalar::Bytes(value) => {
                        if let Some(PrimitiveType::Decimal { precision, .. }) = field.result {
                            super::bounds::decimal(value, precision)?.rem_euclid(i128::from(width)) == 0
                        } else {
                            value.len() <= usize::try_from(width).map_err(|_| Error::Field)?
                        }
                    }
                    _ => false,
                };
                if !valid {
                    return Err(Error::Field);
                }
            }
            if matches!(field.result, Some(PrimitiveType::Time))
                && !matches!(value, AvroScalar::Null | AvroScalar::Long(0..=86_399_999_999))
            {
                return Err(Error::Field);
            }
            if let (Some(PrimitiveType::Decimal { precision, .. }), AvroScalar::Bytes(bytes)) =
                (&field.result, value)
            {
                super::bounds::decimal(bytes, *precision)?;
            }
            let size = match value {
                AvroScalar::String(value) => value.len(),
                AvroScalar::Bytes(value) | AvroScalar::Opaque(value) => value.len(),
                _ => 8,
            };
            remaining = remaining
                .checked_sub(size)
                .ok_or(crate::file::AvroContainerError::Bounds)?;
            let value = match value {
                AvroScalar::Null => PartitionValue::Null,
                AvroScalar::Boolean(value) => PartitionValue::Boolean(value),
                AvroScalar::Int(value) => PartitionValue::Int(value),
                AvroScalar::Long(value) => PartitionValue::Long(value),
                AvroScalar::Float(value) => PartitionValue::Float(value.to_bits()),
                AvroScalar::Double(value) => PartitionValue::Double(value.to_bits()),
                AvroScalar::String(value) => PartitionValue::String(value.into()),
                AvroScalar::Bytes(value) => PartitionValue::Bytes(value.into()),
                AvroScalar::Opaque(value) => PartitionValue::Opaque(value.into()),
                _ => return Err(Error::Field),
            };
            result.push((field.id, value));
        }
        Ok(result)
    }
}

fn compatible(kind: &PrimitiveType, field: &AvroTupleField) -> bool {
    let logical = field
        .annotation
        .as_ref()
        .and_then(|value| value.get("logicalType"))
        .and_then(serde_json::Value::as_str);
    let annotation = field.annotation.as_ref();
    let utc = annotation.and_then(|value| value.get("adjust-to-utc"));
    let utc_valid = utc.is_none() || utc.is_some_and(serde_json::Value::is_boolean);
    let adjusted = utc.and_then(serde_json::Value::as_bool).unwrap_or(false);
    match kind {
        PrimitiveType::Boolean => field.physical == "boolean" && logical.is_none(),
        PrimitiveType::Int => field.physical == "int" && logical.is_none(),
        PrimitiveType::Long => field.physical == "long" && logical.is_none(),
        PrimitiveType::Float => field.physical == "float" && logical.is_none(),
        PrimitiveType::Double => field.physical == "double" && logical.is_none(),
        PrimitiveType::String => field.physical == "string" && logical.is_none(),
        PrimitiveType::Binary => field.physical == "bytes" && logical.is_none(),
        PrimitiveType::Fixed(size) => field.fixed_size == Some(*size) && logical.is_none(),
        PrimitiveType::Uuid => field.fixed_size == Some(16) && logical == Some("uuid"),
        PrimitiveType::Date => field.physical == "int" && logical == Some("date"),
        PrimitiveType::Time => field.physical == "long" && logical == Some("time-micros"),
        PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz
        | PrimitiveType::TimestampNs
        | PrimitiveType::TimestamptzNs => {
            let nanos = matches!(kind, PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs);
            let zone = matches!(kind, PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs);
            field.physical == "long"
                && logical
                    == Some(if nanos {
                        "timestamp-nanos"
                    } else {
                        "timestamp-micros"
                    })
                && utc_valid
                && adjusted == zone
        }
        PrimitiveType::Decimal { precision, scale } => {
            let maximum = 10_u128.pow(*precision) - 1;
            let size = usize::try_from((129 - maximum.leading_zeros()).div_ceil(8)).unwrap_or(0);
            field.fixed_size == Some(size)
                && logical == Some("decimal")
                && annotation.and_then(|value| value["precision"].as_u64()) == Some(u64::from(*precision))
                && annotation.and_then(|value| value["scale"].as_u64()).unwrap_or(0) == u64::from(*scale)
        }
        PrimitiveType::Unknown => field.physical == "null",
        _ => false,
    }
}
