use std::cmp::Ordering;

use super::{primitive, Error};
use crate::{
    file::{
        ParquetColumnValue as Physical, ParquetLogicalType as Logical, ParquetSchemaElement, ParquetTimeUnit,
    },
    manifest::{PartitionTransform, PrimitiveType},
};

#[derive(Debug)]
pub(super) enum Value {
    Null,
    Integer(i128),
    Real(f64),
    Bytes(Vec<u8>),
    Uuid(i64, i64),
}

impl Value {
    pub(super) fn compare(&self, other: &Self) -> Result<Ordering, Error> {
        Ok(match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Null, _) => Ordering::Less,
            (_, Self::Null) => Ordering::Greater,
            (Self::Integer(left), Self::Integer(right)) => left.cmp(right),
            (Self::Real(left), Self::Real(right)) => left.total_cmp(right),
            (Self::Bytes(left), Self::Bytes(right)) => left.cmp(right),
            (Self::Uuid(left_high, left_low), Self::Uuid(right_high, right_low)) => {
                (left_high, left_low).cmp(&(right_high, right_low))
            }
            _ => return Err(Error::Schema),
        })
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Bytes(bytes) => bytes.len(),
                _ => 0,
            }
    }
}

pub(super) fn decode(
    value: Physical,
    field: &ParquetSchemaElement,
    kind: &PrimitiveType,
) -> Result<Value, Error> {
    primitive::validate(field, kind)?;
    if matches!(value, Physical::Null) {
        return Ok(Value::Null);
    }
    match (kind, value) {
        (PrimitiveType::Boolean, Physical::Boolean(value)) => Ok(Value::Integer(i128::from(value))),
        (PrimitiveType::Float | PrimitiveType::Double, Physical::Float(bits)) => {
            Ok(real(f64::from(f32::from_bits(bits))))
        }
        (PrimitiveType::Double, Physical::Double(bits)) => Ok(real(f64::from_bits(bits))),
        (PrimitiveType::Decimal { .. }, value) => decimal(value, field),
        (PrimitiveType::Int | PrimitiveType::Long | PrimitiveType::Date, Physical::Long(value)) => {
            integer(value, field)
        }
        (
            PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs,
            Physical::Long(value),
        ) => temporal(value, field, kind),
        (PrimitiveType::String, Physical::Bytes(bytes)) => {
            std::str::from_utf8(&bytes).map_err(|_| Error::Schema)?;
            Ok(Value::Bytes(bytes))
        }
        (PrimitiveType::Binary | PrimitiveType::Fixed(_), Physical::Bytes(bytes)) => Ok(Value::Bytes(bytes)),
        (PrimitiveType::Uuid, Physical::Bytes(bytes)) if bytes.len() == 16 => Ok(Value::Uuid(
            i64::from_be_bytes(bytes[..8].try_into().map_err(|_| Error::Schema)?),
            i64::from_be_bytes(bytes[8..].try_into().map_err(|_| Error::Schema)?),
        )),
        _ => Err(Error::Unsupported),
    }
}

fn real(value: f64) -> Value {
    Value::Real(if value.is_nan() { f64::NAN } else { value })
}

fn integer(value: i64, field: &ParquetSchemaElement) -> Result<Value, Error> {
    let mut result = i128::from(value);
    if let Some(Logical::Integer { bit_width, signed }) = primitive::annotation(field)? {
        if !signed && bit_width == 32 {
            result = i128::from(u32::from_ne_bytes(
                i32::try_from(value).map_err(|_| Error::Schema)?.to_ne_bytes(),
            ));
        }
        let width = u32::try_from(bit_width).map_err(|_| Error::Schema)?;
        let valid = if signed {
            -(1_i128 << (width - 1)) <= result && result < (1_i128 << (width - 1))
        } else {
            0 <= result && result < (1_i128 << width)
        };
        if !valid {
            return Err(Error::Schema);
        }
    }
    Ok(Value::Integer(result))
}

fn decimal(value: Physical, field: &ParquetSchemaElement) -> Result<Value, Error> {
    let Some(Logical::Decimal { precision, .. }) = primitive::annotation(field)? else {
        return Err(Error::Schema);
    };
    let value = match value {
        Physical::Long(value) => i128::from(value),
        Physical::Bytes(bytes) => {
            let mut bytes = bytes.as_slice();
            while bytes.len() > 16 {
                if !((bytes[0] == 0 && bytes[1] & 128 == 0) || (bytes[0] == 255 && bytes[1] & 128 != 0)) {
                    return Err(Error::Schema);
                }
                bytes = &bytes[1..];
            }
            if bytes.is_empty() {
                return Err(Error::Schema);
            }
            let mut padded = [if bytes[0] & 128 == 0 { 0 } else { 255 }; 16];
            padded[16 - bytes.len()..].copy_from_slice(bytes);
            i128::from_be_bytes(padded)
        }
        _ => return Err(Error::Schema),
    };
    let precision = u32::try_from(precision)
        .ok()
        .filter(|precision| (1..=38).contains(precision))
        .ok_or(Error::Schema)?;
    if value.unsigned_abs() >= 10_u128.pow(precision) {
        return Err(Error::Schema);
    }
    Ok(Value::Integer(value))
}

fn temporal(value: i64, field: &ParquetSchemaElement, kind: &PrimitiveType) -> Result<Value, Error> {
    let Some(Logical::Time { unit, .. } | Logical::Timestamp { unit, .. }) = primitive::annotation(field)?
    else {
        return Err(Error::Schema);
    };
    let nanos = i128::from(value)
        * match unit {
            ParquetTimeUnit::Millis => 1_000_000,
            ParquetTimeUnit::Micros => 1000,
            ParquetTimeUnit::Nanos => 1,
        };
    if *kind == PrimitiveType::Time && !(0..86_400_000_000_000_i128).contains(&nanos) {
        return Err(Error::Schema);
    }
    Ok(Value::Integer(nanos))
}

pub(super) fn transform(
    value: &Value,
    kind: &PrimitiveType,
    transform: Option<&PartitionTransform>,
) -> Result<(), Error> {
    if matches!(value, Value::Null) {
        return Ok(());
    }
    let valid = match transform {
        None | Some(PartitionTransform::Void) => false,
        Some(PartitionTransform::Bucket(buckets)) => {
            matches!(value, Value::Integer(value) if (0..i128::from(*buckets)).contains(value))
        }
        Some(PartitionTransform::Truncate(width)) => match value {
            Value::Integer(value) => value.rem_euclid(i128::from(*width)) == 0,
            Value::Bytes(bytes) if *kind == PrimitiveType::String => {
                std::str::from_utf8(bytes)
                    .map_err(|_| Error::Schema)?
                    .chars()
                    .count()
                    <= usize::try_from(*width).map_err(|_| Error::Schema)?
            }
            Value::Bytes(bytes) => bytes.len() <= usize::try_from(*width).map_err(|_| Error::Schema)?,
            _ => false,
        },
        Some(PartitionTransform::Unknown(_)) => return Err(Error::Unsupported),
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
