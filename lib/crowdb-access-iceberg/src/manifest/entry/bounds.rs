use super::ManifestEntryError as Error;
use crate::manifest::PrimitiveType;
use std::cmp::Ordering;

enum Bound<'data> {
    Integer(i128),
    Float(f64),
    Bytes(&'data [u8]),
    Point(Vec<f64>),
}

pub(in crate::manifest) fn validate(
    kind: &PrimitiveType,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> Result<(), Error> {
    let lower = lower.map(|bytes| decode(kind, bytes)).transpose()?;
    let upper = upper.map(|bytes| decode(kind, bytes)).transpose()?;
    let order = match (lower, upper) {
        (Some(Bound::Integer(lower)), Some(Bound::Integer(upper))) => Some(lower.cmp(&upper)),
        (Some(Bound::Float(lower)), Some(Bound::Float(upper))) => Some(lower.total_cmp(&upper)),
        (Some(Bound::Bytes(lower)), Some(Bound::Bytes(upper))) => Some(lower.cmp(upper)),
        (Some(Bound::Point(lower)), Some(Bound::Point(upper))) => {
            for (index, (lower, upper)) in lower.iter().zip(&upper).enumerate() {
                if matches!(kind, PrimitiveType::Geography(_)) && index == 0 {
                    continue;
                }
                if lower > upper {
                    return Err(Error::Field);
                }
            }
            None
        }
        _ => None,
    };
    if order == Some(Ordering::Greater) {
        return Err(Error::Field);
    }
    Ok(())
}

fn decode<'data>(kind: &PrimitiveType, bytes: &'data [u8]) -> Result<Bound<'data>, Error> {
    use PrimitiveType::{
        Binary, Boolean, Date, Decimal, Double, Fixed, Float, Geography, Geometry, Int, Long, String, Time,
        Timestamp, TimestampNs, Timestamptz, TimestamptzNs, Uuid,
    };
    Ok(match kind {
        Boolean if bytes.len() == 1 => Bound::Integer(i128::from(bytes[0] != 0)),
        Int | Date => Bound::Integer(i128::from(integer32(bytes)?)),
        Long if bytes.len() == 4 => Bound::Integer(i128::from(integer32(bytes)?)),
        Timestamp | TimestampNs if bytes.len() == 4 => {
            let multiplier = if *kind == Timestamp {
                86_400_000_000_i64
            } else {
                86_400_000_000_000_i64
            };
            Bound::Integer(i128::from(
                i64::from(integer32(bytes)?)
                    .checked_mul(multiplier)
                    .ok_or(Error::Field)?,
            ))
        }
        Long | Time | Timestamp | TimestampNs | Timestamptz | TimestamptzNs => {
            let value = i64::from_le_bytes(bytes.try_into().map_err(|_| Error::Field)?);
            if *kind == Time && !(0..86_400_000_000).contains(&value) {
                return Err(Error::Field);
            }
            Bound::Integer(i128::from(value))
        }
        Float | Double => {
            let value = if bytes.len() == 4 {
                f64::from(f32::from_le_bytes(bytes.try_into().map_err(|_| Error::Field)?))
            } else if *kind == Double {
                f64::from_le_bytes(bytes.try_into().map_err(|_| Error::Field)?)
            } else {
                return Err(Error::Field);
            };
            if value.is_nan() {
                return Err(Error::Field);
            }
            Bound::Float(value)
        }
        String => {
            std::str::from_utf8(bytes).map_err(|_| Error::Field)?;
            Bound::Bytes(bytes)
        }
        Uuid if bytes.len() == 16 => Bound::Bytes(bytes),
        Fixed(size) if bytes.len() == *size => Bound::Bytes(bytes),
        Binary => Bound::Bytes(bytes),
        Decimal { precision, .. } => Bound::Integer(decimal(bytes, *precision)?),
        Geometry(_) | Geography(_) => Bound::Point(point(bytes, matches!(kind, Geography(_)))?),
        PrimitiveType::Variant => return Err(crate::manifest::ManifestContextError::Unsupported.into()),
        _ => return Err(Error::Field),
    })
}

fn integer32(bytes: &[u8]) -> Result<i32, Error> {
    Ok(i32::from_le_bytes(bytes.try_into().map_err(|_| Error::Field)?))
}

pub(super) fn decimal(bytes: &[u8], precision: u32) -> Result<i128, Error> {
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(Error::Field);
    }
    let mut padded = [if bytes[0] & 128 == 0 { 0 } else { 255 }; 16];
    padded[16 - bytes.len()..].copy_from_slice(bytes);
    let value = i128::from_be_bytes(padded);
    if value.unsigned_abs() >= 10_u128.pow(precision) {
        return Err(Error::Field);
    }
    Ok(value)
}

fn point(bytes: &[u8], geography: bool) -> Result<Vec<f64>, Error> {
    if !matches!(bytes.len(), 16 | 24 | 32) {
        return Err(Error::Field);
    }
    let values = bytes
        .chunks_exact(8)
        .map(|bytes| Ok(f64::from_le_bytes(bytes.try_into().map_err(|_| Error::Field)?)))
        .collect::<Result<Vec<_>, Error>>()?;
    for (index, value) in values.iter().enumerate() {
        if !(value.is_finite() || index == 2 && values.len() == 4 && value.is_nan()) {
            return Err(Error::Field);
        }
    }
    if geography && (!(-180.0..=180.0).contains(&values[0]) || !(-90.0..=90.0).contains(&values[1])) {
        return Err(Error::Field);
    }
    Ok(values)
}
