use super::{Error, Input};
use std::cmp::Ordering;

pub(super) enum Primitive<'data> {
    Boolean(bool),
    Numeric(i128, u8),
    Float(f32),
    Double(f64),
    Date(i32),
    Time(i64),
    Timestamp(i128, bool),
    Binary(&'data [u8]),
    String(&'data str),
    Uuid(&'data [u8]),
}

pub(super) fn decode(bytes: &[u8]) -> Result<Primitive<'_>, Error> {
    let mut input = Input::new(bytes);
    let header = input.byte()?;
    let value = match header & 3 {
        0 => primitive(&mut input, header >> 2)?,
        1 => Primitive::String(
            std::str::from_utf8(input.take(usize::from(header >> 2))?).map_err(|_| Error::Field)?,
        ),
        _ => return Err(Error::Field),
    };
    input.finish()?;
    Ok(value)
}

fn primitive<'data>(input: &mut Input<'data>, kind: u8) -> Result<Primitive<'data>, Error> {
    Ok(match kind {
        1 | 2 => Primitive::Boolean(kind == 1),
        3..=6 => Primitive::Numeric(signed(input, 1 << (kind - 3))?, 0),
        7 => {
            let value = f64::from_le_bytes(input.take(8)?.try_into().map_err(|_| Error::Field)?);
            if value.is_nan() {
                return Err(Error::Field);
            }
            Primitive::Double(value)
        }
        8..=10 => {
            let scale = input.byte()?;
            let value = signed(input, 4 << (kind - 8))?;
            let precision = [9, 18, 38][usize::from(kind - 8)];
            if scale > 38 || value.unsigned_abs() >= 10_u128.pow(precision) {
                return Err(Error::Field);
            }
            Primitive::Numeric(value, scale)
        }
        11 => Primitive::Date(i32::from_le_bytes(
            input.take(4)?.try_into().map_err(|_| Error::Field)?,
        )),
        12 | 13 | 18 | 19 => {
            let value = signed(input, 8)? * if kind < 18 { 1000 } else { 1 };
            Primitive::Timestamp(value, matches!(kind, 12 | 18))
        }
        14 => {
            let value = f32::from_le_bytes(input.take(4)?.try_into().map_err(|_| Error::Field)?);
            if value.is_nan() {
                return Err(Error::Field);
            }
            Primitive::Float(value)
        }
        15 | 16 => {
            let length = input.uint(4)?;
            let bytes = input.take(length)?;
            if kind == 15 {
                Primitive::Binary(bytes)
            } else {
                Primitive::String(std::str::from_utf8(bytes).map_err(|_| Error::Field)?)
            }
        }
        17 => {
            let value = i64::from_le_bytes(input.take(8)?.try_into().map_err(|_| Error::Field)?);
            if !(0..86_400_000_000).contains(&value) {
                return Err(Error::Field);
            }
            Primitive::Time(value)
        }
        20 => Primitive::Uuid(input.take(16)?),
        0 => return Err(Error::Field),
        _ => return Err(crate::manifest::ManifestContextError::Unsupported.into()),
    })
}

fn signed(input: &mut Input<'_>, length: usize) -> Result<i128, Error> {
    let value = input.take(length)?;
    let mut padded = [if value[length - 1] & 128 == 0 { 0 } else { 255 }; 16];
    padded[..length].copy_from_slice(value);
    Ok(i128::from_le_bytes(padded))
}

impl Primitive<'_> {
    pub(super) fn compare(&self, other: &Self) -> Result<Ordering, Error> {
        Ok(match (self, other) {
            (Self::Boolean(lower), Self::Boolean(upper)) => lower.cmp(upper),
            (Self::Numeric(lower, scale), Self::Numeric(upper, other_scale)) => {
                decimal(*lower, *scale, *upper, *other_scale)
            }
            (Self::Float(lower), Self::Float(upper)) => lower.total_cmp(upper),
            (Self::Double(lower), Self::Double(upper)) => lower.total_cmp(upper),
            (Self::Date(lower), Self::Date(upper)) => lower.cmp(upper),
            (Self::Time(lower), Self::Time(upper)) => lower.cmp(upper),
            (Self::Timestamp(lower, zone), Self::Timestamp(upper, other_zone)) if zone == other_zone => {
                lower.cmp(upper)
            }
            (Self::String(lower), Self::String(upper)) => lower.as_bytes().cmp(upper.as_bytes()),
            (Self::Binary(lower), Self::Binary(upper)) | (Self::Uuid(lower), Self::Uuid(upper)) => {
                lower.cmp(upper)
            }
            _ => return Err(Error::Field),
        })
    }
}

fn decimal(lower: i128, scale: u8, upper: i128, other_scale: u8) -> Ordering {
    if lower.signum() != upper.signum() {
        return lower.signum().cmp(&upper.signum());
    }
    if lower == 0 {
        return Ordering::Equal;
    }
    let mut left = lower.unsigned_abs().to_string();
    let mut right = upper.unsigned_abs().to_string();
    let common = scale.max(other_scale);
    left.extend(std::iter::repeat('0').take(usize::from(common - scale)));
    right.extend(std::iter::repeat('0').take(usize::from(common - other_scale)));
    let order = left.len().cmp(&right.len()).then_with(|| left.cmp(&right));
    if lower < 0 {
        order.reverse()
    } else {
        order
    }
}
