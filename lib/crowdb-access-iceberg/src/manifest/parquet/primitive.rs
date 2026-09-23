use crate::file::{ParquetLogicalType as Logical, ParquetSchemaElement, ParquetTimeUnit as Unit};
use crate::manifest::PrimitiveType;

use super::SelectedParquetError as Error;

pub(super) fn validate(field: &ParquetSchemaElement, expected: &PrimitiveType) -> Result<(), Error> {
    if *expected == PrimitiveType::Unknown {
        return Ok(());
    }
    let logical = annotation(field)?;
    let physical = field.physical_type.ok_or(Error::Schema)?;
    let actual = match logical {
        Some(Logical::String | Logical::Enum | Logical::Json) if physical == 6 => PrimitiveType::String,
        Some(Logical::Bson) if physical == 6 => PrimitiveType::Binary,
        Some(Logical::Date) if physical == 1 => PrimitiveType::Date,
        Some(Logical::Time { unit, .. })
            if (unit == Unit::Millis && physical == 1)
                || (matches!(unit, Unit::Micros | Unit::Nanos) && physical == 2) =>
        {
            PrimitiveType::Time
        }
        Some(Logical::Timestamp { adjusted_to_utc, .. }) if physical == 2 => {
            return if matches!(
                (expected, adjusted_to_utc),
                (PrimitiveType::Timestamp | PrimitiveType::TimestampNs, false)
                    | (PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs, true)
            ) {
                Ok(())
            } else {
                Err(Error::Schema)
            };
        }
        Some(Logical::Integer { bit_width, signed })
            if (bit_width <= 32 && physical == 1) || (bit_width == 64 && physical == 2) =>
        {
            match (bit_width, signed) {
                (8 | 16, _) | (32, true) => PrimitiveType::Int,
                (32, false) | (64, true) => PrimitiveType::Long,
                _ => return Err(Error::Unsupported),
            }
        }
        Some(Logical::Decimal { scale, precision }) => decimal(field, physical, precision, scale)?,
        Some(Logical::Uuid) if physical == 7 && field.type_length == Some(16) => PrimitiveType::Uuid,
        Some(Logical::Geometry { crs }) if physical == 6 => {
            PrimitiveType::Geometry(crs.unwrap_or_else(|| "OGC:CRS84".into()))
        }
        Some(Logical::Geography { crs, algorithm }) if physical == 6 => {
            let algorithm = match algorithm.unwrap_or(0) {
                0 => "spherical",
                1 => "vincenty",
                2 => "thomas",
                3 => "andoyer",
                4 => "karney",
                _ => return Err(Error::Unsupported),
            };
            PrimitiveType::Geography(format!("{},{algorithm}", crs.as_deref().unwrap_or("OGC:CRS84")))
        }
        None => match physical {
            0 => PrimitiveType::Boolean,
            1 => PrimitiveType::Int,
            2 => PrimitiveType::Long,
            3 if matches!(
                expected,
                PrimitiveType::Timestamp
                    | PrimitiveType::Timestamptz
                    | PrimitiveType::TimestampNs
                    | PrimitiveType::TimestamptzNs
            ) =>
            {
                return Ok(())
            }
            4 => PrimitiveType::Float,
            5 => PrimitiveType::Double,
            6 if *expected == PrimitiveType::String => PrimitiveType::String,
            6 => PrimitiveType::Binary,
            7 => PrimitiveType::Fixed(
                usize::try_from(field.type_length.ok_or(Error::Schema)?).map_err(|_| Error::Schema)?,
            ),
            _ => return Err(Error::Schema),
        },
        Some(Logical::Unrecognized(_) | Logical::Float16 | Logical::Unknown) => {
            return Err(Error::Unsupported)
        }
        _ => return Err(Error::Schema),
    };
    if &actual == expected
        || matches!(
            (&actual, expected),
            (PrimitiveType::Int, PrimitiveType::Long) | (PrimitiveType::Float, PrimitiveType::Double)
        )
    {
        return Ok(());
    }
    if let (
        PrimitiveType::Decimal { precision, scale },
        PrimitiveType::Decimal {
            precision: target,
            scale: target_scale,
        },
    ) = (&actual, expected)
    {
        if precision <= target && scale == target_scale {
            return Ok(());
        }
    }
    Err(Error::Schema)
}

pub(super) fn annotation(field: &ParquetSchemaElement) -> Result<Option<Logical>, Error> {
    if let Some(logical) = &field.logical_type {
        return Ok(Some(logical.clone()));
    }
    Ok(match field.converted_type {
        None | Some(2) => None,
        Some(0) => Some(Logical::String),
        Some(1) => Some(Logical::Map),
        Some(3) => Some(Logical::List),
        Some(4) => Some(Logical::Enum),
        Some(5) => Some(Logical::Decimal {
            scale: field.scale.ok_or(Error::Schema)?,
            precision: field.precision.ok_or(Error::Schema)?,
        }),
        Some(6) => Some(Logical::Date),
        Some(7 | 8) => Some(Logical::Time {
            adjusted_to_utc: true,
            unit: if field.converted_type == Some(7) {
                Unit::Millis
            } else {
                Unit::Micros
            },
        }),
        Some(9 | 10) => Some(Logical::Timestamp {
            adjusted_to_utc: true,
            unit: if field.converted_type == Some(9) {
                Unit::Millis
            } else {
                Unit::Micros
            },
        }),
        Some(value @ 11..=18) => Some(Logical::Integer {
            bit_width: [8, 16, 32, 64][usize::try_from((value - 11) % 4).map_err(|_| Error::Schema)?],
            signed: value >= 15,
        }),
        Some(19) => Some(Logical::Json),
        Some(20) => Some(Logical::Bson),
        _ => return Err(Error::Unsupported),
    })
}

fn decimal_bytes(precision: i32) -> i32 {
    let maximum = 10_u128.pow(u32::try_from(precision).unwrap_or(38)) - 1;
    let bits = 128 - maximum.leading_zeros() + 1;
    i32::try_from(bits.div_ceil(8)).unwrap_or(16)
}

fn decimal(
    field: &ParquetSchemaElement,
    physical: i32,
    precision: i32,
    scale: i32,
) -> Result<PrimitiveType, Error> {
    if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
        return Err(Error::Schema);
    }
    let fits = match physical {
        1 => precision <= 9,
        2 => precision <= 18,
        6 => true,
        7 => field
            .type_length
            .is_some_and(|length| length >= decimal_bytes(precision)),
        _ => false,
    };
    if !fits {
        return Err(Error::Schema);
    }
    Ok(PrimitiveType::Decimal {
        precision: u32::try_from(precision).map_err(|_| Error::Schema)?,
        scale: u32::try_from(scale).map_err(|_| Error::Schema)?,
    })
}
