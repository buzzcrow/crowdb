use super::{compact::Value, ParquetMetadataError as Error};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParquetTimeUnit {
    Millis,
    Micros,
    Nanos,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetLogicalType {
    String,
    Map,
    List,
    Enum,
    Decimal {
        scale: i32,
        precision: i32,
    },
    Date,
    Time {
        adjusted_to_utc: bool,
        unit: ParquetTimeUnit,
    },
    Timestamp {
        adjusted_to_utc: bool,
        unit: ParquetTimeUnit,
    },
    Integer {
        bit_width: i8,
        signed: bool,
    },
    Unknown,
    Json,
    Bson,
    Uuid,
    Float16,
    Variant {
        specification_version: Option<i8>,
    },
    Geometry {
        crs: Option<String>,
    },
    Geography {
        crs: Option<String>,
        algorithm: Option<i32>,
    },
    Unrecognized(i16),
}

pub(super) fn decode(value: &Value<'_>) -> Result<ParquetLogicalType, Error> {
    let (id, value) = union(value)?;
    let fields = value.fields()?;
    let required = |id| fields.get(&id).ok_or(Error::Invalid);
    let result = match id {
        1 => ParquetLogicalType::String,
        2 => ParquetLogicalType::Map,
        3 => ParquetLogicalType::List,
        4 => ParquetLogicalType::Enum,
        5 => {
            let scale = integer(required(1)?)?;
            let precision = integer(required(2)?)?;
            if scale < 0 || precision <= 0 || scale > precision {
                return Err(Error::Invalid);
            }
            ParquetLogicalType::Decimal { scale, precision }
        }
        6 => ParquetLogicalType::Date,
        7 | 8 => {
            let adjusted_to_utc = required(1)?.boolean()?;
            let unit = time_unit(required(2)?)?;
            if id == 7 {
                ParquetLogicalType::Time {
                    adjusted_to_utc,
                    unit,
                }
            } else {
                ParquetLogicalType::Timestamp {
                    adjusted_to_utc,
                    unit,
                }
            }
        }
        10 => {
            let bit_width = i8::try_from(required(1)?.integer(3)?).map_err(|_| Error::Invalid)?;
            if !matches!(bit_width, 8 | 16 | 32 | 64) {
                return Err(Error::Invalid);
            }
            ParquetLogicalType::Integer {
                bit_width,
                signed: required(2)?.boolean()?,
            }
        }
        11 => ParquetLogicalType::Unknown,
        12 => ParquetLogicalType::Json,
        13 => ParquetLogicalType::Bson,
        14 => ParquetLogicalType::Uuid,
        15 => ParquetLogicalType::Float16,
        16 => ParquetLogicalType::Variant {
            specification_version: fields
                .get(&1)
                .map(|value| i8::try_from(value.integer(3)?).map_err(|_| Error::Invalid))
                .transpose()?,
        },
        17 => ParquetLogicalType::Geometry {
            crs: fields.get(&1).map(string).transpose()?,
        },
        18 => ParquetLogicalType::Geography {
            crs: fields.get(&1).map(string).transpose()?,
            algorithm: fields.get(&2).map(integer).transpose()?,
        },
        _ => ParquetLogicalType::Unrecognized(id),
    };
    Ok(result)
}

fn union<'value, 'data>(value: &'value Value<'data>) -> Result<(i16, &'value Value<'data>), Error> {
    let fields = value.fields()?;
    if fields.len() != 1 {
        return Err(Error::Invalid);
    }
    let (id, value) = fields.first_key_value().ok_or(Error::Invalid)?;
    value.fields()?;
    Ok((*id, value))
}

fn time_unit(value: &Value<'_>) -> Result<ParquetTimeUnit, Error> {
    match union(value)?.0 {
        1 => Ok(ParquetTimeUnit::Millis),
        2 => Ok(ParquetTimeUnit::Micros),
        3 => Ok(ParquetTimeUnit::Nanos),
        _ => Err(Error::Unsupported),
    }
}

fn integer(value: &Value<'_>) -> Result<i32, Error> {
    i32::try_from(value.integer(5)?).map_err(|_| Error::Invalid)
}

fn string(value: &Value<'_>) -> Result<String, Error> {
    let string = std::str::from_utf8(value.bytes()?).map_err(|_| Error::Invalid)?;
    if string.len() > 1024 {
        return Err(Error::Bounds);
    }
    Ok(string.to_owned())
}
