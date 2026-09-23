use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde_json::Value;

use crate::{manifest::PrimitiveType, table::TableMetadataError as Error};

pub(super) fn validate(kind: &PrimitiveType, value: &Value) -> Result<(), Error> {
    use PrimitiveType::{
        Binary, Boolean, Date, Decimal, Double, Fixed, Float, Geography, Geometry, Int, Long, String, Time,
        Timestamp, TimestampNs, Timestamptz, TimestamptzNs, Unknown, Uuid, Variant,
    };
    let valid = match kind {
        Boolean => value.is_boolean(),
        Int => value.as_i64().is_some_and(|value| i32::try_from(value).is_ok()),
        Long => value.as_i64().is_some(),
        Float => value
            .as_f64()
            .is_some_and(|value| value.is_finite() && value.abs() <= f64::from(f32::MAX)),
        Double => value.as_f64().is_some_and(f64::is_finite),
        String => value.is_string(),
        Uuid => value
            .as_str()
            .is_some_and(|text| text.len() == 36 && uuid::Uuid::parse_str(text).is_ok()),
        Fixed(length) => value
            .as_str()
            .is_some_and(|text| text.len() == length * 2 && hex(text)),
        Binary => value.as_str().is_some_and(hex),
        Decimal { precision, scale } => value
            .as_str()
            .is_some_and(|text| decimal(text, *precision, *scale)),
        Date => value
            .as_str()
            .is_some_and(|text| NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok()),
        Time => value.as_str().is_some_and(|text| time(text, 6)),
        Timestamp | Timestamptz | TimestampNs | TimestamptzNs => {
            value.as_str().is_some_and(|text| timestamp(text, kind))
        }
        Unknown | Variant | Geometry(_) | Geography(_) => false,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Field("default"))
    }
}

fn hex(text: &str) -> bool {
    text.len() % 2 == 0 && text.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn decimal(text: &str, precision: u32, scale: u32) -> bool {
    let (mantissa, exponent) = match text.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => {
            let Ok(exponent) = exponent.parse::<i32>() else {
                return false;
            };
            (mantissa, exponent)
        }
        None => (text, 0),
    };
    let mantissa = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
        || i64::try_from(fraction.len())
            .ok()
            .and_then(|digits| digits.checked_sub(i64::from(exponent)))
            != Some(i64::from(scale))
    {
        return false;
    }
    let significant = whole
        .bytes()
        .chain(fraction.bytes())
        .skip_while(|byte| *byte == b'0')
        .count()
        .max(1);
    significant <= precision as usize
}

fn time(text: &str, precision: usize) -> bool {
    let fraction = text.split_once('.').map_or("", |(_, fraction)| fraction);
    fraction.len() <= precision
        && NaiveTime::parse_from_str(text, "%H:%M:%S%.f").is_ok_and(|time| time.nanosecond() < 1_000_000_000)
}

pub(super) fn identity(kind: &PrimitiveType, value: &Value) -> Result<Value, Error> {
    let text = || value.as_str().ok_or(Error::Field("default"));
    let result = match kind {
        PrimitiveType::Float => format!(
            "{:?}",
            value
                .to_string()
                .parse::<f32>()
                .map_err(|_| Error::Field("default"))?
        ),
        PrimitiveType::Double => format!("{:?}", value.as_f64().ok_or(Error::Field("default"))?),
        PrimitiveType::Uuid | PrimitiveType::Fixed(_) | PrimitiveType::Binary => text()?.to_ascii_lowercase(),
        PrimitiveType::Decimal { .. } => {
            let raw = text()?.split(['e', 'E']).next().ok_or(Error::Field("default"))?;
            let digits: String = raw.chars().filter(char::is_ascii_digit).collect();
            let digits = digits.trim_start_matches('0');
            format!(
                "{}{}",
                if raw.starts_with('-') && !digits.is_empty() {
                    "-"
                } else {
                    ""
                },
                if digits.is_empty() { "0" } else { digits }
            )
        }
        PrimitiveType::Date => NaiveDate::parse_from_str(text()?, "%Y-%m-%d")
            .map_err(|_| Error::Field("default"))?
            .to_string(),
        PrimitiveType::Time => NaiveTime::parse_from_str(text()?, "%H:%M:%S%.f")
            .map_err(|_| Error::Field("default"))?
            .to_string(),
        PrimitiveType::Timestamp
        | PrimitiveType::TimestampNs
        | PrimitiveType::Timestamptz
        | PrimitiveType::TimestamptzNs => {
            let text = text()?;
            NaiveDateTime::parse_from_str(
                text.strip_suffix("+00:00").unwrap_or(text),
                "%Y-%m-%dT%H:%M:%S%.f",
            )
            .map_err(|_| Error::Field("default"))?
            .to_string()
        }
        _ => return Ok(value.clone()),
    };
    Ok(Value::String(result))
}

fn timestamp(text: &str, kind: &PrimitiveType) -> bool {
    let nanos = matches!(kind, PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs);
    let zoned = matches!(kind, PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs);
    let text = if zoned {
        let Some(text) = text.strip_suffix("+00:00") else {
            return false;
        };
        text
    } else {
        text
    };
    let Some((_, clock)) = text.split_once('T') else {
        return false;
    };
    if !time(clock, if nanos { 9 } else { 6 }) {
        return false;
    }
    let Ok(value) = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f") else {
        return false;
    };
    !nanos || value.and_utc().timestamp_nanos_opt().is_some()
}
