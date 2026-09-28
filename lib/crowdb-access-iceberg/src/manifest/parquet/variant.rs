use std::collections::BTreeSet;

use super::{primitive, SelectedParquetError as Error};
use crate::file::{ParquetLogicalType as Logical, ParquetSchemaElement, ParquetTimeUnit as Unit};
use crate::manifest::PrimitiveType;

pub(super) fn validate(schema: &[ParquetSchemaElement], index: usize, depth: usize) -> Result<usize, Error> {
    let field = schema.get(index).ok_or(Error::Schema)?;
    if !matches!(
        field.logical_type,
        Some(Logical::Variant {
            specification_version: None | Some(1)
        })
    ) {
        return Err(Error::Unsupported);
    }
    value(schema, index, depth, true)
}

fn value(schema: &[ParquetSchemaElement], index: usize, depth: usize, root: bool) -> Result<usize, Error> {
    let field = schema.get(index).ok_or(Error::Schema)?;
    if depth > 32 || field.physical_type.is_some() || (!root && primitive::annotation(field)?.is_some()) {
        return Err(Error::Schema);
    }
    let mut cursor = index + 1;
    let mut names = BTreeSet::new();
    let mut required_value = false;
    for _ in 0..field.children {
        let child = schema.get(cursor).ok_or(Error::Schema)?;
        if !names.insert(child.name.as_str()) {
            return Err(Error::Schema);
        }
        match child.name.as_str() {
            "metadata" if root && child.repetition == Some(0) => {
                binary(child)?;
                cursor += 1;
            }
            "value" if matches!(child.repetition, Some(0 | 1)) => {
                binary(child)?;
                required_value = child.repetition == Some(0);
                cursor += 1;
            }
            "typed_value" if child.repetition == Some(1) => {
                cursor = typed(schema, cursor, depth + 1)?;
            }
            _ => return Err(Error::Schema),
        }
    }
    if root && !names.contains("metadata")
        || !names.contains("value") && !names.contains("typed_value")
        || required_value && names.contains("typed_value")
    {
        return Err(Error::Schema);
    }
    Ok(cursor)
}

fn binary(field: &ParquetSchemaElement) -> Result<(), Error> {
    if field.physical_type != Some(6)
        || field.children != 0
        || field.field_id.is_some()
        || primitive::annotation(field)?.is_some()
    {
        return Err(Error::Schema);
    }
    Ok(())
}

fn typed(schema: &[ParquetSchemaElement], index: usize, depth: usize) -> Result<usize, Error> {
    let field = schema.get(index).ok_or(Error::Schema)?;
    if depth > 32 {
        return Err(Error::Schema);
    }
    if field.physical_type.is_some() {
        typed_primitive(field)?;
        return Ok(index + 1);
    }
    match primitive::annotation(field)? {
        Some(Logical::List) => {
            let repeated = schema.get(index + 1).ok_or(Error::Schema)?;
            let element = schema.get(index + 2).ok_or(Error::Schema)?;
            if field.children != 1
                || repeated.children != 1
                || repeated.physical_type.is_some()
                || repeated.repetition != Some(2)
                || element.repetition != Some(0)
            {
                return Err(Error::Schema);
            }
            value(schema, index + 2, depth + 2, false)
        }
        None => {
            let mut cursor = index + 1;
            let mut names = BTreeSet::new();
            for _ in 0..field.children {
                let child = schema.get(cursor).ok_or(Error::Schema)?;
                if child.repetition != Some(0) || !names.insert(&child.name) {
                    return Err(Error::Schema);
                }
                cursor = value(schema, cursor, depth + 1, false)?;
            }
            Ok(cursor)
        }
        _ => Err(Error::Schema),
    }
}

fn typed_primitive(field: &ParquetSchemaElement) -> Result<(), Error> {
    if field.children != 0 {
        return Err(Error::Schema);
    }
    let expected = match primitive::annotation(field)? {
        Some(Logical::String) => PrimitiveType::String,
        Some(Logical::Integer {
            bit_width: 8 | 16,
            signed: true,
        }) => PrimitiveType::Int,
        Some(Logical::Date) => PrimitiveType::Date,
        Some(Logical::Time {
            adjusted_to_utc: false,
            unit: Unit::Micros,
        }) => PrimitiveType::Time,
        Some(Logical::Timestamp {
            adjusted_to_utc,
            unit: Unit::Micros | Unit::Nanos,
        }) => {
            if adjusted_to_utc {
                PrimitiveType::Timestamptz
            } else {
                PrimitiveType::Timestamp
            }
        }
        Some(Logical::Uuid) => PrimitiveType::Uuid,
        Some(Logical::Decimal { precision, scale }) => PrimitiveType::Decimal {
            precision: u32::try_from(precision).map_err(|_| Error::Schema)?,
            scale: u32::try_from(scale).map_err(|_| Error::Schema)?,
        },
        None => match field.physical_type {
            Some(0) => PrimitiveType::Boolean,
            Some(1) => PrimitiveType::Int,
            Some(2) => PrimitiveType::Long,
            Some(4) => PrimitiveType::Float,
            Some(5) => PrimitiveType::Double,
            Some(6) => PrimitiveType::Binary,
            _ => return Err(Error::Schema),
        },
        _ => return Err(Error::Schema),
    };
    primitive::validate(field, &expected)
}
