use std::collections::BTreeSet;

use super::{
    compact::Value, logical, ParquetLogicalType, ParquetMetadataError as Error, ParquetMetadataLimits,
};

#[derive(Debug, Eq, PartialEq)]
pub struct ParquetSchemaElement {
    pub name: String,
    pub field_id: Option<i32>,
    pub physical_type: Option<i32>,
    pub repetition: Option<i32>,
    pub children: usize,
    pub type_length: Option<i32>,
    pub converted_type: Option<i32>,
    pub scale: Option<i32>,
    pub precision: Option<i32>,
    pub logical_type: Option<ParquetLogicalType>,
}

pub(super) fn decode(
    value: &Value<'_>,
    limits: ParquetMetadataLimits,
) -> Result<Vec<ParquetSchemaElement>, Error> {
    let values = value.list(12)?;
    if values.is_empty() || values.len() > limits.schema_elements {
        return Err(Error::Bounds);
    }
    let mut schema = Vec::with_capacity(values.len());
    let mut pending = vec![1_usize];
    let mut ids = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        while pending.last() == Some(&0) {
            pending.pop();
        }
        let remaining = pending.last_mut().ok_or(Error::Invalid)?;
        *remaining -= 1;
        let field = element(value)?;
        if index == 0 {
            if field.physical_type.is_some() || field.repetition.is_some_and(|value| value != 0) {
                return Err(Error::Invalid);
            }
        } else if field.repetition.is_none() {
            return Err(Error::Invalid);
        }
        if field.field_id.is_some_and(|id| !ids.insert(id)) {
            return Err(Error::Invalid);
        }
        if field.children > 0 {
            if pending.len() >= limits.depth {
                return Err(Error::Bounds);
            }
            pending.push(field.children);
        }
        schema.push(field);
    }
    if pending.iter().any(|count| *count != 0) {
        return Err(Error::Invalid);
    }
    Ok(schema)
}

fn element(value: &Value<'_>) -> Result<ParquetSchemaElement, Error> {
    let fields = value.fields()?;
    let number = |id| {
        fields
            .get(&id)
            .map(|value| {
                value
                    .integer(5)
                    .and_then(|value| i32::try_from(value).map_err(|_| Error::Invalid))
            })
            .transpose()
    };
    let physical_type = number(1)?;
    let type_length = number(2)?;
    let repetition = number(3)?;
    let name =
        std::str::from_utf8(fields.get(&4).ok_or(Error::Invalid)?.bytes()?).map_err(|_| Error::Invalid)?;
    let children = number(5)?;
    if name.is_empty()
        || name.len() > 1024
        || physical_type.is_some_and(|kind| !(0..=7).contains(&kind))
        || repetition.is_some_and(|kind| !(0..=2).contains(&kind))
        || children.is_some_and(|count| count < 0)
        || physical_type.is_some() && children.is_some_and(|count| count != 0)
        || physical_type.is_none() && children.is_none()
        || physical_type == Some(7) && !type_length.is_some_and(|length| length > 0)
    {
        return Err(Error::Invalid);
    }
    let logical_type = fields.get(&10).map(logical::decode).transpose()?;
    Ok(ParquetSchemaElement {
        name: name.to_owned(),
        field_id: number(9)?,
        physical_type,
        repetition,
        children: usize::try_from(children.unwrap_or(0)).map_err(|_| Error::Invalid)?,
        type_length,
        converted_type: number(6)?,
        scale: number(7)?,
        precision: number(8)?,
        logical_type,
    })
}
