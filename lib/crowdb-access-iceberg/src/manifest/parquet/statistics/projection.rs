use std::collections::BTreeMap;

use serde_json::Value;

use super::{charge, Error};
use crate::{
    manifest::{ManifestContext, ManifestVersion, PartitionTransform, PrimitiveType, SchemaField},
    table::TableMetadataDocument,
};

pub(super) struct PartitionField {
    pub source: i32,
    pub transform: PartitionTransform,
    pub result: Option<PrimitiveType>,
    pub may_omit: bool,
}

pub(super) fn project(
    document: &TableMetadataDocument,
    work: &mut usize,
) -> Result<BTreeMap<i32, PartitionField>, Error> {
    let version = match document.selected_head().format_version {
        1 => ManifestVersion::V1,
        2 => ManifestVersion::V2,
        3 => ManifestVersion::V3,
        _ => return Err(Error::Schema),
    };
    let sources = sources(document, version, work)?;
    let root = document.fields();
    let specs = definitions(root.get("partition-specs"), root.get("partition-spec"))?;
    let mut ordered = BTreeMap::new();
    for spec in specs {
        charge(work, 1)?;
        let id = spec.get("spec-id").map_or(Ok(0), identifier)?;
        if ordered.insert(id, spec).is_some() {
            return Err(Error::Schema);
        }
    }
    let mut result = BTreeMap::new();
    for spec in ordered.values().rev() {
        let fields = spec
            .get("fields")
            .unwrap_or(spec)
            .as_array()
            .ok_or(Error::Schema)?;
        for (index, field) in fields.iter().enumerate() {
            charge_value(field, work)?;
            let id = field.get("field-id").map_or_else(
                || {
                    if version != ManifestVersion::V1 {
                        return Err(Error::Schema);
                    }
                    i32::try_from(index)
                        .ok()
                        .and_then(|index| index.checked_add(1000))
                        .ok_or(Error::Schema)
                },
                identifier,
            )?;
            let source = identifier(&field["source-id"])?;
            let transform = PartitionTransform::parse(field["transform"].as_str().ok_or(Error::Schema)?)
                .map_err(|_| Error::Schema)?;
            if matches!(transform, PartitionTransform::Unknown(_)) {
                return Err(Error::Unsupported);
            }
            let source_field = sources.get(&source);
            let primitive = source_field
                .map(|(_, field)| {
                    if field.repeated {
                        return Err(Error::Schema);
                    }
                    field.primitive.as_ref().ok_or(Error::Schema)
                })
                .transpose()?;
            let projected = PartitionField {
                source,
                result: primitive
                    .map(|source| transform.result(source))
                    .transpose()
                    .map_err(|_| Error::Schema)?
                    .flatten(),
                transform,
                may_omit: !source_field.is_some_and(|(current, _)| *current),
            };
            merge(&mut result, id, projected)?;
        }
    }
    Ok(result)
}

fn merge(fields: &mut BTreeMap<i32, PartitionField>, id: i32, field: PartitionField) -> Result<(), Error> {
    if let Some(previous) = fields.get_mut(&id) {
        if previous.source != field.source
            || (previous.transform != field.transform
                && previous.transform != PartitionTransform::Void
                && field.transform != PartitionTransform::Void)
        {
            return Err(Error::Schema);
        }
        if previous.transform == PartitionTransform::Void && field.transform != PartitionTransform::Void {
            *previous = field;
        }
    } else {
        fields.insert(id, field);
    }
    Ok(())
}

fn sources(
    document: &TableMetadataDocument,
    version: ManifestVersion,
    work: &mut usize,
) -> Result<BTreeMap<i32, (bool, SchemaField)>, Error> {
    let root = document.fields();
    let schemas = definitions(root.get("schemas"), root.get("schema"))?;
    let current = root
        .get("current-schema-id")
        .or_else(|| root.get("schema").and_then(|schema| schema.get("schema-id")))
        .map_or(Ok(0), identifier)?;
    let mut ordered = BTreeMap::new();
    for schema in schemas {
        charge_value(schema, work)?;
        let id = schema.get("schema-id").map_or(Ok(0), identifier)?;
        if ordered.insert((id == current, id), schema).is_some() {
            return Err(Error::Schema);
        }
    }
    if !ordered.contains_key(&(true, current)) {
        return Err(Error::Schema);
    }
    let mut sources = BTreeMap::new();
    for ((current, id), schema) in ordered {
        let context = ManifestContext::parse(
            version,
            id,
            0,
            &serde_json::to_vec(schema).map_err(|_| Error::Schema)?,
            b"[]",
        )
        .map_err(|_| Error::Schema)?;
        for (id, field) in context.fields() {
            charge(work, 1)?;
            sources.insert(*id, (current, field.clone()));
        }
    }
    Ok(sources)
}

fn definitions<'data>(
    collection: Option<&'data Value>,
    legacy: Option<&'data Value>,
) -> Result<&'data [Value], Error> {
    match collection {
        Some(value) => value.as_array().map(Vec::as_slice).ok_or(Error::Schema),
        None => legacy.map(std::slice::from_ref).ok_or(Error::Schema),
    }
}

fn identifier(value: &Value) -> Result<i32, Error> {
    value
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value >= 0)
        .ok_or(Error::Schema)
}

fn charge_value(value: &Value, work: &mut usize) -> Result<(), Error> {
    charge(work, 1)?;
    match value {
        Value::Array(values) => {
            for value in values {
                charge_value(value, work)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                charge(work, key.len())?;
                charge_value(value, work)?;
            }
        }
        Value::String(value) => charge(work, value.len())?,
        _ => {}
    }
    Ok(())
}
