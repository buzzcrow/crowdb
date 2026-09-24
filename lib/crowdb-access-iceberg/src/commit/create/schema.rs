use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::{
    manifest::{ManifestContext, ManifestVersion},
    table::{validate_schema_definition, TableMetadataError as Error, TableMetadataLimits},
};

pub(super) struct FreshSchema {
    pub value: Value,
    pub ids: BTreeMap<i32, i32>,
    pub last_id: i32,
    pub context: ManifestContext,
}

pub(super) fn prepare(input: &Value, version: u8, limits: TableMetadataLimits) -> Result<FreshSchema, Error> {
    let version = match version {
        1 => ManifestVersion::V1,
        2 => ManifestVersion::V2,
        3 => ManifestVersion::V3,
        _ => return Err(Error::Field("format-version")),
    };
    let mut value = input.clone();
    let object = value.as_object_mut().ok_or(Error::Field("schema"))?;
    object.entry("schema-id").or_insert(json!(0));
    let schema_id = value["schema-id"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or(Error::Field("schema-id"))?;
    let encoded = super::super::evaluator::encode_bounded(&value, limits.bytes)?;
    let context = ManifestContext::parse(version, schema_id, 0, encoded.get().as_bytes(), b"[]")
        .map_err(|_| Error::Field("schema"))?;
    validate_schema_definition(&value, version, limits.values)?;
    let original = value.clone();
    let mut ids = BTreeMap::new();
    let mut last_id = 0;
    assign(&mut value, &mut ids, &mut last_id)?;
    defaults(&original, &mut value, &ids)?;
    value["schema-id"] = json!(0);
    if let Some(identifiers) = value.get_mut("identifier-field-ids") {
        for identifier in identifiers
            .as_array_mut()
            .ok_or(Error::Field("identifier-field-ids"))?
        {
            *identifier = json!(mapped(identifier, &ids)?);
        }
    }
    Ok(FreshSchema {
        value,
        ids,
        last_id,
        context,
    })
}

pub(super) fn mapped(value: &Value, ids: &BTreeMap<i32, i32>) -> Result<i32, Error> {
    value
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .and_then(|value| ids.get(&value).copied())
        .ok_or(Error::Field("source-id"))
}

fn next(value: &mut Value, ids: &mut BTreeMap<i32, i32>, last: &mut i32) -> Result<(), Error> {
    let old = value
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or(Error::Field("id"))?;
    *last = last.checked_add(1).ok_or(Error::Bounds)?;
    if ids.insert(old, *last).is_some() {
        return Err(Error::Field("id"));
    }
    *value = json!(*last);
    Ok(())
}

fn assign(value: &mut Value, ids: &mut BTreeMap<i32, i32>, last: &mut i32) -> Result<(), Error> {
    match value["type"].as_str() {
        Some("struct") => {
            let fields = value["fields"].as_array_mut().ok_or(Error::Field("fields"))?;
            for field in fields.iter_mut() {
                next(&mut field["id"], ids, last)?;
            }
            for field in fields {
                assign(&mut field["type"], ids, last)?;
            }
        }
        Some("list") => {
            next(&mut value["element-id"], ids, last)?;
            assign(&mut value["element"], ids, last)?;
        }
        Some("map") => {
            next(&mut value["key-id"], ids, last)?;
            next(&mut value["value-id"], ids, last)?;
            assign(&mut value["key"], ids, last)?;
            assign(&mut value["value"], ids, last)?;
        }
        _ => {}
    }
    Ok(())
}

fn defaults(original: &Value, fresh: &mut Value, ids: &BTreeMap<i32, i32>) -> Result<(), Error> {
    match original["type"].as_str() {
        Some("struct") => {
            let fields = original["fields"].as_array().ok_or(Error::Field("fields"))?;
            let targets = fresh["fields"].as_array_mut().ok_or(Error::Field("fields"))?;
            for (field, target) in fields.iter().zip(targets) {
                for name in ["initial-default", "write-default"] {
                    if let Some(value) = target.get_mut(name) {
                        remap_default(&field["type"], value, ids)?;
                    }
                }
                defaults(&field["type"], &mut target["type"], ids)?;
            }
        }
        Some("list") => defaults(&original["element"], &mut fresh["element"], ids)?,
        Some("map") => {
            defaults(&original["key"], &mut fresh["key"], ids)?;
            defaults(&original["value"], &mut fresh["value"], ids)?;
        }
        _ => {}
    }
    Ok(())
}

fn remap_default(schema: &Value, value: &mut Value, ids: &BTreeMap<i32, i32>) -> Result<(), Error> {
    if value.is_null() {
        return Ok(());
    }
    match schema["type"].as_str() {
        Some("struct") => {
            let source = value.as_object_mut().ok_or(Error::Field("default"))?;
            let original = std::mem::take(source);
            for field in schema["fields"].as_array().ok_or(Error::Field("fields"))? {
                let old = field["id"].as_i64().ok_or(Error::Field("id"))?.to_string();
                if let Some(mut entry) = original.get(&old).cloned() {
                    remap_default(&field["type"], &mut entry, ids)?;
                    source.insert(mapped(&field["id"], ids)?.to_string(), entry);
                }
            }
        }
        Some("list") => {
            for entry in value.as_array_mut().ok_or(Error::Field("default"))? {
                remap_default(&schema["element"], entry, ids)?;
            }
        }
        Some("map") => {
            for (name, kind) in [("keys", "key"), ("values", "value")] {
                for entry in value[name].as_array_mut().ok_or(Error::Field("default"))? {
                    remap_default(&schema[kind], entry, ids)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
