use serde_json::Value;
use std::collections::BTreeSet;

use super::TableMetadataError as Error;
use crate::manifest::{ManifestVersion, PrimitiveType};

mod scalar;

pub(super) fn validate(schema: &Value, version: ManifestVersion, work: &mut usize) -> Result<(), Error> {
    charge(work)?;
    match schema["type"].as_str() {
        Some("struct") => {
            for field in schema["fields"].as_array().ok_or(Error::Field("fields"))? {
                for name in ["initial-default", "write-default"] {
                    if let Some(default) = field.get(name).filter(|value| !value.is_null()) {
                        if field["type"]["type"] == "struct" {
                            if !default.as_object().is_some_and(serde_json::Map::is_empty) {
                                return Err(Error::Field(name));
                            }
                        } else {
                            value(&field["type"], default, version, work)?;
                        }
                    }
                }
                validate(&field["type"], version, work)?;
            }
        }
        Some("list") => validate(&schema["element"], version, work)?,
        Some("map") => {
            validate(&schema["key"], version, work)?;
            validate(&schema["value"], version, work)?;
        }
        _ => {}
    }
    Ok(())
}

fn value(schema: &Value, default: &Value, version: ManifestVersion, work: &mut usize) -> Result<(), Error> {
    charge(work)?;
    if default.is_null() {
        return Ok(());
    }
    if let Some(name) = schema.as_str() {
        let primitive = PrimitiveType::parse(name, version).map_err(|_| Error::Field("type"))?;
        return scalar::validate(&primitive, default);
    }
    match schema["type"].as_str() {
        Some("list") => {
            for entry in default.as_array().ok_or(Error::Field("default"))? {
                nullable(&schema["element-required"], entry)?;
                value(&schema["element"], entry, version, work)?;
            }
        }
        Some("map") => {
            let keys = default["keys"].as_array().ok_or(Error::Field("default"))?;
            let values = default["values"].as_array().ok_or(Error::Field("default"))?;
            if keys.len() != values.len() {
                return Err(Error::Field("default"));
            }
            let mut unique = BTreeSet::new();
            for (key, entry) in keys.iter().zip(values) {
                if key.is_null() {
                    return Err(Error::Field("default"));
                }
                value(&schema["key"], key, version, work)?;
                if !unique.insert(identity(&schema["key"], key, version, work)?) {
                    return Err(Error::Field("default"));
                }
                nullable(&schema["value-required"], entry)?;
                value(&schema["value"], entry, version, work)?;
            }
        }
        Some("struct") => {
            let object = default.as_object().ok_or(Error::Field("default"))?;
            for field in schema["fields"].as_array().ok_or(Error::Field("fields"))? {
                charge(work)?;
                let key = field["id"].as_i64().ok_or(Error::Field("id"))?.to_string();
                if let Some(entry) = object.get(&key) {
                    nullable(&field["required"], entry)?;
                    value(&field["type"], entry, version, work)?;
                }
            }
        }
        _ => return Err(Error::Field("type")),
    }
    Ok(())
}

fn nullable(required: &Value, value: &Value) -> Result<(), Error> {
    if required == true && value.is_null() {
        return Err(Error::Field("default"));
    }
    Ok(())
}

fn charge(work: &mut usize) -> Result<(), Error> {
    *work = work.checked_sub(1).ok_or(Error::Bounds)?;
    Ok(())
}

pub(super) fn identity(
    schema: &Value,
    value: &Value,
    version: ManifestVersion,
    work: &mut usize,
) -> Result<String, Error> {
    charge(work)?;
    if value.is_null() {
        return Ok("null".into());
    }
    if let Some(name) = schema.as_str() {
        let kind = PrimitiveType::parse(name, version).map_err(|_| Error::Field("type"))?;
        return Ok(serde_json::to_string(&scalar::identity(&kind, value)?)?);
    }
    let mut parts = Vec::new();
    match schema["type"].as_str() {
        Some("list") => {
            for entry in value.as_array().ok_or(Error::Field("default"))? {
                parts.push(identity(&schema["element"], entry, version, work)?);
            }
        }
        Some("struct") => {
            for field in schema["fields"].as_array().ok_or(Error::Field("fields"))? {
                let key = field["id"].as_i64().ok_or(Error::Field("id"))?.to_string();
                parts.push(identity(&field["type"], &value[&key], version, work)?);
            }
        }
        Some("map") => {
            let keys = value["keys"].as_array().ok_or(Error::Field("default"))?;
            let values = value["values"].as_array().ok_or(Error::Field("default"))?;
            let mut entries = std::collections::BTreeMap::new();
            for (key, entry) in keys.iter().zip(values) {
                entries.insert(
                    identity(&schema["key"], key, version, work)?,
                    identity(&schema["value"], entry, version, work)?,
                );
            }
            return Ok(serde_json::to_string(&entries)?);
        }
        _ => return Err(Error::Field("type")),
    }
    Ok(serde_json::to_string(&parts)?)
}
