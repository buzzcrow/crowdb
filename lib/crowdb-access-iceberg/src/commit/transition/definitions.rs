use std::collections::BTreeMap;

use serde_json::Value;

use super::charge;
use crate::table::{TableMetadataDocument, TableMetadataError as Error};

pub(super) fn validate(
    prior: &TableMetadataDocument,
    candidate: &TableMetadataDocument,
    work: &mut usize,
) -> Result<(), Error> {
    for (collection, identity, legacy) in [
        ("schemas", "schema-id", Some("schema")),
        ("partition-specs", "spec-id", Some("partition-spec")),
        ("sort-orders", "order-id", None),
    ] {
        let before = definitions(prior, collection, identity, legacy, work)?;
        let after = definitions(candidate, collection, identity, legacy, work)?;
        for (identity, definition) in after {
            if let Some(previous) = before.get(&identity) {
                if previous != &definition {
                    return Err(Error::Field(collection));
                }
            }
        }
    }
    Ok(())
}

fn definitions(
    document: &TableMetadataDocument,
    collection: &'static str,
    identity: &'static str,
    legacy: Option<&str>,
    work: &mut usize,
) -> Result<BTreeMap<i64, Value>, Error> {
    let root = document.fields();
    let values = match root.get(collection) {
        Some(Value::Array(values)) => values.as_slice(),
        Some(_) => return Err(Error::Field(collection)),
        None => legacy
            .and_then(|name| root.get(name))
            .map_or(&[] as &[Value], std::slice::from_ref),
    };
    let mut result = BTreeMap::new();
    for value in values {
        charge_tree(value, work)?;
        let id = value.get(identity).and_then(Value::as_i64).unwrap_or(0);
        let normalized = normalize(value, collection, identity)?;
        if result.insert(id, normalized).is_some() {
            return Err(Error::Field(collection));
        }
    }
    Ok(result)
}

fn normalize(value: &Value, collection: &'static str, identity: &str) -> Result<Value, Error> {
    let mut normalized = if collection == "partition-specs" && value.is_array() {
        serde_json::json!({"fields":value})
    } else {
        value.clone()
    };
    let object = normalized.as_object_mut().ok_or(Error::Field(collection))?;
    object.remove(identity);
    if collection == "schemas" {
        match object.get_mut("identifier-field-ids") {
            None => {}
            Some(Value::Array(fields)) if fields.is_empty() => {
                object.remove("identifier-field-ids");
            }
            Some(Value::Array(fields)) => fields.sort_by_key(Value::as_i64),
            Some(_) => return Err(Error::Field(collection)),
        }
    }
    if collection == "partition-specs" {
        let fields = object
            .get_mut("fields")
            .and_then(Value::as_array_mut)
            .ok_or(Error::Field(collection))?;
        for (index, field) in fields.iter_mut().enumerate() {
            let field = field.as_object_mut().ok_or(Error::Field(collection))?;
            field
                .entry("field-id")
                .or_insert_with(|| Value::from(1000 + index));
        }
    }
    Ok(normalized)
}

fn charge_tree(value: &Value, work: &mut usize) -> Result<(), Error> {
    charge(work)?;
    match value {
        Value::Array(values) => {
            for value in values {
                charge_tree(value, work)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                charge_tree(value, work)?;
            }
        }
        _ => {}
    }
    Ok(())
}
