use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

use crate::{
    manifest::ManifestContext,
    table::{TableMetadataError as Error, TableMetadataLimits},
};

pub(super) fn prepare(
    request: &Value,
    limits: TableMetadataLimits,
) -> Result<(u8, Map<String, Value>), Error> {
    let mut properties = request
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if properties.len() > limits.collection_entries || properties.values().any(|value| !value.is_string()) {
        return Err(Error::Field("properties"));
    }
    let version = properties
        .get("format-version")
        .map(|value| {
            value
                .as_str()
                .ok_or(Error::Field("format-version"))?
                .parse::<u8>()
                .map_err(|_| Error::Field("format-version"))
        })
        .transpose()?
        .unwrap_or(2);
    if !(1..=3).contains(&version) {
        return Err(Error::Field("format-version"));
    }
    for name in [
        "commit.retry.num-retries",
        "commit.retry.min-wait-ms",
        "commit.retry.max-wait-ms",
        "commit.retry.total-timeout-ms",
    ] {
        if properties.get(name).is_some_and(|value| {
            !value
                .as_str()
                .and_then(|value| value.parse::<i32>().ok())
                .is_some_and(|value| value >= 0)
        }) {
            return Err(Error::Field(name));
        }
    }
    for name in ["encryption.key-id", "encryption.data-key-length"] {
        if version < 3 && properties.contains_key(name) {
            return Err(Error::Field(name));
        }
    }
    if properties
        .get("write.metadata.metrics.max-inferred-column-defaults")
        .is_some_and(|value| {
            value
                .as_str()
                .and_then(|value| value.parse::<i32>().ok())
                .is_none()
        })
    {
        return Err(Error::Field(
            "write.metadata.metrics.max-inferred-column-defaults",
        ));
    }
    for name in [
        "format-version",
        "uuid",
        "snapshot-count",
        "current-snapshot-id",
        "current-snapshot-summary",
        "current-snapshot-timestamp-ms",
        "current-schema",
        "default-partition-spec",
        "default-sort-order",
    ] {
        properties.remove(name);
    }
    properties
        .entry("write.parquet.compression-codec")
        .or_insert(json!("zstd"));
    Ok((version, properties))
}

pub(super) fn validate_columns(
    properties: &Map<String, Value>,
    context: &ManifestContext,
) -> Result<(), Error> {
    let mut names = BTreeSet::new();
    for (_, field) in context.fields() {
        let mut names_at_path = vec![field.name.as_str()];
        let mut short_names = names_at_path.clone();
        let mut parent = field.parent;
        while let Some(parent_id) = parent {
            let field = context.field(parent_id).ok_or(Error::Field("schema"))?;
            names_at_path.push(field.name.as_str());
            let container = field.parent.and_then(|parent| context.field(parent));
            let omitted = field.kind == "struct"
                && container.is_some_and(|container| {
                    (container.kind == "list" && field.name == "element")
                        || (container.kind == "map" && field.name == "value")
                });
            if !omitted {
                short_names.push(field.name.as_str());
            }
            parent = field.parent;
        }
        names_at_path.reverse();
        short_names.reverse();
        names.insert(names_at_path.join("."));
        names.insert(short_names.join("."));
    }
    for name in properties.keys() {
        if name
            .strip_prefix("write.metadata.metrics.column.")
            .is_some_and(|column| !names.contains(column))
        {
            return Err(Error::Field("write.metadata.metrics.column"));
        }
    }
    Ok(())
}
