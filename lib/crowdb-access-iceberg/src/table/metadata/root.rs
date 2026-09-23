use serde_json::Value;
use std::collections::BTreeSet;

use super::{
    array, id, integer, nonnegative, optional_array, strings, text, TableMetadataError as Error,
    TableMetadataLimits,
};
use crate::{file::TableLocation, table::TableHead};

pub(super) struct Envelope {
    pub sequence: i64,
    pub next_row: Option<i64>,
    pub current_snapshot: Option<i64>,
    pub schemas: BTreeSet<i32>,
}

pub(super) fn validate(
    root: &Value,
    head: &TableHead,
    limits: TableMetadataLimits,
) -> Result<Envelope, Error> {
    if !root.is_object() {
        return Err(Error::Field("metadata"));
    }
    if integer(&root["format-version"], "format-version")? != i64::from(head.format_version) {
        return Err(Error::Binding);
    }
    let location = text(&root["location"], "location")?;
    let location = if location.ends_with('/') {
        location.to_owned()
    } else {
        format!("{location}/")
    };
    if location.parse::<TableLocation>().map_err(|_| Error::Binding)? != head.metadata_location.table() {
        return Err(Error::Binding);
    }
    let uuid = root
        .get("table-uuid")
        .filter(|value| !value.is_null())
        .map(|value| {
            let text = text(value, "table-uuid")?;
            if text.len() != 36 {
                return Err(Error::Field("table-uuid"));
            }
            uuid::Uuid::parse_str(text).map_err(|_| Error::Field("table-uuid"))
        })
        .transpose()?;
    if uuid != head.table_uuid {
        return Err(Error::Binding);
    }
    integer(&root["last-updated-ms"], "last-updated-ms")?;
    id(&root["last-column-id"], "last-column-id")?;
    let sequence = if head.format_version == 1 {
        0
    } else {
        nonnegative(&root["last-sequence-number"], "last-sequence-number")?
    };
    let next_row = if head.format_version == 3 {
        Some(nonnegative(&root["next-row-id"], "next-row-id")?)
    } else {
        None
    };
    let schemas = collection(
        root,
        "schemas",
        "schema-id",
        "current-schema-id",
        head.format_version,
        limits,
    )?;
    collection(
        root,
        "partition-specs",
        "spec-id",
        "default-spec-id",
        head.format_version,
        limits,
    )?;
    collection(
        root,
        "sort-orders",
        "order-id",
        "default-sort-order-id",
        head.format_version,
        limits,
    )?;
    if head.format_version > 1 || root.get("last-partition-id").is_some() {
        id(&root["last-partition-id"], "last-partition-id")?;
    }
    if let Some(properties) = root.get("properties") {
        strings(properties, "properties", limits)?;
    }
    for field in ["statistics", "partition-statistics", "encryption-keys"] {
        optional_array(root, field, limits)?;
    }
    let current_snapshot = root
        .get("current-snapshot-id")
        .filter(|value| !value.is_null())
        .map(|value| integer(value, "current-snapshot-id"))
        .transpose()?
        .filter(|value| *value != -1);
    Ok(Envelope {
        sequence,
        next_row,
        current_snapshot,
        schemas,
    })
}

fn collection(
    root: &Value,
    name: &'static str,
    id_name: &'static str,
    default_name: &'static str,
    version: u8,
    limits: TableMetadataLimits,
) -> Result<BTreeSet<i32>, Error> {
    let Some(value) = root.get(name) else {
        if version != 1 {
            return Err(Error::Field(name));
        }
        return match name {
            "schemas" if root["schema"].is_object() => Ok(BTreeSet::from([root["schema"]
                .get("schema-id")
                .map_or(Ok(0), |value| id(value, "schema-id"))?])),
            "partition-specs" => {
                array(&root["partition-spec"], "partition-spec", limits)?;
                Ok(BTreeSet::from([0]))
            }
            "sort-orders" => Ok(BTreeSet::from([0])),
            _ => Err(Error::Field(name)),
        };
    };
    let values = array(value, name, limits)?;
    let mut ids = BTreeSet::new();
    for value in values {
        let value = id(&value[id_name], id_name)?;
        if !ids.insert(value) {
            return Err(Error::Field(id_name));
        }
    }
    if !ids.contains(&id(&root[default_name], default_name)?) {
        return Err(Error::Field(default_name));
    }
    Ok(ids)
}
