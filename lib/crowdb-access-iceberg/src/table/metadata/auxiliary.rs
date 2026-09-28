use std::collections::BTreeSet;

use base64::Engine;
use serde_json::Value;

use super::{
    array, id, integer, nonnegative, optional_array, snapshots::location, strings, text,
    TableMetadataError as Error, TableMetadataLimits,
};
use crate::table::TableHead;

pub(super) fn validate(root: &Value, head: &TableHead, limits: TableMetadataLimits) -> Result<(), Error> {
    for field in ["statistics", "partition-statistics"] {
        let mut snapshots = BTreeSet::new();
        for entry in optional_array(root, field, limits)? {
            let snapshot = integer(&entry["snapshot-id"], "snapshot-id")?;
            if field == "partition-statistics" && !snapshots.insert(snapshot) {
                return Err(Error::Field(field));
            }
            location(
                &entry["statistics-path"],
                "statistics-path",
                head.metadata_location.table(),
            )?;
            let bytes = nonnegative(&entry["file-size-in-bytes"], "file-size-in-bytes")?;
            if field == "statistics" {
                let footer = nonnegative(&entry["file-footer-size-in-bytes"], "file-footer-size-in-bytes")?;
                if footer > bytes {
                    return Err(Error::Field("file-footer-size-in-bytes"));
                }
                if let Some(value) = entry.get("key-metadata") {
                    binary(value, "key-metadata")?;
                }
                for blob in array(&entry["blob-metadata"], "blob-metadata", limits)? {
                    validate_blob(blob, limits)?;
                }
            }
        }
    }
    let mut keys = BTreeSet::new();
    for key in optional_array(root, "encryption-keys", limits)? {
        let key_id = text(&key["key-id"], "key-id")?;
        if key_id.is_empty() || !keys.insert(key_id) {
            return Err(Error::Field("key-id"));
        }
        binary(&key["encrypted-key-metadata"], "encrypted-key-metadata")?;
        if let Some(value) = key.get("encrypted-by-id").filter(|value| !value.is_null()) {
            text(value, "encrypted-by-id")?;
        }
        if let Some(value) = key.get("properties") {
            strings(value, "properties", limits)?;
        }
    }
    for snapshot in optional_array(root, "snapshots", limits)? {
        if let Some(value) = snapshot.get("key-id").filter(|value| !value.is_null()) {
            text(value, "key-id")?;
        }
    }
    Ok(())
}

fn validate_blob(blob: &Value, limits: TableMetadataLimits) -> Result<(), Error> {
    text(&blob["type"], "type")?;
    integer(&blob["snapshot-id"], "snapshot-id")?;
    integer(&blob["sequence-number"], "sequence-number")?;
    for field in array(&blob["fields"], "fields", limits)? {
        if id(field, "fields")? == 0 {
            return Err(Error::Field("fields"));
        }
    }
    if let Some(value) = blob.get("properties") {
        strings(value, "properties", limits)?;
    }
    Ok(())
}

fn binary(value: &Value, field: &'static str) -> Result<(), Error> {
    base64::engine::general_purpose::STANDARD
        .decode(text(value, field)?)
        .map_err(|_| Error::Field(field))?;
    Ok(())
}
