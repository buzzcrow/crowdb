use serde_json::Value;
use std::collections::BTreeMap;

use super::super::{
    integer, nonnegative, optional_array, text, TableMetadataError as Error, TableMetadataLimits,
};
use super::{location, TableSnapshot};
use crate::table::TableHead;

pub(crate) fn references(
    root: &Value,
    current: Option<i64>,
    snapshots: &BTreeMap<i64, TableSnapshot>,
    limits: TableMetadataLimits,
) -> Result<(), Error> {
    if current.is_some_and(|current| !snapshots.contains_key(&current)) {
        return Err(Error::Field("current-snapshot-id"));
    }
    let Some(refs) = root.get("refs").filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let refs = refs.as_object().ok_or(Error::Field("refs"))?;
    if refs.len() > limits.collection_entries {
        return Err(Error::Bounds);
    }
    let mut main = None;
    for (name, reference) in refs {
        if name.is_empty() {
            return Err(Error::Field("refs"));
        }
        let snapshot = integer(&reference["snapshot-id"], "snapshot-id")?;
        if !snapshots.contains_key(&snapshot) {
            return Err(Error::Field("refs"));
        }
        let kind = text(&reference["type"], "type")?;
        if !matches!(kind, "tag" | "branch") || (name == "main" && kind != "branch") {
            return Err(Error::Field("type"));
        }
        for field in ["min-snapshots-to-keep", "max-snapshot-age-ms", "max-ref-age-ms"] {
            if let Some(value) = reference.get(field) {
                let value = nonnegative(value, field)?;
                if value == 0
                    || (kind == "tag" && field != "max-ref-age-ms")
                    || (field == "min-snapshots-to-keep" && value > i64::from(i32::MAX))
                {
                    return Err(Error::Field(field));
                }
            }
        }
        if name == "main" {
            main = Some(snapshot);
        }
    }
    if main != current {
        return Err(Error::Field("refs.main"));
    }
    Ok(())
}

pub(crate) fn logs(
    root: &Value,
    head: &TableHead,
    snapshots: &BTreeMap<i64, TableSnapshot>,
    limits: TableMetadataLimits,
) -> Result<(), Error> {
    for field in ["snapshot-log", "metadata-log"] {
        let mut previous = None;
        for entry in optional_array(root, field, limits)? {
            let timestamp = integer(&entry["timestamp-ms"], "timestamp-ms")?;
            if previous.is_some_and(|previous| i128::from(timestamp) - i128::from(previous) < -60_000) {
                return Err(Error::Field(field));
            }
            previous = Some(timestamp);
            if field == "snapshot-log" {
                let snapshot = integer(&entry["snapshot-id"], "snapshot-id")?;
                if !snapshots.contains_key(&snapshot) {
                    return Err(Error::Field(field));
                }
            } else {
                location(
                    &entry["metadata-file"],
                    "metadata-file",
                    head.metadata_location.table(),
                )?;
            }
        }
        let updated = integer(&root["last-updated-ms"], "last-updated-ms")?;
        if previous.is_some_and(|previous| i128::from(updated) - i128::from(previous) < -60_000) {
            return Err(Error::Field("last-updated-ms"));
        }
    }
    Ok(())
}
