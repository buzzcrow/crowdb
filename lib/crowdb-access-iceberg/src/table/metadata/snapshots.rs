use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use super::{
    array, id, integer, nonnegative, optional_array, root::Envelope, strings, text,
    TableMetadataError as Error, TableMetadataLimits,
};
use crate::{
    file::{FileLocation, TableLocation},
    manifest::{ManifestListSelection, ManifestVersion},
    table::TableHead,
};

mod references;
pub(super) use references::{logs, references};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableSnapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence: i64,
    pub timestamp_ms: i64,
    pub schema_id: Option<i32>,
    pub manifest_list: Option<FileLocation>,
    pub manifests: Vec<FileLocation>,
    pub first_row_id: Option<i64>,
    pub added_rows: Option<i64>,
}

impl TableSnapshot {
    /// Selects a list-backed snapshot without inventing lineage for upgraded history.
    /// # Errors
    /// Legacy embedded manifests require their own enumerator rather than a fake list.
    pub fn manifest_selection(&self, version: ManifestVersion) -> Result<ManifestListSelection, Error> {
        Ok(ManifestListSelection {
            location: self.manifest_list.clone().ok_or(Error::Field("manifest-list"))?,
            table_version: version,
            snapshot_id: self.snapshot_id,
            parent_snapshot_id: self.parent_snapshot_id,
            sequence: self.sequence,
            first_row_id: self.first_row_id,
            added_rows: self.added_rows,
        })
    }
}

pub(super) fn parse(
    root: &Value,
    head: &TableHead,
    envelope: &Envelope,
    limits: TableMetadataLimits,
) -> Result<BTreeMap<i64, TableSnapshot>, Error> {
    let mut snapshots = BTreeMap::new();
    let mut sequences = BTreeSet::new();
    let mut ranges = BTreeMap::new();
    for value in optional_array(root, "snapshots", limits)? {
        let snapshot = snapshot(value, head, envelope, limits)?;
        if snapshot.sequence > 0 && !sequences.insert(snapshot.sequence) {
            return Err(Error::Field("sequence-number"));
        }
        if let (Some(first), Some(rows)) = (snapshot.first_row_id, snapshot.added_rows) {
            if rows > 0 && ranges.insert(first, first + rows).is_some() {
                return Err(Error::Field("first-row-id"));
            }
        }
        if snapshots.insert(snapshot.snapshot_id, snapshot).is_some() {
            return Err(Error::Field("snapshot-id"));
        }
    }
    let mut end = 0;
    for (first, next) in ranges {
        if first < end {
            return Err(Error::Field("first-row-id"));
        }
        end = next;
    }
    ancestry(&snapshots)?;
    Ok(snapshots)
}

fn snapshot(
    value: &Value,
    head: &TableHead,
    envelope: &Envelope,
    limits: TableMetadataLimits,
) -> Result<TableSnapshot, Error> {
    let snapshot_id = integer(&value["snapshot-id"], "snapshot-id")?;
    let parent_snapshot_id = value
        .get("parent-snapshot-id")
        .map(|value| integer(value, "parent-snapshot-id"))
        .transpose()?;
    let sequence = value
        .get("sequence-number")
        .map_or(Ok(0), |value| nonnegative(value, "sequence-number"))?;
    if sequence > envelope.sequence || parent_snapshot_id == Some(snapshot_id) {
        return Err(Error::Field("sequence-number"));
    }
    let timestamp_ms = integer(&value["timestamp-ms"], "timestamp-ms")?;
    let schema_id = value
        .get("schema-id")
        .filter(|value| !value.is_null())
        .map(|value| id(value, "schema-id"))
        .transpose()?;
    if schema_id.is_some_and(|id| !envelope.schemas.contains(&id)) {
        return Err(Error::Field("schema-id"));
    }
    let table = head.metadata_location.table();
    let manifest_list = value
        .get("manifest-list")
        .map(|value| location(value, "manifest-list", table))
        .transpose()?;
    let manifests = if manifest_list.is_some() {
        if value.get("manifests").is_some() {
            return Err(Error::Field("manifests"));
        }
        Vec::new()
    } else {
        if sequence != 0 {
            return Err(Error::Field("manifest-list"));
        }
        array(&value["manifests"], "manifests", limits)?
            .iter()
            .map(|value| location(value, "manifests", table))
            .collect::<Result<Vec<_>, _>>()?
    };
    if let Some(summary) = value.get("summary") {
        strings(summary, "summary", limits)?;
        if !matches!(
            text(&summary["operation"], "operation")?,
            "append" | "replace" | "overwrite" | "delete"
        ) {
            return Err(Error::Field("operation"));
        }
    } else if sequence > 0 {
        return Err(Error::Field("summary"));
    }
    let first_row_id = optional_nonnegative(value, "first-row-id")?;
    let added_rows = optional_nonnegative(value, "added-rows")?;
    match (first_row_id, added_rows, envelope.next_row) {
        (None, None, _) => {}
        (Some(first), Some(rows), Some(next))
            if sequence > 0 && first.checked_add(rows).is_some_and(|end| end <= next) => {}
        _ => return Err(Error::Field("first-row-id")),
    }
    Ok(TableSnapshot {
        snapshot_id,
        parent_snapshot_id,
        sequence,
        timestamp_ms,
        schema_id,
        manifest_list,
        manifests,
        first_row_id,
        added_rows,
    })
}

fn ancestry(snapshots: &BTreeMap<i64, TableSnapshot>) -> Result<(), Error> {
    let mut complete = BTreeSet::new();
    for snapshot in snapshots.values() {
        let mut path = BTreeSet::new();
        let mut cursor = Some(snapshot);
        while let Some(current) = cursor {
            if complete.contains(&current.snapshot_id) {
                break;
            }
            if !path.insert(current.snapshot_id) {
                return Err(Error::Field("parent-snapshot-id"));
            }
            cursor = current
                .parent_snapshot_id
                .and_then(|parent| snapshots.get(&parent));
            if cursor.is_some_and(|parent| current.sequence > 0 && parent.sequence >= current.sequence) {
                return Err(Error::Field("sequence-number"));
            }
        }
        complete.extend(path);
    }
    Ok(())
}

pub(super) fn location(
    value: &Value,
    field: &'static str,
    table: TableLocation,
) -> Result<FileLocation, Error> {
    let location = text(value, field)?
        .parse::<FileLocation>()
        .map_err(|_| Error::Field(field))?;
    if location.table() != table {
        return Err(Error::Binding);
    }
    Ok(location)
}

fn optional_nonnegative(value: &Value, field: &'static str) -> Result<Option<i64>, Error> {
    value
        .get(field)
        .filter(|value| !value.is_null())
        .map(|value| nonnegative(value, field))
        .transpose()
}
