use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Map, Value};

/// An object-shaped schema, layout, snapshot or auxiliary payload, not a semantic proof.
/// The selected-version evaluator must validate its nested fields before applying it.
#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
pub struct MetadataObject {
    fields: Map<String, Value>,
    #[serde(skip)]
    canonical: Option<Box<RawValue>>,
}

impl MetadataObject {
    #[must_use]
    pub fn fields(&self) -> &Map<String, Value> {
        &self.fields
    }

    /// Original wire object, when decoded through `CommitRequest::decode`.
    /// Prefer this to re-encoding unknown optional values during candidate construction.
    #[must_use]
    pub fn canonical(&self) -> Option<&str> {
        self.canonical.as_ref().map(|value| value.get())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotRefType {
    Branch,
    Tag,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    AssignUuid {
        uuid: String,
    },
    UpgradeFormatVersion {
        #[serde(rename = "format-version")]
        format_version: i32,
    },
    AddSchema {
        schema: MetadataObject,
        #[serde(rename = "last-column-id")]
        last_column_id: Option<i32>,
    },
    SetCurrentSchema {
        #[serde(rename = "schema-id")]
        schema_id: i32,
    },
    AddSpec {
        spec: MetadataObject,
    },
    SetDefaultSpec {
        #[serde(rename = "spec-id")]
        spec_id: i32,
    },
    AddSortOrder {
        #[serde(rename = "sort-order")]
        sort_order: MetadataObject,
    },
    SetDefaultSortOrder {
        #[serde(rename = "sort-order-id")]
        sort_order_id: i32,
    },
    AddSnapshot {
        snapshot: MetadataObject,
    },
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "type")]
        kind: SnapshotRefType,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "min-snapshots-to-keep")]
        min_snapshots_to_keep: Option<i32>,
        #[serde(rename = "max-snapshot-age-ms")]
        max_snapshot_age_ms: Option<i64>,
        #[serde(rename = "max-ref-age-ms")]
        max_ref_age_ms: Option<i64>,
    },
    RemoveSnapshots {
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },
    RemoveSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
    },
    SetLocation {
        location: String,
    },
    SetProperties {
        updates: BTreeMap<String, String>,
    },
    RemoveProperties {
        removals: Vec<String>,
    },
    SetStatistics {
        statistics: MetadataObject,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    RemoveStatistics {
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
    },
    SetPartitionStatistics {
        #[serde(rename = "partition-statistics")]
        partition_statistics: MetadataObject,
    },
    RemovePartitionStatistics {
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
    },
    RemovePartitionSpecs {
        #[serde(rename = "spec-ids")]
        spec_ids: Vec<i32>,
    },
    RemoveSchemas {
        #[serde(rename = "schema-ids")]
        schema_ids: Vec<i32>,
    },
    AddEncryptionKey {
        #[serde(rename = "encryption-key")]
        encryption_key: MetadataObject,
    },
    RemoveEncryptionKey {
        #[serde(rename = "key-id")]
        key_id: String,
    },
}

impl TableUpdate {
    pub(super) fn retain_payload(&mut self, raw: &RawValue) -> Result<(), serde_json::Error> {
        let (name, object) = match self {
            Self::AddSchema { schema, .. } => ("schema", schema),
            Self::AddSpec { spec } => ("spec", spec),
            Self::AddSortOrder { sort_order } => ("sort-order", sort_order),
            Self::AddSnapshot { snapshot } => ("snapshot", snapshot),
            Self::SetStatistics { statistics, .. } => ("statistics", statistics),
            Self::SetPartitionStatistics { partition_statistics } => {
                ("partition-statistics", partition_statistics)
            }
            Self::AddEncryptionKey { encryption_key } => ("encryption-key", encryption_key),
            _ => return Ok(()),
        };
        let fields: BTreeMap<&str, &RawValue> = serde_json::from_str(raw.get())?;
        object.canonical = fields.get(name).map(|value| (*value).to_owned());
        Ok(())
    }
}
