// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Portable JSON control records, independent of persisted authority records.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize)]
pub(super) struct DatasetInfo {
    pub id: String,
    pub namespace: String,
    pub name: String,
    pub latest: Option<String>,
    pub stable: Option<String>,
}
#[derive(Deserialize, Serialize)]
pub(super) struct FieldDefinition {
    pub name: String,
    pub required: bool,
}
#[derive(Deserialize, Serialize)]
pub(super) struct Manifest {
    pub schema: Vec<FieldDefinition>,
    pub samples: Vec<Sample>,
}
#[derive(Deserialize, Serialize)]
pub(super) struct Sample {
    pub id: String,
    #[serde(deserialize_with = "unique_fields")]
    pub fields: BTreeMap<String, FieldValue>,
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum FieldValue {
    Inline { data: String },
    Payload { upload: String },
    Tombstone,
}
#[derive(Deserialize)]
pub(super) struct Publication {
    pub operation: String,
    #[serde(deserialize_with = "required_option")]
    pub parent: Option<String>,
    pub manifest: Manifest,
}
#[derive(Serialize)]
pub(super) struct Snapshot {
    pub id: String,
    pub parent: Option<String>,
    pub operation: String,
    pub state: String,
    pub is_latest: bool,
}
#[derive(Deserialize)]
pub(super) struct SnapshotRequest {
    pub snapshot: String,
}

fn required_option<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}

fn unique_fields<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<String, FieldValue>, D::Error> {
    struct Fields;
    impl<'de> serde::de::Visitor<'de> for Fields {
        type Value = BTreeMap<String, FieldValue>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a field map with unique names")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut fields = BTreeMap::new();
            while let Some((name, value)) = map.next_entry()? {
                if fields.insert(name, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate field"));
                }
            }
            Ok(fields)
        }
    }
    d.deserialize_map(Fields)
}
