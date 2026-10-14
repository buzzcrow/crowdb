// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Portable Dataset v1 JSON records. Identifiers and digests are opaque strings.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetInfo {
    pub id: String,
    pub namespace: String,
    pub name: String,
    #[serde(deserialize_with = "required_option")]
    pub latest: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub stable: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub schema: Vec<FieldDefinition>,
    pub samples: Vec<Sample>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sample {
    pub id: String,
    pub fields: BTreeMap<String, FieldValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldValue {
    Inline { data: String },
    Payload { upload: String },
    Tombstone,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Publication {
    pub operation: String,
    #[serde(deserialize_with = "required_option")]
    pub parent: Option<String>,
    pub manifest: Manifest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: String,
    #[serde(deserialize_with = "required_option")]
    pub parent: Option<String>,
    pub operation: String,
    pub state: PublicationState,
    pub is_latest: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationState {
    Prepared,
    Published,
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadState {
    Open,
    Complete,
    Claimed,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub snapshot: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadRequest {
    pub id: String,
    pub length: u64,
    pub part_size: u32,
    pub md5: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Part {
    pub index: u32,
    pub length: u64,
    pub md5: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Upload {
    pub id: String,
    pub length: u64,
    pub part_size: u32,
    pub md5: String,
    pub state: UploadState,
    pub expires_at: u64,
    pub parts: Vec<Part>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchRequest {
    pub snapshot: String,
    pub sample_ids: Vec<String>,
    pub fields: Vec<String>,
}

fn required_option<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}
