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
    #[serde(default)]
    pub cleanup_complete: bool,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerPartition {
    pub worker: u16,
    pub workers: u16,
    pub rank: u16,
    pub world_size: u16,
}
impl Default for WorkerPartition {
    fn default() -> Self {
        Self {
            worker: 0,
            workers: 1,
            rank: 0,
            world_size: 1,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shuffle {
    pub seed: u64,
    pub group_size: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Selection {
    Explicit { ids: Vec<String> },
    Prefix { value: String },
    Keyword { value: String },
    Range { start: String, end: String },
    FieldMatch { field: String, value: String },
    And { items: Vec<Selection> },
    Or { items: Vec<Selection> },
    Not { item: Box<Selection> },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadPlan {
    pub snapshot: String,
    pub selection: Selection,
    pub fields: Vec<String>,
    pub batch_size: usize,
    pub partition: WorkerPartition,
    pub shuffle: Option<Shuffle>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBinding {
    pub snapshot: String,
    pub cursor: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotPage {
    pub snapshots: Vec<Snapshot>,
    #[serde(deserialize_with = "required_option")]
    pub next: Option<String>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimState {
    Pending,
    Completed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reclaim {
    pub snapshot: String,
    pub state: ReclaimState,
    pub chunks_reclaimed: u64,
}
