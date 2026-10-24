// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{snapshot, ApiError},
    predicate::Predicate,
    split_models::{decimal, required_option, revision, Source},
};
use crowdb_access_dataset::{split::SplitName, ReadBinding, ReadPartition, ReadShuffle, ReadSpec, ReadSplit};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub snapshot: String,
    pub selection: Predicate,
    pub fields: Vec<String>,
    pub batch_size: usize,
    pub partition: Partition,
    #[serde(deserialize_with = "required_shuffle")]
    pub shuffle: Option<Shuffle>,
    #[serde(deserialize_with = "required_split")]
    pub split: Option<Split>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Partition {
    pub worker: u16,
    pub workers: u16,
    pub rank: u16,
    pub world_size: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Shuffle {
    pub seed: String,
    pub group_size: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Split {
    #[serde(deserialize_with = "required_option")]
    pub revision: Option<String>,
    pub name: String,
}
fn required_shuffle<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Shuffle>, D::Error> {
    Option::deserialize(d)
}
fn required_split<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Split>, D::Error> {
    Option::deserialize(d)
}
impl Plan {
    pub(super) fn native(self) -> Result<ReadSpec, ApiError> {
        if self.snapshot != "latest" && self.snapshot != "stable" {
            snapshot(&self.snapshot)?;
        }
        Ok(ReadSpec {
            snapshot: self.snapshot,
            selection: self.selection.native()?,
            fields: self.fields,
            batch_size: self.batch_size,
            partition: ReadPartition {
                worker: self.partition.worker,
                workers: self.partition.workers,
                rank: self.partition.rank,
                world_size: self.partition.world_size,
            },
            shuffle: self
                .shuffle
                .map(|s| {
                    Ok::<_, ApiError>(ReadShuffle {
                        seed: decimal(&s.seed)?,
                        group_size: s.group_size,
                    })
                })
                .transpose()?,
            split: self
                .split
                .map(|s| {
                    Ok::<_, ApiError>(ReadSplit {
                        revision: s.revision.as_deref().map(revision).transpose()?,
                        name: SplitName::new(s.name).map_err(|_| ApiError::invalid())?,
                    })
                })
                .transpose()?,
        })
    }
}
pub(super) fn binding(value: ReadBinding) -> Value {
    json!({"snapshot":value.snapshot.to_string(), "split":value.split.map(Source::from), "cursor":value.cursor})
}
