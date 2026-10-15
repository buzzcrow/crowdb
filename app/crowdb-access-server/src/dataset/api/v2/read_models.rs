// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{operation, snapshot, ApiError},
    predicate::Predicate,
    split_models::{decimal, required_option, revision, Source},
};
use crowdb_access_dataset::{
    split::SplitName, ReadPreparation, ReadSession, SessionPartition, SessionShuffle, SessionSpec,
    SessionSpecV2, SessionSplit,
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub id: String,
    pub snapshot: String,
    pub selection: Predicate,
    pub fields: Vec<String>,
    pub batch_size: usize,
    pub partition: Partition,
    #[serde(deserialize_with = "required_shuffle")]
    pub shuffle: Option<Shuffle>,
    #[serde(deserialize_with = "required_split")]
    pub split: Option<Split>,
    pub expected_page: String,
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
    pub(super) fn native(self) -> Result<(SessionSpecV2, u64), ApiError> {
        if self.snapshot != "latest" && self.snapshot != "stable" {
            snapshot(&self.snapshot)?;
        }
        let spec = SessionSpecV2 {
            read: SessionSpec {
                id: operation(&self.id)?,
                snapshot: self.snapshot,
                selection: self.selection.native()?,
                fields: self.fields,
                batch_size: self.batch_size,
                partition: SessionPartition {
                    worker: self.partition.worker,
                    workers: self.partition.workers,
                    rank: self.partition.rank,
                    world_size: self.partition.world_size,
                },
                shuffle: self
                    .shuffle
                    .map(|s| {
                        Ok::<_, ApiError>(SessionShuffle {
                            seed: decimal(&s.seed)?,
                            group_size: s.group_size,
                        })
                    })
                    .transpose()?,
            },
            split: self
                .split
                .map(|s| {
                    Ok::<_, ApiError>(SessionSplit {
                        revision: s.revision.as_deref().map(revision).transpose()?,
                        name: SplitName::new(s.name).map_err(|_| ApiError::invalid())?,
                    })
                })
                .transpose()?,
        };
        Ok((spec, decimal(&self.expected_page)?))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Acknowledge {
    pub expected: String,
    pub next: String,
}
pub(super) fn session(s: &ReadSession) -> Value {
    json!({"id":s.id, "snapshot":s.snapshot, "plan":s.plan, "acknowledged":s.acknowledged, "expires_at":s.expires_at.to_string(), "closed":s.closed})
}
pub(super) fn preparation(p: ReadPreparation) -> Value {
    json!({"snapshot":p.snapshot.to_string(), "split":p.split.map(Source::from), "next_page":p.next_page.to_string(), "admitted":p.admitted.to_string(), "complete":p.complete, "session":p.session.as_ref().map(session)})
}
