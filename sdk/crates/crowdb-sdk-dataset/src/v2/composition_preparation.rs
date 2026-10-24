// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    required_option, CompositionSource, ControlClient, Decimal, DraftSplit, OpaqueId, SampleId, Schema,
    SplitPolicy,
};
use crate::Error;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareSnapshot {
    #[serde(deserialize_with = "required_option")]
    pub parent: Option<OpaqueId>,
    pub source: CompositionSource,
    #[serde(deserialize_with = "required_option")]
    pub schema: Option<Schema>,
    pub samples: Vec<SnapshotChange>,
    pub splits: Vec<DraftSplit>,
    pub policy: SplitPolicy,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotChange {
    Remove {
        id: SampleId,
    },
    Upsert {
        id: SampleId,
        fields: Vec<SnapshotField>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotField {
    pub name: String,
    pub value: SnapshotValue,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotValue {
    Inline {
        data: String,
    },
    Upload {
        id: OpaqueId,
        length: Decimal,
        md5: String,
    },
    Tombstone,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparedSnapshot {
    pub snapshot: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub initial_split: Option<OpaqueId>,
    pub sample_count: Decimal,
}

impl ControlClient {
    /// Computes complete immutable content from retained caller inputs; head publication is separate.
    /// # Errors
    /// Returns missing capability, source/content conflicts, payload validation or transfer errors.
    pub async fn prepare_snapshot(
        &self,
        snapshot: &OpaqueId,
        request: &PrepareSnapshot,
    ) -> Result<PreparedSnapshot, Error> {
        self.call(
            "composition-preparation",
            "PUT",
            &["snapshots", snapshot.as_str()],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(snapshot.as_str()))
    }
}
