// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One cluster-scoped Console catalog operation, without per-node races.

use super::{console, IcebergBootstrapError};
use crowdb_kv_client::{CrowdbKvClient, Error, GetOutcome, ReadMode};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};
use uuid::Uuid;

const KEY: &[u8] = b"/deployment/console-catalog-bootstrap";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Operations {
    version: u32,
    initialize: Uuid,
    activate: Uuid,
}

pub(super) async fn operations(
    root: &Path,
    kv: &CrowdbKvClient,
) -> Result<(Uuid, Uuid), IcebergBootstrapError> {
    tokio::time::timeout(Duration::from_secs(3), reserve(root, kv))
        .await
        .map_err(|_| IcebergBootstrapError::Command("Console catalog authority reservation timed out"))?
}

async fn reserve(root: &Path, kv: &CrowdbKvClient) -> Result<(Uuid, Uuid), IcebergBootstrapError> {
    let local = console::load(&root.join("console-iceberg-bootstrap.json"))?;
    let candidate = Operations {
        version: 1,
        initialize: local.as_ref().map_or_else(Uuid::now_v7, |value| value.initialize),
        activate: local.as_ref().map_or_else(Uuid::now_v7, |value| value.activate),
    };
    let bytes = serde_json::to_vec(&candidate)
        .map_err(|_| IcebergBootstrapError::Conflict("Console catalog operations cannot be encoded"))?;
    for _ in 0..10 {
        if let GetOutcome::Found { value, .. } = kv.get(0, 0, KEY, ReadMode::Linearizable, None).await? {
            if value.len() > 4096 {
                return Err(IcebergBootstrapError::Conflict(
                    "Console catalog operations exceed bound",
                ));
            }
            let current: Operations = serde_json::from_slice(&value)
                .map_err(|_| IcebergBootstrapError::Conflict("Console catalog operations are invalid"))?;
            if current.version != 1
                || current.initialize.get_version_num() != 7
                || current.activate.get_version_num() != 7
                || current.initialize == current.activate
            {
                return Err(IcebergBootstrapError::Conflict(
                    "Console catalog operation identity is invalid",
                ));
            }
            if local.as_ref().is_some_and(|local| {
                local.initialize != current.initialize || local.activate != current.activate
            }) {
                return Err(IcebergBootstrapError::Conflict(
                    "local Console catalog operation differs from Group 0",
                ));
            }
            return Ok((current.initialize, current.activate));
        }
        match kv.put_cas(0, 0, KEY, &bytes, 0).await {
            Ok(_) => return Ok((candidate.initialize, candidate.activate)),
            Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::OutcomeUnknown.into())
}
