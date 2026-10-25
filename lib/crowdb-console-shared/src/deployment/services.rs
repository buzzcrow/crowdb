// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    config::NodeEntry,
    error::{Error, Result},
};
use crowdb_kv_client::{CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::mgmt::node::{NodeControl, NodeServiceIntent};
use serde_json::Value;

mod identity;
mod planned_start;
pub use planned_start::publish_planned_start;
mod readiness;
pub use readiness::diskdb_storage_ready;

/// Commit a current service intent before asking its monitor to execute it.
/// Identical retries retain the accepted operation identity.
///
/// # Errors
/// Rejects lost quorum, concurrent supersession and failed remote execution.
pub async fn execute(kv: &CrowdbKvClient, node: &NodeEntry, mut intent: NodeServiceIntent) -> Result<Value> {
    claim_service_id(kv, &intent).await?;
    let key = format!("/deployment/services/{}/{}", intent.node_id, intent.service_id);
    let current = kv.get(0, 0, key.as_bytes(), ReadMode::Linearizable, None).await?;
    let revision = match current {
        GetOutcome::Found { value, revision, .. } => {
            let existing: NodeServiceIntent =
                serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string()))?;
            if identity::replay(&existing, &intent) {
                intent.operation_id = existing.operation_id;
                return super::admission::remote_control(node, &NodeControl::Service { intent }).await;
            }
            revision
        }
        GetOutcome::NotFound => 0,
    };
    let value = serde_json::to_vec(&intent).map_err(|error| Error::Config(error.to_string()))?;
    kv.put_cas(0, 0, key.as_bytes(), &value, revision).await?;
    super::admission::remote_control(node, &NodeControl::Service { intent }).await
}

/// Read the current committed intent for one service.
///
/// # Errors
/// Rejects unavailable authority or invalid stored records.
pub async fn current(kv: &CrowdbKvClient, node: u64, service: &str) -> Result<Option<NodeServiceIntent>> {
    let key = format!("/deployment/services/{node}/{service}");
    match kv.get(0, 0, key.as_bytes(), ReadMode::Linearizable, None).await? {
        GetOutcome::Found { value, .. } => serde_json::from_slice(&value)
            .map(Some)
            .map_err(|error| Error::Config(error.to_string())),
        GetOutcome::NotFound => Ok(None),
    }
}

/// Read the bounded, consistent deployment view shared by all consoles.
///
/// # Errors
/// Rejects unavailable authority, oversized views or invalid stored records.
pub async fn list(kv: &CrowdbKvClient) -> Result<Vec<NodeServiceIntent>> {
    let result = kv
        .scan_bounded(0, 0, b"/deployment/services/", b"", b"", 4096, false, None)
        .await?;
    if result.truncated || result.timed_out {
        return Err(Error::Config("deployment view exceeds its bound".into()));
    }
    result
        .items
        .into_iter()
        .map(|(_, value)| serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string())))
        .collect()
}

async fn claim_service_id(kv: &CrowdbKvClient, intent: &NodeServiceIntent) -> Result<()> {
    let key = format!("/deployment/service-owners/{}", intent.service_id);
    let owner = serde_json::to_vec(&(intent.cluster_id.as_str(), intent.node_id, intent.kind.as_str()))
        .map_err(|error| Error::Config(error.to_string()))?;
    match kv.put_cas(0, 0, key.as_bytes(), &owner, 0).await {
        Ok(_) => Ok(()),
        Err(error) => match kv.get(0, 0, key.as_bytes(), ReadMode::Linearizable, None).await? {
            GetOutcome::Found { value, .. } if value.as_ref() == owner.as_slice() => Ok(()),
            GetOutcome::Found { .. } => Err(Error::Conflict {
                kind: "service identity".into(),
                id: intent.service_id.clone(),
            }),
            GetOutcome::NotFound => Err(error.into()),
        },
    }
}
