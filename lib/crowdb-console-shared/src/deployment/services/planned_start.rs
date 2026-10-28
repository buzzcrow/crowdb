// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Accepted node plans cannot supersede later explicit service controls.

use crowdb_paxos_kv_client::{GetOutcome, PaxosKvClient, ReadMode};
use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};

use crate::error::{Error, Result};

/// Publish a fixed new Start for the node monitor to reconcile independently.
/// Exact replay is read-only, including its operation identity and configuration.
///
/// # Errors
/// Rejects invalid starts, an existing different intent, changed service ownership
/// and unavailable authority. In particular a later Stop/Restart/Delete can never
/// be overwritten by an old accepted plan or its delayed retry.
pub async fn publish_planned_start(kv: &PaxosKvClient, intent: &NodeServiceIntent) -> Result<()> {
    let operation = uuid::Uuid::parse_str(&intent.operation_id)
        .map_err(|error| Error::Config(format!("planned Start operation: {error}")))?;
    if intent.action != NodeServiceAction::Start
        || operation.is_nil()
        || operation.to_string() != intent.operation_id
        || intent.cluster_id.is_empty()
        || intent.service_id.is_empty()
    {
        return Err(Error::Config(
            "planned service requires a fixed Start identity".into(),
        ));
    }
    super::claim_service_id(kv, intent).await?;
    let key = format!("/deployment/services/{}/{}", intent.node_id, intent.service_id);
    let bytes = serde_json::to_vec(intent).map_err(|error| Error::Config(error.to_string()))?;
    match kv.put_cas(0, 0, key.as_bytes(), &bytes, 0).await {
        Ok(_) => Ok(()),
        Err(error) => match kv.get(0, 0, key.as_bytes(), ReadMode::Linearizable, None).await? {
            GetOutcome::Found { value, .. } => {
                let current: NodeServiceIntent =
                    serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string()))?;
                if &current == intent {
                    Ok(())
                } else {
                    Err(Error::Conflict {
                        kind: "service intent supersedes the accepted node plan".into(),
                        id: intent.service_id.clone(),
                    })
                }
            }
            GetOutcome::NotFound => Err(error.into()),
        },
    }
}
