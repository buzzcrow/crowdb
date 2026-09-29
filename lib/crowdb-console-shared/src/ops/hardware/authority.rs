// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional Group 0 hardware publication with confirmed outcomes.

use crowdb_kv_client::{BatchOp, Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::key::TextKey;
use crowdb_protocol::{
    common::{NodeValue, RackValue},
    key::{NodeKey, RackKey},
};

use crate::error::{Error, Result};
use crate::ops::OpContext;

pub(super) async fn ready(ctx: &OpContext) -> Result<()> {
    ctx.kv().refresh_topology().await?;
    Ok(())
}

pub(super) async fn create<T: serde::Serialize>(ctx: &OpContext, key: impl TextKey, value: &T) -> Result<()> {
    ready(ctx).await?;
    let path = key.to_path();
    let intended = serde_json::to_value(value).map_err(|error| Error::Config(error.to_string()))?;
    let payload = serde_json::to_vec(value).map_err(|error| Error::Config(error.to_string()))?;
    match ctx.kv().put_cas(0, 0, path.as_bytes(), &payload, 0).await {
        Ok(_) => Ok(()),
        Err(error @ (KvError::CasFailed { .. } | KvError::OutcomeUnknown)) => {
            match ctx
                .kv()
                .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                GetOutcome::Found { value, .. } => {
                    let actual: serde_json::Value =
                        serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string()))?;
                    if actual == intended {
                        Ok(())
                    } else {
                        Err(Error::Conflict {
                            kind: "hardware".into(),
                            id: path,
                        })
                    }
                }
                GetOutcome::NotFound => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// Update the rack membership and create its node in one conditional Group 0 write.
pub(super) async fn create_node(
    ctx: &OpContext,
    rack_id: u64,
    node_id: u64,
    value: &NodeValue,
) -> Result<()> {
    ready(ctx).await?;
    let rack_path = RackKey { rack_id }.to_path();
    let node_path = NodeKey { rack_id, node_id }.to_path();
    for attempt in 0..10u64 {
        let (mut rack, revision) = match ctx
            .kv()
            .get(0, 0, rack_path.as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::Found { value, revision, .. } => (
                serde_json::from_slice::<RackValue>(&value)
                    .map_err(|error| Error::Config(error.to_string()))?,
                revision,
            ),
            GetOutcome::NotFound => {
                return Err(Error::NotFound {
                    kind: "rack".into(),
                    id: rack_id.to_string(),
                })
            }
        };
        let existing = ctx
            .kv()
            .get(0, 0, node_path.as_bytes(), ReadMode::Linearizable, None)
            .await?;
        let node_exists = matches!(existing, GetOutcome::Found { .. });
        if let GetOutcome::Found { value: stored, .. } = existing {
            let actual: NodeValue =
                serde_json::from_slice(&stored).map_err(|error| Error::Config(error.to_string()))?;
            if !same_node_connection(&actual, value) {
                return Err(Error::Conflict {
                    kind: "node".into(),
                    id: node_id.to_string(),
                });
            }
            if rack.node_ids.contains(&node_id) {
                return Ok(());
            }
        }
        if !rack.node_ids.contains(&node_id) {
            rack.node_ids.push(node_id);
            rack.node_ids.sort_unstable();
        }
        let rack_bytes = serde_json::to_vec(&rack).map_err(|error| Error::Config(error.to_string()))?;
        let mut ops = vec![BatchOp::Put {
            key: rack_path.as_bytes().to_vec().into(),
            value: rack_bytes.into(),
        }];
        if !node_exists {
            let node_bytes = serde_json::to_vec(value).map_err(|error| Error::Config(error.to_string()))?;
            ops.push(BatchOp::Put {
                key: node_path.as_bytes().to_vec().into(),
                value: node_bytes.into(),
            });
        }
        match ctx
            .kv()
            .batch_write_cas(0, 0, &ops, rack_path.as_bytes(), revision)
            .await
        {
            Ok(_) => return Ok(()),
            Err(KvError::CasFailed { .. } | KvError::CasBusy) => {
                tokio::time::sleep(std::time::Duration::from_millis((attempt + 1) * 5)).await;
            }
            Err(KvError::OutcomeUnknown) => {
                let confirmed = ctx
                    .kv()
                    .get(0, 0, node_path.as_bytes(), ReadMode::Linearizable, None)
                    .await?;
                if let GetOutcome::Found { value: stored, .. } = confirmed {
                    let actual: NodeValue =
                        serde_json::from_slice(&stored).map_err(|error| Error::Config(error.to_string()))?;
                    if same_node_connection(&actual, value) {
                        let rack = ctx
                            .kv()
                            .get(0, 0, rack_path.as_bytes(), ReadMode::Linearizable, None)
                            .await?;
                        if let GetOutcome::Found { value, .. } = rack {
                            let rack: RackValue = serde_json::from_slice(&value)
                                .map_err(|error| Error::Config(error.to_string()))?;
                            if rack.node_ids.contains(&node_id) {
                                return Ok(());
                            }
                        }
                        return Err(KvError::OutcomeUnknown.into());
                    }
                    return Err(Error::Conflict {
                        kind: "node".into(),
                        id: node_id.to_string(),
                    });
                }
                return Err(KvError::OutcomeUnknown.into());
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(KvError::CasBusy.into())
}

fn same_node_connection(actual: &NodeValue, intended: &NodeValue) -> bool {
    actual.management_host == intended.management_host
        && actual.ssh_port == intended.ssh_port
        && actual.ssh_user == intended.ssh_user
        && actual.ssh_credential_ref == intended.ssh_credential_ref
}
