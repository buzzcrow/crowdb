// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Confirm bootstrap records without replacing existing authority.

use crate::error::{Error, Result};
use crate::ops::OpContext;
use crowdb_kv_client::{Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::common::{GroupValue, HwStatus, NodeValue, RackValue, ReplicaValue, StoreValue};
use crowdb_protocol::key::{KvGroupKey, KvReplicaKey, KvStoreKey, NodeKey, RackKey, TextKey};

struct Record {
    key: String,
    value: serde_json::Value,
}

impl Record {
    fn new(key: &impl TextKey, value: impl serde::Serialize) -> Result<Self> {
        Ok(Self {
            key: key.to_path(),
            value: serde_json::to_value(value).map_err(|error| Error::Config(error.to_string()))?,
        })
    }

    async fn confirmed(&self, ctx: &OpContext) -> Result<bool> {
        match ctx
            .kv()
            .get(0, 0, self.key.as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::NotFound => Ok(false),
            GetOutcome::Found { value, .. } => {
                let actual: serde_json::Value =
                    serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string()))?;
                if self.same_identity(ctx, &actual)? {
                    Ok(true)
                } else {
                    Err(Error::Conflict {
                        kind: "bootstrap metadata".into(),
                        id: self.key.clone(),
                    })
                }
            }
        }
    }

    fn same_identity(&self, ctx: &OpContext, actual: &serde_json::Value) -> Result<bool> {
        if self.key.starts_with("/hw/rack/") {
            let intended: RackValue = serde_json::from_value(self.value.clone())
                .map_err(|error| Error::Config(error.to_string()))?;
            let actual: RackValue =
                serde_json::from_value(actual.clone()).map_err(|error| Error::Config(error.to_string()))?;
            let rack_id = RackKey::from_path(&self.key)
                .map_err(|error| Error::Config(error.to_string()))?
                .rack_id;
            let expected_nodes: Vec<_> = ctx
                .config()
                .nodes
                .iter()
                .filter(|node| node.rack_id == rack_id)
                .map(|node| node.id)
                .collect();
            return Ok(actual.name == intended.name
                && actual.node_ids.iter().all(|node| expected_nodes.contains(node)));
        }
        if self.key.starts_with("/hw/node/") {
            let intended: NodeValue = serde_json::from_value(self.value.clone())
                .map_err(|error| Error::Config(error.to_string()))?;
            let actual: NodeValue =
                serde_json::from_value(actual.clone()).map_err(|error| Error::Config(error.to_string()))?;
            return Ok(actual.management_host == intended.management_host
                && actual.ssh_port == intended.ssh_port
                && actual.ssh_user == intended.ssh_user
                && actual.ssh_credential_ref == intended.ssh_credential_ref);
        }
        Ok(actual == &self.value)
    }

    async fn create(&self, ctx: &OpContext) -> Result<()> {
        let payload = serde_json::to_vec(&self.value).map_err(|error| Error::Config(error.to_string()))?;
        match ctx.kv().put_cas(0, 0, self.key.as_bytes(), &payload, 0).await {
            Ok(_) => Ok(()),
            Err(error @ (KvError::CasFailed { .. } | KvError::OutcomeUnknown)) => {
                if self.confirmed(ctx).await? {
                    Ok(())
                } else {
                    Err(error.into())
                }
            }
            Err(error) => Err(error.into()),
        }
    }
}

pub(super) async fn write_topology_to_sysdata(
    ctx: &OpContext,
    store_nodes: &[u64],
    members: &[(u64, u64)],
) -> Result<()> {
    let records = intended_records(ctx, store_nodes, members)?;
    let mut missing = Vec::new();
    // Prove existing content before writing any missing record. An interrupted
    // attempt can resume, but an initialized cluster is never overwritten.
    for record in &records {
        if !record.confirmed(ctx).await? {
            missing.push(record);
        }
    }
    for record in missing {
        record.create(ctx).await?;
    }
    for record in &records {
        if !record.confirmed(ctx).await? {
            return Err(Error::NotFound {
                kind: "confirmed bootstrap metadata".into(),
                id: record.key.clone(),
            });
        }
    }
    Ok(())
}

fn intended_records(ctx: &OpContext, store_nodes: &[u64], members: &[(u64, u64)]) -> Result<Vec<Record>> {
    let config = ctx.config();
    let mut records = Vec::new();
    for rack in &config.racks {
        records.push(Record::new(
            &RackKey { rack_id: rack.id },
            RackValue {
                status: HwStatus::Up as i32,
                node_ids: Vec::new(),
                name: rack.name.clone(),
            },
        )?);
    }
    for node in &config.nodes {
        records.push(Record::new(
            &NodeKey {
                rack_id: node.rack_id,
                node_id: node.id,
            },
            NodeValue {
                status: HwStatus::Up as i32,
                management_host: node.host.clone(),
                ssh_port: node.ssh_port,
                ssh_user: node.ssh_user.clone(),
                ssh_credential_ref: node.ssh_credential_ref.clone(),
                ..Default::default()
            },
        )?);
    }
    records.push(Record::new(
        &KvStoreKey { store_id: 0 },
        StoreValue {
            store_id: 0,
            node_ids: store_nodes.to_vec(),
        },
    )?);
    records.push(Record::new(
        &KvGroupKey {
            store_id: 0,
            group_id: 0,
        },
        GroupValue {
            store_id: 0,
            group_id: 0,
        },
    )?);
    for (node, replica) in members {
        records.push(Record::new(
            &KvReplicaKey {
                store_id: 0,
                group_id: 0,
                replica_id: *replica,
            },
            ReplicaValue {
                store_id: 0,
                group_id: 0,
                replica_id: *replica,
                node_id: *node,
                role: String::new(),
                voting: true,
                endpoint: config
                    .server_for_node(*node)
                    .and_then(|server| server.rpc_url.clone())
                    .unwrap_or_default(),
            },
        )?);
    }
    Ok(records)
}
