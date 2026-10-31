// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::{chunk_kv::Id128, chunk_stream::StreamName, mgmt::SystemBootstrapIdentity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    deployment::PreparedBootstrap,
    error::{Error, Result},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultReplica {
    pub node_id: u64,
    pub replica_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultMetadataGroup {
    pub store_id: u64,
    pub group_id: u64,
    pub replicas: Vec<DefaultReplica>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultStorageBinding {
    pub rack_id: u64,
    pub node_id: u64,
    /// Initial automatic-medium placement. Supplied media retain compatible
    /// per-class placement in the target's sealed journal under this metadata.
    pub disk_group_id: u64,
    pub store_id: u64,
    pub group_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultChunkKvBootstrap {
    pub node_id: u64,
    pub partition_id: Id128,
    pub tree_id: u64,
    pub stream_name: StreamName,
    pub owner_epoch: u64,
    pub metadata_group_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultPreparation {
    pub version: u32,
    pub bootstrap: SystemBootstrapIdentity,
    pub operation_id: String,
    pub metadata: DefaultMetadataGroup,
    pub storage: Vec<DefaultStorageBinding>,
    pub chunk_kv: DefaultChunkKvBootstrap,
    /// Reference only; access secrets are never included in progress/status.
    pub credential_ref: String,
    pub retired: bool,
}

impl DefaultPreparation {
    pub(super) fn new(bootstrap: &PreparedBootstrap) -> Result<Self> {
        Self::with_operation(bootstrap, Uuid::new_v4().to_string())
    }

    fn with_operation(bootstrap: &PreparedBootstrap, operation_id: String) -> Result<Self> {
        let members = bootstrap.intent.members();
        if !matches!(members.len(), 1 | 3 | 5) {
            return Err(Error::Config(
                "default preparation requires one, three or five admitted voters".into(),
            ));
        }
        let config = bootstrap.intent.to_config();
        let (high, low) = Uuid::parse_str(&operation_id)
            .map_err(|_| Error::Config("default preparation operation is invalid".into()))?
            .as_u64_pair();
        let high = (high & i64::MAX as u64).max(1);
        let low = (low & i64::MAX as u64).max(1);
        let mut replicas = Vec::new();
        let mut storage = Vec::new();
        for (index, node_id) in members.iter().enumerate() {
            let node = config
                .node(*node_id)
                .ok_or_else(|| Error::Config("default preparation member is not admitted".into()))?;
            replicas.push(DefaultReplica {
                node_id: *node_id,
                replica_id: index as u64 + 1,
            });
            storage.push(DefaultStorageBinding {
                rack_id: node.rack_id,
                node_id: *node_id,
                disk_group_id: index as u64 + 1,
                store_id: 0,
                group_id: 1,
            });
        }
        Ok(Self {
            version: 1,
            bootstrap: bootstrap.identity.clone(),
            credential_ref: format!("access/default/{operation_id}"),
            operation_id,
            metadata: DefaultMetadataGroup {
                store_id: 0,
                group_id: 1,
                replicas,
            },
            storage,
            chunk_kv: DefaultChunkKvBootstrap {
                node_id: members[0],
                partition_id: Id128 { high, low },
                tree_id: low,
                stream_name: StreamName { high, low: low ^ 1 },
                owner_epoch: 1,
                metadata_group_id: 1,
            },
            retired: false,
        })
    }

    pub(super) fn validate(&self, bootstrap: &PreparedBootstrap) -> Result<()> {
        let operation = Uuid::parse_str(&self.operation_id)
            .map_err(|_| Error::Config("default preparation operation is invalid".into()))?;
        if operation.is_nil() || operation.to_string() != self.operation_id {
            return Err(Error::Config("default preparation operation is invalid".into()));
        }
        let mut expected = Self::with_operation(bootstrap, self.operation_id.clone())?;
        expected.retired = self.retired;
        if self != &expected {
            return Err(Error::Config(
                "default preparation differs from committed membership".into(),
            ));
        }
        Ok(())
    }
}
