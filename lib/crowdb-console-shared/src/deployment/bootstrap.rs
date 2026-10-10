// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::bootstrap_intent::BootstrapIntent;
use crate::config::ConsoleConfig;
use crate::error::{Error, Result};
use crate::ops::OpContext;

const MAX_BYTES: u64 = 1024 * 1024;
pub const CLUSTER_KEY: &[u8] = b"/deployment/cluster";

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedBootstrap {
    pub identity: SystemBootstrapIdentity,
    pub intent: BootstrapIntent,
    pub nodes: Vec<super::registry::NodeRecord>,
}

impl PreparedBootstrap {
    /// Load fixed inputs on retry, or durably publish a new operation before contacting peers.
    ///
    /// # Errors
    /// Rejects corrupt state, changed members and persistence failures.
    pub fn open(path: &Path, config: &ConsoleConfig, members: &[u64]) -> Result<Self> {
        Self::open_with_nodes(path, config, members, Vec::new())
    }

    /// # Errors
    /// Rejects changed members or malformed durable operation inputs.
    pub fn open_with_nodes(
        path: &Path,
        config: &ConsoleConfig,
        members: &[u64],
        mut nodes: Vec<super::registry::NodeRecord>,
    ) -> Result<Self> {
        if fs::symlink_metadata(path).is_ok() {
            let existing = Self::load(path)?;
            if existing.intent.members() != members {
                return Err(Error::Conflict {
                    kind: "bootstrap members".into(),
                    id: path.display().to_string(),
                });
            }
            return Ok(existing);
        }
        let mut config = config.clone();
        config.nodes.retain(|node| members.contains(&node.id));
        config
            .servers
            .retain(|server| server.node_id.is_some_and(|node| members.contains(&node)));
        config
            .racks
            .retain(|rack| config.nodes.iter().any(|node| node.rack_id == rack.id));
        nodes.retain(|node| members.contains(&node.node_id));
        nodes.sort_by_key(|node| node.node_id);
        let intent = BootstrapIntent::capture(&config, members)?;
        let bytes = serde_json::to_vec(&(&intent, &nodes)).map_err(config_error)?;
        let operation = Self {
            identity: SystemBootstrapIdentity {
                cluster_id: Uuid::new_v4().to_string(),
                operation_id: Uuid::new_v4().to_string(),
                configuration_digest: hex::encode(Sha256::digest(bytes)),
            },
            intent,
            nodes,
        };
        let parent = path
            .parent()
            .ok_or_else(|| Error::Config("operation has no parent".into()))?;
        fs::create_dir_all(parent)?;
        let temp = parent.join(format!(".bootstrap-{}.tmp", operation.identity.operation_id));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temp)?;
            file.write_all(&serde_json::to_vec(&operation).map_err(config_error)?)?;
            file.sync_all()?;
            match fs::hard_link(&temp, path) {
                Ok(()) => File::open(parent)?.sync_all()?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            Self::load(path)
        })();
        let cleanup = fs::remove_file(temp);
        if result.is_ok() {
            cleanup?;
        }
        let winner = result?;
        if winner.intent != operation.intent || winner.nodes != operation.nodes {
            return Err(Error::Conflict {
                kind: "bootstrap inputs".into(),
                id: path.display().to_string(),
            });
        }
        Ok(winner)
    }

    /// # Errors
    /// Rejects symlinks, permissions, oversized content and invalid identity/digest.
    pub fn load(path: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o777 != 0o600 || metadata.len() > MAX_BYTES
        {
            return Err(Error::Config("invalid prepared bootstrap file".into()));
        }
        let mut bytes = Vec::new();
        File::open(path)?.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
        let operation: Self = serde_json::from_slice(&bytes).map_err(config_error)?;
        operation.validate()?;
        Ok(operation)
    }

    /// Validate the fixed topology and its public configuration digest.
    ///
    /// # Errors
    /// Rejects invalid identity, altered inputs and malformed topology.
    pub fn validate(&self) -> Result<()> {
        for id in [&self.identity.cluster_id, &self.identity.operation_id] {
            if Uuid::parse_str(id).map_err(config_error)?.is_nil() {
                return Err(Error::Config("nil bootstrap identity".into()));
            }
        }
        let digest = hex::encode(Sha256::digest(
            serde_json::to_vec(&(&self.intent, &self.nodes)).map_err(config_error)?,
        ));
        if digest != self.identity.configuration_digest {
            return Err(Error::Config("bootstrap configuration digest differs".into()));
        }
        BootstrapIntent::capture(&self.intent.to_config(), self.intent.members())?;
        Ok(())
    }

    /// Resume only the sealed inputs, then confirm cluster identity through Group 0.
    ///
    /// # Errors
    /// Propagates preparation, consensus, publication or cluster identity conflicts.
    pub async fn execute(&self, ctx: &OpContext) -> Result<crate::ops::cluster::InitSummary> {
        *ctx.config_mut() = self.intent.to_config();
        let summary = crate::ops::cluster::init_prepared(ctx, self.intent.members(), &self.identity).await?;
        let bytes = serde_json::to_vec(self).map_err(config_error)?;
        match ctx.kv().put_cas(0, 0, CLUSTER_KEY, &bytes, 0).await {
            Ok(_) => {}
            Err(error) => match ctx
                .kv()
                .get(0, 0, CLUSTER_KEY, ReadMode::Linearizable, None)
                .await?
            {
                GetOutcome::Found { value, .. } if value.as_ref() == bytes.as_slice() => {}
                _ => return Err(error.into()),
            },
        }
        let mut nodes = self.nodes.clone();
        for node in &mut nodes {
            node.confirmed = true;
        }
        super::registry::publish(
            ctx.kv(),
            &super::registry::NodeRegistry {
                cluster_id: self.identity.cluster_id.clone(),
                nodes,
            },
        )
        .await?;
        Ok(summary)
    }
}

fn config_error(error: impl std::fmt::Display) -> Error {
    Error::Config(error.to_string())
}
