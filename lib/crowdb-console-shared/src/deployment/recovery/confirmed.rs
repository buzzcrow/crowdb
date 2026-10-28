// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Confirmed non-voter recovery contacts retain the original voting authority.

use super::{invalid, MemberTrust, TrustedMember};
use crate::{
    config::{NodeEntry, ServerEntry},
    deployment::{registry::NodeRegistry, PreparedBootstrap},
    error::Result,
    ConsoleConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfirmedMemberTrust {
    pub version: u32,
    pub initial: MemberTrust,
    pub current: MemberTrust,
    pub registry: NodeRegistry,
}

impl MemberTrust {
    /// Read the initial public pins sealed into the authenticated manifest.
    ///
    /// # Errors
    /// Rejects an invalid manifest or any missing authenticated host key.
    pub fn from_bootstrap(operation: &PreparedBootstrap) -> Result<Self> {
        operation.validate()?;
        let mut members = operation
            .nodes
            .iter()
            .map(|node| {
                Ok(TrustedMember {
                    discovery_id: node.discovery_id.clone(),
                    node_id: node.node_id,
                    admission_operation_id: node.operation_id.clone(),
                    host_key: node
                        .host_key
                        .clone()
                        .ok_or_else(|| invalid("bootstrap has no authenticated member key"))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        members.sort_by_key(|member| member.node_id);
        let trust = Self {
            version: 1,
            bootstrap: operation.identity.clone(),
            members,
        };
        trust.validate_bootstrap(operation)?;
        Ok(trust)
    }
}

impl ConfirmedMemberTrust {
    /// # Errors
    /// Requires the current registry and the immutable initial pins to agree.
    pub fn new(initial: MemberTrust, registry: NodeRegistry) -> Result<Self> {
        let current = initial.confirmed_registry(&registry)?;
        Ok(Self {
            version: 1,
            initial,
            current,
            registry,
        })
    }

    /// # Errors
    /// Rejects changed initial votes/pins, pending members and mismatched topology.
    pub fn validate(&self, operation: &PreparedBootstrap) -> Result<()> {
        self.initial.validate_bootstrap(operation)?;
        if self.version != 1
            || self.current != self.initial.confirmed_registry(&self.registry)?
            || self
                .initial
                .members
                .iter()
                .any(|member| !self.current.members.contains(member))
        {
            return Err(invalid(
                "confirmed recovery contacts change the accepted initial authority",
            ));
        }
        Ok(())
    }

    /// Retain original voting/store configuration and add confirmed contact hints.
    ///
    /// # Errors
    /// Rejects malformed identities or an incomplete confirmed registry snapshot.
    pub fn topology(&self, operation: &PreparedBootstrap) -> Result<ConsoleConfig> {
        self.validate(operation)?;
        let mut config = operation.intent.to_config();
        for member in &self.current.members {
            if config.node(member.node_id).is_some() {
                continue;
            }
            let node = self
                .registry
                .nodes
                .iter()
                .find(|node| node.node_id == member.node_id)
                .ok_or_else(|| invalid("confirmed member topology is absent"))?;
            config.nodes.push(NodeEntry {
                id: node.node_id,
                rack_id: node.rack_id,
                host: node.host.clone(),
                ssh_port: node.ssh_port,
                ssh_user: node.ssh_user.clone(),
                ssh_key: None,
                ssh_password: None,
                ssh_credential_ref: None,
            });
            let address = node
                .host
                .parse::<std::net::IpAddr>()
                .map_err(|_| invalid("invalid recovery contact address"))?;
            let mut server = ServerEntry::new(
                format!("kv-{}", node.node_id),
                format!(
                    "http://{}",
                    std::net::SocketAddr::new(address, crowdb_protocol::PAXOS_KV_SERVER_MGMT_BASE)
                ),
            );
            server.node_id = Some(node.node_id);
            server.rpc_url = Some(
                std::net::SocketAddr::new(address, crowdb_protocol::PAXOS_KV_SERVER_RPC_BASE).to_string(),
            );
            config.servers.push(server);
        }
        Ok(config)
    }
}
