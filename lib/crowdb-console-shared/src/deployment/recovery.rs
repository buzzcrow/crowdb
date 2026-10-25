// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Identity-bound trust for rediscovery; addresses are never trust anchors.

mod endpoints;
mod services;
pub use services::rebind_service;
mod confirmed;
mod persistence;
mod registry;
pub use confirmed::ConfirmedMemberTrust;

use std::collections::BTreeSet;

use russh::keys::{key::PublicKey, parse_public_key_base64};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::PreparedBootstrap;
use crate::config::NodeEntry;
use crate::error::{Error, Result};
use crowdb_protocol::mgmt::node::{observation::NodeRuntimeObservation, NodeControl};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;

/// Public identity pins established through authenticated bootstrap/admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberTrust {
    pub version: u32,
    pub bootstrap: SystemBootstrapIdentity,
    pub members: Vec<TrustedMember>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedMember {
    pub discovery_id: String,
    pub node_id: u64,
    pub admission_operation_id: String,
    pub host_key: String,
}

impl MemberTrust {
    /// Read a member's retained identity over SSH pinned independently of its IP.
    ///
    /// # Errors
    /// Rejects unknown members, SSH failures and conflicting durable identities.
    pub async fn observe(&self, node: &NodeEntry) -> Result<NodeRuntimeObservation> {
        self.validate()?;
        let member = self
            .members
            .iter()
            .find(|member| member.node_id == node.id)
            .ok_or_else(|| invalid("node is not an accepted recovery member"))?;
        let value =
            super::admission::remote_control_pinned(node, member, &NodeControl::RuntimeObservation).await?;
        let observed: NodeRuntimeObservation =
            serde_json::from_value(value).map_err(|_| invalid("invalid authenticated member observation"))?;
        if observed.version != 1
            || observed.discovery_id != member.discovery_id
            || !observed.binding.as_ref().is_some_and(|binding| {
                binding.node_id == member.node_id && binding.bootstrap == self.bootstrap
            })
        {
            return Err(invalid("authenticated peer differs from accepted member binding"));
        }
        Ok(observed)
    }

    /// Validate pins against the exact accepted initial membership.
    ///
    /// # Errors
    /// Rejects mismatched, missing, duplicate or malformed identity bindings.
    pub fn validate_bootstrap(&self, accepted: &PreparedBootstrap) -> Result<()> {
        accepted.validate()?;
        self.validate()?;
        if self.bootstrap != accepted.identity
            || self.members.len() != accepted.nodes.len()
            || self.members.len() != accepted.intent.members().len()
        {
            return Err(invalid("member trust differs from accepted bootstrap"));
        }
        for member in &self.members {
            let expected = member.key()?;
            if !accepted.intent.members().contains(&member.node_id)
                || !accepted.nodes.iter().any(|node| {
                    node.node_id == member.node_id
                        && node.discovery_id == member.discovery_id
                        && node.operation_id == member.admission_operation_id
                        && !node.cancelled
                        && node.host_key.as_ref().map_or(true, |key| {
                            host_key(key).is_ok_and(|key| key.fingerprint() == expected.fingerprint())
                        })
                })
            {
                return Err(invalid("member trust changes accepted membership"));
            }
        }
        Ok(())
    }

    /// # Errors
    /// Rejects unsupported versions, invalid identities and shared host keys.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 || self.members.is_empty() || self.members.len() > 4096 {
            return Err(invalid("invalid member trust version or size"));
        }
        valid_uuid(&self.bootstrap.cluster_id)?;
        valid_uuid(&self.bootstrap.operation_id)?;
        if self.bootstrap.configuration_digest.len() != 64
            || !self
                .bootstrap
                .configuration_digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid("invalid member trust bootstrap digest"));
        }
        let mut ids = BTreeSet::new();
        let mut uuids = BTreeSet::new();
        let mut keys = BTreeSet::new();
        for member in &self.members {
            valid_uuid(&member.discovery_id)?;
            valid_uuid(&member.admission_operation_id)?;
            let key = member.key()?;
            if member.node_id == 0
                || !ids.insert(member.node_id)
                || !uuids.insert(&member.discovery_id)
                || !keys.insert(key.fingerprint())
            {
                return Err(invalid("duplicate or invalid trusted member identity"));
            }
        }
        Ok(())
    }
}

impl TrustedMember {
    pub(crate) fn key(&self) -> Result<PublicKey> {
        host_key(&self.host_key)
    }
}

pub(crate) fn host_key(text: &str) -> Result<PublicKey> {
    let fields: Vec<_> = text.split_whitespace().collect();
    if text.len() > 4096 || text.lines().count() != 1 || fields.len() < 2 || fields[0] != "ssh-ed25519" {
        return Err(invalid("expected one Ed25519 SSH host key"));
    }
    let key = parse_public_key_base64(fields[1]).map_err(|_| invalid("invalid SSH host key"))?;
    if key.name() != "ssh-ed25519" {
        return Err(invalid("SSH host key algorithm differs"));
    }
    Ok(key)
}

fn valid_uuid(value: &str) -> Result<()> {
    if Uuid::parse_str(value)
        .map_err(|_| invalid("invalid member trust UUID"))?
        .is_nil()
    {
        return Err(invalid("nil member trust UUID"));
    }
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::Config(message.into())
}
