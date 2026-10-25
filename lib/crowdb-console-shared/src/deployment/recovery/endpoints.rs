// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{invalid, valid_uuid, MemberTrust};
use crate::{config::NodeEntry, error::Result};
use crowdb_protocol::mgmt::node::{recovery::MemberRecoveryIdentity, NodeControl};

impl MemberTrust {
    /// Verify durable identity and current addresses over the pinned SSH channel.
    ///
    /// # Errors
    /// Rejects unknown members, foreign bindings, malformed addresses and keys.
    pub async fn endpoint(&self, node: &NodeEntry) -> Result<MemberRecoveryIdentity> {
        self.validate()?;
        let member = self
            .members
            .iter()
            .find(|member| member.node_id == node.id)
            .ok_or_else(|| invalid("unknown recovery member"))?;
        let value =
            super::super::admission::remote_control_pinned(node, member, &NodeControl::RecoveryIdentity)
                .await?;
        let identity: MemberRecoveryIdentity =
            serde_json::from_value(value).map_err(|_| invalid("invalid authenticated recovery identity"))?;
        if identity.version != 1
            || identity.binding.bootstrap != self.bootstrap
            || identity.binding.node_id != member.node_id
            || identity.endpoint.node_id != member.node_id
            || identity.endpoint.discovery_id != member.discovery_id
            || identity.endpoint.addresses.is_empty()
            || identity.endpoint.addresses.len() > 16
            || identity
                .endpoint
                .addresses
                .iter()
                .any(|address| address.is_unspecified() || address.is_multicast())
            || !identity
                .endpoint
                .addresses
                .iter()
                .any(|address| address.to_string() == node.host)
        {
            return Err(invalid(
                "authenticated recovery endpoint differs from accepted member",
            ));
        }
        valid_uuid(&identity.endpoint.boot_id)?;
        Ok(identity)
    }
}
