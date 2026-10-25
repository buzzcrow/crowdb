// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Extend initial recovery pins only from a current confirmed Group-0 registry.

use super::{invalid, MemberTrust, TrustedMember};
use crate::{deployment::registry::NodeRegistry, error::Result};

impl MemberTrust {
    /// Build the current recovery destinations from confirmed authority.
    ///
    /// The caller must obtain this registry through a current linearizable read.
    /// This projection cannot be used as a cached admission or voting grant.
    ///
    /// # Errors
    /// Rejects another cluster, changed initial identities/keys and unpinned
    /// confirmed members. Pending and cancelled admissions grant no trust.
    pub fn confirmed_registry(&self, registry: &NodeRegistry) -> Result<Self> {
        self.validate()?;
        if registry.cluster_id != self.bootstrap.cluster_id || registry.nodes.len() > 4096 {
            return Err(invalid(
                "recovery registry belongs to another cluster or exceeds its bound",
            ));
        }
        let mut members = Vec::new();
        for node in registry
            .nodes
            .iter()
            .filter(|node| node.confirmed && !node.cancelled)
        {
            if let Some(initial) = self.members.iter().find(|member| member.node_id == node.node_id) {
                if initial.discovery_id != node.discovery_id
                    || initial.admission_operation_id != node.operation_id
                    || node.host_key.as_ref().is_some_and(|key| {
                        super::host_key(key).map_or(true, |key| {
                            initial
                                .key()
                                .map_or(true, |accepted| accepted.fingerprint() != key.fingerprint())
                        })
                    })
                {
                    return Err(invalid("confirmed registry changes an initial recovery pin"));
                }
                members.push(initial.clone());
            } else {
                members.push(TrustedMember {
                    discovery_id: node.discovery_id.clone(),
                    node_id: node.node_id,
                    admission_operation_id: node.operation_id.clone(),
                    host_key: node
                        .host_key
                        .clone()
                        .ok_or_else(|| invalid("confirmed member has no authenticated host key"))?,
                });
            }
        }
        members.sort_by_key(|member| member.node_id);
        let current = Self {
            version: self.version,
            bootstrap: self.bootstrap.clone(),
            members,
        };
        current.validate()?;
        Ok(current)
    }
}
