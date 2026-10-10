// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{durable, AcceptedNode, Result};
use crate::NodeIdentity;
use crowdb_console_shared::deployment::PreparedBootstrap;
use crowdb_protocol::mgmt::SystemBootstrapIdentity;
use serde_json::Value;
use std::path::Path;

pub(super) fn accept(
    root: &Path,
    node_id: u64,
    bootstrap: SystemBootstrapIdentity,
    manifest: Option<Value>,
    credentials: Option<crowdb_protocol::mgmt::node::NodeServiceCredentials>,
    admission: Option<crowdb_protocol::mgmt::node::NodeAdmissionGrant>,
) -> Result<AcceptedNode> {
    if manifest.is_none() && admission.is_none() {
        return Err("bootstrap manifest or current admission grant required".into());
    }
    if node_id == 0
        || uuid::Uuid::parse_str(&bootstrap.operation_id)?.is_nil()
        || uuid::Uuid::parse_str(&bootstrap.cluster_id)?.is_nil()
        || bootstrap.configuration_digest.len() != 64
        || !bootstrap
            .configuration_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("invalid bootstrap acceptance".into());
    }
    if root
        .join(format!("retired-bootstrap-{}.json", bootstrap.operation_id))
        .exists()
    {
        return Err("bootstrap operation is retired".into());
    }
    let manifest = manifest
        .map(serde_json::from_value::<PreparedBootstrap>)
        .transpose()?;
    if let Some(operation) = &manifest {
        operation.validate()?;
        let discovery_id = NodeIdentity::load_or_create(root)?.uuid().to_string();
        if operation.identity != bootstrap
            || !operation.intent.members().contains(&node_id)
            || !operation
                .nodes
                .iter()
                .any(|node| node.node_id == node_id && node.discovery_id == discovery_id)
        {
            return Err("bootstrap manifest does not select this node".into());
        }
    }
    let mut accepted = AcceptedNode {
        node_id,
        bootstrap,
        prepared: false,
        admission,
    };
    let path = root.join("accepted-node.json");
    if let Some(existing) = durable::read::<AcceptedNode>(&path)? {
        if existing.node_id != accepted.node_id
            || existing.bootstrap != accepted.bootstrap
            || existing.admission != accepted.admission
        {
            return Err("node belongs to another bootstrap operation".into());
        }
    } else {
        durable::write(&path, &accepted)?;
    }
    if let Some(credentials) = credentials {
        super::credentials::provision(root, &accepted.bootstrap, &credentials)?;
    }
    if let Some(operation) = manifest {
        let path = root.join("prepared-bootstrap.json");
        if let Some(existing) = durable::read::<PreparedBootstrap>(&path)? {
            if existing != operation {
                return Err("accepted bootstrap manifest differs".into());
            }
        } else {
            durable::write(&path, &operation)?;
        }
    }
    accepted.prepared = true;
    durable::write(&path, &accepted)?;
    Ok(accepted)
}
