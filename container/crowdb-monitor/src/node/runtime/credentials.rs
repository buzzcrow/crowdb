// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{authorize_bound, durable, AcceptedNode, Result};
use crowdb_protocol::mgmt::{
    node::{NodeBinding, NodeServiceCredentials},
    SystemBootstrapIdentity,
};
use serde_json::{json, Value};
use std::path::Path;

pub(super) fn provision(
    root: &Path,
    bootstrap: &SystemBootstrapIdentity,
    credentials: &NodeServiceCredentials,
) -> Result<Value> {
    let accepted: AcceptedNode =
        durable::read(&root.join("accepted-node.json"))?.ok_or("node not prepared")?;
    if &accepted.bootstrap != bootstrap
        || root
            .join(format!(
                "retired-bootstrap-{}.json",
                accepted.bootstrap.operation_id
            ))
            .exists()
    {
        return Err("credentials belong to another or retired operation".into());
    }
    crate::ServerCredentials::import(root, &credentials.environment)?;
    Ok(json!({}))
}

pub(super) async fn read(root: &Path, cluster: &str) -> Result<Value> {
    let binding: NodeBinding = durable::read(&root.join("node-binding.json"))?.ok_or("node unbound")?;
    if binding.bootstrap.cluster_id != cluster {
        return Err("cluster identity differs".into());
    }
    authorize_bound(root).await?;
    Ok(json!({"environment": crate::ServerCredentials::load_existing(root)?.server_env()}))
}
