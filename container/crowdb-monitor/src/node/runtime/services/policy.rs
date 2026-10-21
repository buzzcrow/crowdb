// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;

use crowdb_protocol::mgmt::node::{deployment::DeploymentMode, NodeServiceAction, NodeServiceIntent};

use super::Result;

pub(super) fn validate(root: &Path, intent: &NodeServiceIntent) -> Result<()> {
    if !matches!(
        intent.action,
        NodeServiceAction::Start | NodeServiceAction::Restart
    ) {
        return Ok(());
    }
    let Some(policy) = crate::node::read_node_policy(root)? else {
        return Ok(());
    };
    if policy.mode != DeploymentMode::Production {
        return Ok(());
    }
    let config: toml::Value =
        toml::from_str(&intent.configuration).map_err(|_| "invalid managed service configuration")?;
    if matches!(intent.kind.as_str(), "chunkdb" | "access") {
        let mode = config
            .get("deployment")
            .and_then(|deployment| deployment.get("mode"))
            .and_then(toml::Value::as_str)
            .unwrap_or("production");
        if mode != "production" {
            return Err("production node policy forbids local or unsafe service deployment".into());
        }
    }
    if intent.kind == "chunk-kv"
        && config
            .get("storage")
            .and_then(|storage| storage.get("stream_mirror_copies"))
            .and_then(toml::Value::as_integer)
            .is_some_and(|copies| copies < 2)
    {
        return Err("production node policy requires protected Chunk-KV streams".into());
    }
    Ok(())
}
