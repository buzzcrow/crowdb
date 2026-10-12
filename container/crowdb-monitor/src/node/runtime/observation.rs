// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;

use crowdb_protocol::mgmt::node::NodeBinding;
use serde_json::{json, Value};

use super::{durable, Result};
use crate::ProcessManager;

pub(super) fn processes(
    root: &Path,
    manager: &mut ProcessManager,
    cluster: &str,
    node: u64,
) -> Result<Value> {
    let binding: NodeBinding = durable::read(&root.join("node-binding.json"))?.ok_or("node is unbound")?;
    if binding.node_id != node || binding.bootstrap.cluster_id != cluster {
        return Err("process observation belongs to another node or cluster".into());
    }
    // This authenticated read observes only local children. It needs no live
    // Group-0 grant and remains available while that authority is unavailable.
    Ok(json!({"cluster_id":cluster,"node_id":node,"processes":manager.live_pids()?}))
}
