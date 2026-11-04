// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Completed stop receipts observe a prior effect; they never execute one.

use super::{
    durable, DeploymentProfile, Execution, NodeServiceAction, NodeServiceIntent, ProcessManager, Result,
};
use crowdb_protocol::mgmt::node::NodeBinding;

pub(super) fn completed_kv_stop(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    binding: &NodeBinding,
    intent: &NodeServiceIntent,
    previous: Option<&Execution>,
) -> Result<bool> {
    let Some(previous) = previous else {
        return Ok(false);
    };
    if intent.kind != "kv"
        || !matches!(intent.action, NodeServiceAction::Stop | NodeServiceAction::Delete)
        || previous.intent != *intent
        || previous.bootstrap.as_ref() != Some(&binding.bootstrap)
        || !previous.complete
        || previous.pid.is_some()
        || processes.owns("kv") && processes.alive("kv")?
    {
        return Ok(false);
    }
    let root = &profile.paths.data_root;
    let Some(accepted) = durable::read::<super::super::AcceptedNode>(&root.join("accepted-node.json"))?
    else {
        return Ok(false);
    };
    Ok(accepted.prepared
        && accepted.node_id == binding.node_id
        && accepted.bootstrap == binding.bootstrap
        && !super::super::admission::is_cancelled(root, &accepted)
        && !root
            .join(format!(
                "retired-bootstrap-{}.json",
                binding.bootstrap.operation_id
            ))
            .exists())
}
