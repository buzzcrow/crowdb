// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};

pub(super) fn replay(current: &NodeServiceIntent, proposed: &NodeServiceIntent) -> bool {
    // Restart is an explicit side effect; another request must execute again.
    // Replaying its own durable identity still resumes the recorded execution.
    if matches!(proposed.action, NodeServiceAction::Restart) && proposed.operation_id != current.operation_id
    {
        return false;
    }
    let mut normalized = proposed.clone();
    normalized.operation_id.clone_from(&current.operation_id);
    &normalized == current
}
