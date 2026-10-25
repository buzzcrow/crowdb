// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{NodeServiceAction, NodeServiceIntent, ProcessObservations, ServerEntry};
use serde_json::json;

#[test]
fn accepted_start_waits_until_owned_pid_or_current_operation_exhaustion() {
    let intent = NodeServiceIntent {
        node_id: 2,
        service_id: "chunkdb-7".into(),
        kind: "chunkdb".into(),
        cluster_id: "cluster".into(),
        operation_id: "current".into(),
        action: NodeServiceAction::Start,
        configuration: String::new(),
        environment: std::collections::BTreeMap::new(),
    };
    let mut entry = ServerEntry::new("chunkdb-7", "http://127.0.0.1:7300");
    entry.node_id = Some(2);
    entry.auto_start = true;
    for (operation, exhausted, pending) in [
        ("old", true, true),
        ("current", false, true),
        ("current", true, false),
    ] {
        let mut observation = ProcessObservations::default();
        observation.record(
            2,
            &json!({"processes":{}, "service_failures": {
                "chunkdb-7": {"operation_id": operation, "exhausted": exhausted}
            }}),
            std::slice::from_ref(&intent),
        );
        assert_eq!(observation.start_pending(&entry, true, None), pending);
        assert!(!observation.start_pending(&entry, true, Some(42)));
        assert!(!observation.start_pending(&entry, false, None));
    }
    entry.auto_start = false;
    assert!(!ProcessObservations::default().start_pending(&entry, true, None));
}
