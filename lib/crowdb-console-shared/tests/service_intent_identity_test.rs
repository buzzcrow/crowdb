// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "../src/deployment/services/identity.rs"]
mod identity;

use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};

fn intent(action: NodeServiceAction) -> NodeServiceIntent {
    NodeServiceIntent {
        cluster_id: "cluster".into(),
        operation_id: "first-operation".into(),
        node_id: 1,
        service_id: "chunk-kv-1".into(),
        kind: "chunk-kv".into(),
        action,
        configuration: "instance_id = 1".into(),
        environment: std::collections::BTreeMap::new(),
    }
}

#[test]
fn restart_replay_preserves_identity_but_another_restart_executes() {
    let current = intent(NodeServiceAction::Restart);
    assert!(identity::replay(&current, &current));
    let mut another = current.clone();
    another.operation_id = "next-operation".into();
    assert!(!identity::replay(&current, &another));
    assert_eq!(another.operation_id, "next-operation");
    assert_eq!(current.operation_id, "first-operation");
}

#[test]
fn repeated_desired_state_resumes_its_existing_operation() {
    for action in [
        NodeServiceAction::Start,
        NodeServiceAction::Stop,
        NodeServiceAction::Delete,
    ] {
        let current = intent(action);
        let mut repeated = current.clone();
        repeated.operation_id = "next-operation".into();
        assert!(identity::replay(&current, &repeated));
    }
}

#[test]
fn changed_target_action_configuration_or_environment_is_never_a_replay() {
    let current = intent(NodeServiceAction::Start);
    for field in [
        "cluster",
        "node",
        "service",
        "kind",
        "action",
        "configuration",
        "environment",
    ] {
        let mut changed = current.clone();
        match field {
            "cluster" => changed.cluster_id = "other-cluster".into(),
            "node" => changed.node_id = 2,
            "service" => changed.service_id = "chunk-kv-2".into(),
            "kind" => changed.kind = "chunkdb".into(),
            "action" => changed.action = NodeServiceAction::Stop,
            "configuration" => changed.configuration.push_str("\nnode_id = 2"),
            "environment" => {
                changed.environment.insert("CROWDB_LOG".into(), "info".into());
            }
            _ => unreachable!(),
        }
        assert!(!identity::replay(&current, &changed), "{field}");
    }
}
