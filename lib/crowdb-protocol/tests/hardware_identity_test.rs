// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::common::{NodeValue, RackValue};

#[test]
fn hardware_values_preserve_shared_identity_and_read_older_records() {
    let rack: RackValue = serde_json::from_str(r#"{"status":1,"node_ids":[7]}"#).unwrap();
    assert!(rack.name.is_empty());
    let node: NodeValue = serde_json::from_str(
        r#"{"status":1,"last_used_dg_id":0,"disk_group_ids":[],"status_changed_at_ms":0,"temp_failure_since_ms":null}"#,
    )
    .unwrap();
    assert!(node.management_host.is_empty());
    assert_eq!(node.ssh_port, 0);
    assert!(node.ssh_credential_ref.is_none());

    let current = NodeValue {
        management_host: "node.example".into(),
        ssh_port: 2222,
        ssh_user: "operator".into(),
        ssh_credential_ref: Some("node-7".into()),
        ..node
    };
    let round_trip: NodeValue = serde_json::from_slice(&serde_json::to_vec(&current).unwrap()).unwrap();
    assert_eq!(round_trip, current);
}
