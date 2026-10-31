// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{
    deployment::{
        recovery::{ConfirmedMemberTrust, MemberTrust, TrustedMember},
        registry::{NodeRecord, NodeRegistry},
        PreparedBootstrap,
    },
    ConsoleConfig,
};
use crowdb_monitor::NodeIdentity;
use serde_json::json;
use std::path::Path;

pub fn fixture(root: &Path) -> ConfirmedMemberTrust {
    let identity = NodeIdentity::load_or_create(root).unwrap();
    let mut config = ConsoleConfig::default();
    config
        .racks
        .push(serde_json::from_value(json!({"id":1,"name":"rack"})).unwrap());
    let nodes: Vec<_> = (1_u64..=3).map(|id| {
        config.nodes.push(serde_json::from_value(json!({"id":id,"rack_id":1,"host":format!("192.0.2.{id}"),"ssh_user":"crowdb"})).unwrap());
        config.servers.push(serde_json::from_value(json!({"id":format!("kv-{id}"),"node_id":id,"url":format!("http://192.0.2.{id}:10000"),"rpc_url":format!("192.0.2.{id}:10100")})).unwrap());
        NodeRecord { web_port: 9090, discovery_id: uuid::Uuid::new_v4().to_string(), node_id:id, physical_host_id:format!("host-{id}"),
            rack_id:1, host:format!("192.0.2.{id}"), ssh_port:22, ssh_user:"crowdb".into(),
            operation_id:uuid::Uuid::new_v4().to_string(), confirmed:false, cancelled:false,
            host_key:Some(super::trust::key(u8::try_from(id).unwrap())) }
    }).collect();
    let operation = PreparedBootstrap::open_with_nodes(
        &root.join("prepared-bootstrap.json"),
        &config,
        &[1, 2, 3],
        nodes.clone(),
    )
    .unwrap();
    let initial = MemberTrust::from_bootstrap(&operation).unwrap();
    initial.seal(&root.join("member-trust.json")).unwrap();
    let own = TrustedMember {
        discovery_id: identity.uuid().to_string(),
        node_id: 4,
        admission_operation_id: uuid::Uuid::new_v4().to_string(),
        host_key: super::trust::key(4),
    };
    std::fs::create_dir(root.join("ssh")).unwrap();
    std::fs::write(root.join("ssh/ssh_host_ed25519_key.pub"), &own.host_key).unwrap();
    let mut admitted = initial.clone();
    admitted.members.push(own.clone());
    admitted
        .seal(&root.join(format!(
            "admitted-member-trust-{}.json",
            own.admission_operation_id
        )))
        .unwrap();
    super::trust::write(
        &root.join("accepted-node.json"),
        &json!({"node_id":4,"bootstrap":operation.identity,"prepared":true,
        "admission":{"operation_id":own.admission_operation_id,"management_seeds":["http://192.0.2.1:10000"]}}),
    );
    let mut confirmed = nodes;
    for node in &mut confirmed {
        node.confirmed = true;
    }
    confirmed.push(NodeRecord {
        web_port: 9090,
        discovery_id: own.discovery_id,
        node_id: 4,
        physical_host_id: "host-4".into(),
        rack_id: 1,
        host: "192.0.2.4".into(),
        ssh_port: 22,
        ssh_user: "crowdb".into(),
        operation_id: own.admission_operation_id,
        host_key: Some(own.host_key),
        confirmed: true,
        cancelled: false,
    });
    let snapshot = ConfirmedMemberTrust::new(
        initial,
        NodeRegistry {
            cluster_id: operation.identity.cluster_id,
            nodes: confirmed,
        },
    )
    .unwrap();
    super::trust::write(&root.join("confirmed-member-trust.json"), &snapshot);
    snapshot
}
