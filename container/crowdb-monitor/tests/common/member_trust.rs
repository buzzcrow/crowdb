// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{
    deployment::{
        recovery::{MemberTrust, TrustedMember},
        registry::NodeRecord,
        PreparedBootstrap,
    },
    ConsoleConfig,
};
use crowdb_monitor::NodeIdentity;
use serde_json::json;
use std::{os::unix::fs::PermissionsExt, path::Path};

pub fn write(path: &Path, value: &impl serde::Serialize) {
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

pub fn key(byte: u8) -> String {
    let private = ssh_key::private::Ed25519PrivateKey::from_bytes(&[byte; 32]);
    let public = ssh_key::public::Ed25519PublicKey::from(&private);
    ssh_key::PublicKey::new(ssh_key::public::KeyData::Ed25519(public), "")
        .to_openssh()
        .unwrap()
}

pub fn fixture(root: &Path) -> MemberTrust {
    let identity = NodeIdentity::load_or_create(root).unwrap();
    let ids = [
        identity.uuid().to_string(),
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
    ];
    let mut config = ConsoleConfig::default();
    let mut nodes = Vec::new();
    for id in 1_u64..=3 {
        config.nodes.push(
            serde_json::from_value(
                json!({"id":id,"rack_id":1,"host":format!("192.0.2.{id}"),"ssh_user":"crowdb"}),
            )
            .unwrap(),
        );
        config.servers.push(serde_json::from_value(json!({"id":format!("kv-{id}"),"node_id":id,"url":format!("http://192.0.2.{id}:10000"),"rpc_url":format!("192.0.2.{id}:10100")})).unwrap());
        nodes.push(NodeRecord {
            discovery_id: ids[usize::try_from(id - 1).unwrap()].clone(),
            node_id: id,
            physical_host_id: "test-host".into(),
            rack_id: 1,
            host: format!("192.0.2.{id}"),
            ssh_port: 22,
            ssh_user: "crowdb".into(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            confirmed: false,
            host_key: None,
            cancelled: false,
        });
    }
    config
        .racks
        .push(serde_json::from_value(json!({"id":1,"name":"rack"})).unwrap());
    let bootstrap =
        PreparedBootstrap::open_with_nodes(&root.join("prepared-bootstrap.json"), &config, &[1, 2, 3], nodes)
            .unwrap();
    write(
        &root.join("accepted-node.json"),
        &json!({"node_id":1,"bootstrap":bootstrap.identity,"prepared":true}),
    );
    std::fs::create_dir(root.join("ssh")).unwrap();
    std::fs::write(root.join("ssh/ssh_host_ed25519_key.pub"), key(1)).unwrap();
    std::fs::write(
        root.join("ssh/known_hosts"),
        format!("192.0.2.2 {}\n192.0.2.3 {}\n", key(2), key(3)),
    )
    .unwrap();
    MemberTrust {
        version: 1,
        bootstrap: bootstrap.identity,
        members: bootstrap
            .nodes
            .into_iter()
            .map(|node| TrustedMember {
                discovery_id: node.discovery_id,
                node_id: node.node_id,
                admission_operation_id: node.operation_id,
                host_key: key(u8::try_from(node.node_id).unwrap()),
            })
            .collect(),
    }
}
