// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{
    config::NodeEntry,
    deployment::recovery::{MemberTrust, TrustedMember},
};
use crowdb_protocol::mgmt::{
    node::{deployment::NodeDeploymentPolicy, observation::NodeRuntimeObservation, NodeBinding},
    SystemBootstrapIdentity,
};
use russh::keys::{key::KeyPair, PublicKeyBase64};
use std::path::Path;

pub fn host_key(key: &KeyPair) -> String {
    format!(
        "ssh-ed25519 {}",
        key.clone_public_key().unwrap().public_key_base64()
    )
}

pub fn trust(key: &KeyPair) -> MemberTrust {
    MemberTrust {
        version: 1,
        bootstrap: SystemBootstrapIdentity {
            cluster_id: uuid::Uuid::new_v4().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            configuration_digest: "a".repeat(64),
        },
        members: vec![TrustedMember {
            discovery_id: uuid::Uuid::new_v4().to_string(),
            node_id: 1,
            admission_operation_id: uuid::Uuid::new_v4().to_string(),
            host_key: host_key(key),
        }],
    }
}

pub fn observation(trust: &MemberTrust) -> NodeRuntimeObservation {
    NodeRuntimeObservation {
        version: 1,
        discovery_id: trust.members[0].discovery_id.clone(),
        policy: NodeDeploymentPolicy::default(),
        binding: Some(NodeBinding {
            node_id: 1,
            bootstrap: trust.bootstrap.clone(),
            management_seeds: vec!["http://old-address:10000".into()],
        }),
        setup: None,
        observed_at_unix_ms: 0,
        services: vec![],
        protocols: vec![],
    }
}

pub fn reply(observation: &NodeRuntimeObservation) -> Vec<u8> {
    serde_json::to_vec(observation).unwrap()
}

pub fn node(port: u16, path: &Path, key: &KeyPair) -> NodeEntry {
    russh::keys::encode_pkcs8_pem(key, std::fs::File::create(path).unwrap()).unwrap();
    NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: port,
        ssh_user: "operator".into(),
        ssh_key: Some(path.to_string_lossy().into_owned()),
        ssh_password: None,
        ssh_credential_ref: None,
    }
}
