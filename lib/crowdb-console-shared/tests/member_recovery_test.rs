// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::os::unix::fs::{symlink, PermissionsExt};

use crowdb_console_shared::deployment::recovery::MemberTrust;
use crowdb_test_harness::test_dirs::tempdir_in_test_data;
use russh::keys::key::KeyPair;

#[path = "common/member_recovery.rs"]
mod recovery;
#[path = "common/ssh_server.rs"]
#[allow(dead_code)]
mod ssh_server;

#[path = "common/member_evidence.rs"]
#[allow(dead_code)]
mod member_evidence;

#[test]
fn trust_must_match_the_sealed_bootstrap_members_and_admission() {
    use sha2::{Digest, Sha256};

    let mut accepted = member_evidence::TestMemberEvidence::new().bootstrap;
    let mut trust = recovery::trust(&KeyPair::generate_ed25519().unwrap());
    accepted.nodes[0].host_key = Some(trust.members[0].host_key.clone());
    accepted.identity.configuration_digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&(&accepted.intent, &accepted.nodes)).unwrap(),
    ));
    trust.bootstrap = accepted.identity.clone();
    trust.members[0]
        .discovery_id
        .clone_from(&accepted.nodes[0].discovery_id);
    trust.members[0]
        .admission_operation_id
        .clone_from(&accepted.nodes[0].operation_id);
    trust.validate_bootstrap(&accepted).unwrap();
    let mut changed = trust.clone();
    changed.members[0].host_key = recovery::host_key(&KeyPair::generate_ed25519().unwrap());
    assert!(changed.validate_bootstrap(&accepted).is_err());
    changed = trust.clone();
    changed.members[0].admission_operation_id = uuid::Uuid::new_v4().to_string();
    assert!(changed.validate_bootstrap(&accepted).is_err());
    changed = trust.clone();
    changed.members[0].node_id = 2;
    assert!(changed.validate_bootstrap(&accepted).is_err());
    changed = trust;
    changed.bootstrap.cluster_id = uuid::Uuid::new_v4().to_string();
    assert!(changed.validate_bootstrap(&accepted).is_err());
}

#[test]
fn trust_rejects_duplicate_identity_shared_keys_and_invalid_versions() {
    let key = KeyPair::generate_ed25519().unwrap();
    let accepted = recovery::trust(&key);
    accepted.validate().unwrap();
    let mut changed = accepted.clone();
    changed.version = 2;
    assert!(changed.validate().is_err());
    changed = accepted.clone();
    let mut duplicate = changed.members[0].clone();
    duplicate.node_id = 2;
    duplicate.discovery_id = uuid::Uuid::new_v4().to_string();
    changed.members.push(duplicate);
    assert!(
        changed.validate().is_err(),
        "two member identities cannot share one host key"
    );
    changed.members[1].host_key = recovery::host_key(&KeyPair::generate_ed25519().unwrap());
    changed.validate().unwrap();
    changed.members[1].discovery_id = changed.members[0].discovery_id.clone();
    assert!(changed.validate().is_err());
    changed = accepted;
    changed.members[0].host_key = "ssh-ed25519 invalid".into();
    assert!(changed.validate().is_err());
}

#[test]
fn sealed_trust_is_private_immutable_and_never_relearned() {
    let dir = tempdir_in_test_data("member-trust-seal");
    let path = dir.path().join("trust.json");
    let accepted = recovery::trust(&KeyPair::generate_ed25519().unwrap());
    assert!(MemberTrust::load(&path).is_err());
    accepted.seal(&path).unwrap();
    accepted.seal(&path).unwrap();
    assert_eq!(MemberTrust::load(&path).unwrap(), accepted);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let original = std::fs::read(&path).unwrap();
    let mut changed = accepted.clone();
    changed.members[0].host_key = recovery::host_key(&KeyPair::generate_ed25519().unwrap());
    assert!(changed.seal(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    let link = dir.path().join("linked.json");
    symlink(&path, &link).unwrap();
    assert!(MemberTrust::load(&link).is_err());
    assert!(accepted.seal(&link).is_err());
    std::fs::write(&path, "corrupt").unwrap();
    assert!(accepted.seal(&path).is_err());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "corrupt");
}

#[test]
fn concurrent_sealers_cannot_replace_the_winning_identity() {
    let dir = tempdir_in_test_data("member-trust-race");
    let path = dir.path().join("trust.json");
    let first = recovery::trust(&KeyPair::generate_ed25519().unwrap());
    let mut second = first.clone();
    second.members[0].host_key = recovery::host_key(&KeyPair::generate_ed25519().unwrap());
    std::thread::scope(|scope| {
        let a = scope.spawn(|| first.seal(&path));
        let b = scope.spawn(|| second.seal(&path));
        assert_ne!(a.join().unwrap().is_ok(), b.join().unwrap().is_ok());
    });
    let winner = MemberTrust::load(&path).unwrap();
    assert!(winner == first || winner == second);
}

#[tokio::test]
async fn pinned_identity_survives_endpoint_changes_without_address_tofu() {
    let dir = tempdir_in_test_data("member-recovery-move");
    let address_cache = dir.path().join("address-cache");
    std::fs::write(&address_cache, "deliberately invalid address-keyed trust").unwrap();
    std::env::set_var("CROWDB_PAXOS_KV_KNOWN_HOSTS", &address_cache);
    let key = KeyPair::generate_ed25519().unwrap();
    let host = KeyPair::generate_ed25519().unwrap();
    let trust = recovery::trust(&host);
    let observed = recovery::observation(&trust);
    let reply = recovery::reply(&observed);
    let first = ssh_server::TestSshServer::start_with_host_key(
        key.clone_public_key().unwrap(),
        Some(reply.clone()),
        host.clone(),
    )
    .await;
    let mut node = recovery::node(first.port, &dir.path().join("key"), &key);
    assert_eq!(trust.observe(&node).await.unwrap(), observed);
    drop(first);
    let second =
        ssh_server::TestSshServer::start_with_host_key(key.clone_public_key().unwrap(), Some(reply), host)
            .await;
    node.ssh_port = second.port;
    assert_eq!(trust.observe(&node).await.unwrap(), observed);
    let impostor = ssh_server::TestSshServer::start_with_reply(
        key.clone_public_key().unwrap(),
        Some(recovery::reply(&observed)),
    )
    .await;
    node.ssh_port = impostor.port;
    assert!(
        trust.observe(&node).await.is_err(),
        "self-reported identity cannot override host-key pins"
    );
    assert_eq!(
        std::fs::read_to_string(address_cache).unwrap(),
        "deliberately invalid address-keyed trust"
    );
}

#[tokio::test]
async fn valid_host_key_cannot_claim_another_cluster_or_member() {
    let dir = tempdir_in_test_data("member-recovery-binding");
    let key = KeyPair::generate_ed25519().unwrap();
    let host = KeyPair::generate_ed25519().unwrap();
    let trust = recovery::trust(&host);
    for variant in 0..4 {
        let mut observed = recovery::observation(&trust);
        match variant {
            0 => observed.discovery_id = uuid::Uuid::new_v4().to_string(),
            1 => observed.binding.as_mut().unwrap().node_id = 2,
            2 => observed.binding.as_mut().unwrap().bootstrap.cluster_id = uuid::Uuid::new_v4().to_string(),
            _ => observed.binding = None,
        }
        let server = ssh_server::TestSshServer::start_with_host_key(
            key.clone_public_key().unwrap(),
            Some(recovery::reply(&observed)),
            host.clone(),
        )
        .await;
        let node = recovery::node(server.port, &dir.path().join("key"), &key);
        assert!(
            trust.observe(&node).await.is_err(),
            "variant {variant} must be rejected"
        );
    }
}

#[tokio::test]
async fn authenticated_endpoint_requires_the_contacted_address_and_live_incarnation() {
    use crowdb_protocol::mgmt::node::recovery::{MemberEndpoint, MemberRecoveryIdentity};
    let dir = tempdir_in_test_data("member-current-address");
    let key = KeyPair::generate_ed25519().unwrap();
    let host = KeyPair::generate_ed25519().unwrap();
    let trust = recovery::trust(&host);
    let identity = MemberRecoveryIdentity {
        version: 1,
        binding: recovery::observation(&trust).binding.unwrap(),
        endpoint: MemberEndpoint {
            node_id: 1,
            discovery_id: trust.members[0].discovery_id.clone(),
            boot_id: uuid::Uuid::new_v4().to_string(),
            addresses: vec!["127.0.0.1".parse().unwrap()],
        },
    };
    for variant in 0..4 {
        let mut reply = identity.clone();
        match variant {
            1 => reply.endpoint.addresses = vec!["127.0.0.2".parse().unwrap()],
            2 => reply.endpoint.boot_id = uuid::Uuid::nil().to_string(),
            3 => reply.binding.bootstrap.cluster_id = uuid::Uuid::new_v4().to_string(),
            _ => {}
        }
        let server = ssh_server::TestSshServer::start_with_host_key(
            key.clone_public_key().unwrap(),
            Some(serde_json::to_vec(&reply).unwrap()),
            host.clone(),
        )
        .await;
        let node = recovery::node(server.port, &dir.path().join("key"), &key);
        let result = trust.endpoint(&node).await;
        assert_eq!(result.is_ok(), variant == 0, "variant {variant}: {result:?}");
    }
}

#[tokio::test]
async fn pinned_control_rejects_oversized_malformed_and_password_fallback() {
    let dir = tempdir_in_test_data("member-recovery-reply");
    let key = KeyPair::generate_ed25519().unwrap();
    let host = KeyPair::generate_ed25519().unwrap();
    let trust = recovery::trust(&host);
    for reply in [vec![b'x'; 65537], b"not json".to_vec()] {
        let server = ssh_server::TestSshServer::start_with_host_key(
            key.clone_public_key().unwrap(),
            Some(reply),
            host.clone(),
        )
        .await;
        let node = recovery::node(server.port, &dir.path().join("key"), &key);
        assert!(trust.observe(&node).await.is_err());
    }
    let mut node = recovery::node(1, &dir.path().join("key"), &key);
    node.ssh_password = Some("must-not-be-used".into());
    assert!(trust
        .observe(&node)
        .await
        .unwrap_err()
        .to_string()
        .contains("local key"));
}

#[test]
fn address_rebind_updates_only_owned_internal_listeners() {
    let source = r#"
http_advertise_addr = "192.0.2.1:7400"
[server]
bind_address = "192.0.2.1"
kv_server_mgmt_seeds = ["http://192.0.2.1:10000"]
rpc_listen_addr = "192.0.2.1:7102"
http_listen_addr = "0.0.0.0:7101"
[s3]
listen = "192.0.2.1:9091"
public_uri = "http://192.0.2.1:49091"
[dataset]
listen = "192.0.2.8:9093"
"#;
    let result = crowdb_console_shared::deployment::recovery::rebind_service(
        source,
        "192.0.2.1".parse().unwrap(),
        "192.0.2.2".parse().unwrap(),
        &["http://192.0.2.2:10000".into()],
    )
    .unwrap();
    let config: toml::Value = toml::from_str(&result).unwrap();
    assert_eq!(
        config["server"]["kv_server_mgmt_seeds"][0].as_str(),
        Some("http://192.0.2.2:10000")
    );
    assert_eq!(config["server"]["bind_address"].as_str(), Some("192.0.2.2"));
    assert_eq!(
        config["server"]["rpc_listen_addr"].as_str(),
        Some("192.0.2.2:7102")
    );
    assert_eq!(
        config["server"]["http_listen_addr"].as_str(),
        Some("0.0.0.0:7101")
    );
    assert_eq!(config["s3"]["listen"].as_str(), Some("192.0.2.2:9091"));
    assert_eq!(
        config["s3"]["public_uri"].as_str(),
        Some("http://192.0.2.1:49091")
    );
    assert_eq!(config["dataset"]["listen"].as_str(), Some("192.0.2.8:9093"));
    assert_eq!(
        crowdb_console_shared::deployment::recovery::rebind_service(
            &result,
            "192.0.2.1".parse().unwrap(),
            "192.0.2.2".parse().unwrap(),
            &["http://192.0.2.2:10000".into()],
        )
        .unwrap(),
        result
    );
}

#[test]
fn authenticated_admission_pin_survives_serialization_and_address_changes() {
    let mut node = member_evidence::TestMemberEvidence::new().expected;
    let key = recovery::host_key(&KeyPair::generate_ed25519().unwrap());
    node.pin_host_key(&format!("{key} authenticated-host")).unwrap();
    let accepted = node.host_key.clone();
    node.host = "192.0.2.42".into();
    node.pin_host_key(&format!("{key} updated-comment")).unwrap();
    assert_eq!(node.host_key, accepted);
    let bytes = serde_json::to_vec(&node).unwrap();
    let retained: crowdb_console_shared::deployment::registry::NodeRecord =
        serde_json::from_slice(&bytes).unwrap();
    assert_eq!(retained.host_key, accepted);
    let changed = recovery::host_key(&KeyPair::generate_ed25519().unwrap());
    assert!(node.pin_host_key(&changed).is_err());
    assert!(node.pin_host_key("ssh-ed25519 invalid").is_err());
    assert_eq!(node.host_key, accepted);
}

#[test]
fn confirmed_authority_alone_extends_and_retires_later_member_trust() {
    use crowdb_console_shared::deployment::registry::NodeRegistry;
    let initial = recovery::trust(&KeyPair::generate_ed25519().unwrap());
    let mut node = member_evidence::TestMemberEvidence::new().expected;
    node.discovery_id.clone_from(&initial.members[0].discovery_id);
    node.operation_id
        .clone_from(&initial.members[0].admission_operation_id);
    node.host_key = Some(initial.members[0].host_key.clone());
    node.confirmed = true;
    let mut later = node.clone();
    later.discovery_id = uuid::Uuid::new_v4().to_string();
    later.operation_id = uuid::Uuid::new_v4().to_string();
    later.node_id = 2;
    later.host_key = None;
    later
        .pin_host_key(&recovery::host_key(&KeyPair::generate_ed25519().unwrap()))
        .unwrap();
    later.confirmed = false;
    let mut registry = NodeRegistry {
        cluster_id: initial.bootstrap.cluster_id.clone(),
        nodes: vec![node, later],
    };
    assert_eq!(initial.confirmed_registry(&registry).unwrap(), initial);
    registry.nodes[1].confirmed = true;
    let current = initial.confirmed_registry(&registry).unwrap();
    assert_eq!(current.members.len(), 2);
    assert_eq!(current.members[0], initial.members[0]);
    registry.nodes[1].cancelled = true;
    assert_eq!(initial.confirmed_registry(&registry).unwrap(), initial);
    registry.nodes[1].cancelled = false;
    let accepted = registry.clone();
    for change in [
        "cluster",
        "initial-key",
        "initial-operation",
        "missing-pin",
        "shared-key",
    ] {
        let mut changed = accepted.clone();
        match change {
            "cluster" => changed.cluster_id = uuid::Uuid::new_v4().to_string(),
            "initial-key" => changed.nodes[0].host_key = changed.nodes[1].host_key.clone(),
            "initial-operation" => changed.nodes[0].operation_id = uuid::Uuid::new_v4().to_string(),
            "missing-pin" => changed.nodes[1].host_key = None,
            "shared-key" => changed.nodes[1].host_key = changed.nodes[0].host_key.clone(),
            _ => unreachable!(),
        }
        assert!(initial.confirmed_registry(&changed).is_err(), "{change}");
    }
    assert_eq!(initial.members.len(), 1, "initial sealed trust remains immutable");
}
