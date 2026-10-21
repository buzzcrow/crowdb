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
    accepted.identity.configuration_digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&(&accepted.intent, &accepted.nodes)).unwrap(),
    ));
    let mut trust = recovery::trust(&KeyPair::generate_ed25519().unwrap());
    trust.bootstrap = accepted.identity.clone();
    trust.members[0]
        .discovery_id
        .clone_from(&accepted.nodes[0].discovery_id);
    trust.members[0]
        .admission_operation_id
        .clone_from(&accepted.nodes[0].operation_id);
    trust.validate_bootstrap(&accepted).unwrap();
    let mut changed = trust.clone();
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
    std::env::set_var("CROWDB_KV_KNOWN_HOSTS", &address_cache);
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
