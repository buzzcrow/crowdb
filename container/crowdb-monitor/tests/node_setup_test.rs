// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{DeploymentProfile, NodeIdentity, NodeSetupForTests};
use crowdb_protocol::mgmt::node::setup::{NodeSetupPhase, NodeSetupRequest, SetupStorage};
use std::path::Path;
#[path = "common/node_setup.rs"]
mod node_setup;

fn request(root: &Path) -> NodeSetupRequest {
    NodeSetupRequest {
        version: 1,
        discovery_id: NodeIdentity::load_or_create(root).unwrap().uuid().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: crowdb_protocol::mgmt::node::setup::NodeSetupChoices::default(),
    }
}

fn authorize(root: &Path, request: &NodeSetupRequest) {
    // This same-user fixture represents the existing SSH admission proof.
    std::fs::create_dir_all(root.join("ssh")).unwrap();
    std::fs::write(
        root.join("ssh/authorized_keys"),
        format!("ssh-ed25519 fixture crowdb:{}\n", request.operation_id),
    )
    .unwrap();
}

#[test]
fn setup_requires_target_identity_and_authenticated_admission_then_replays_read_only() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-setup-accept");
    let root = directory.path();
    let intent = request(root);
    let mut setup = NodeSetupForTests::default();
    assert!(setup.accept_for_tests(root, intent.clone()).is_err());
    assert!(!root.join("node-setup.json").exists());
    authorize(root, &intent);
    let status = setup.accept_for_tests(root, intent.clone()).unwrap();
    assert_eq!(status.phase, NodeSetupPhase::Accepted);
    let bytes = std::fs::read(root.join("node-setup.json")).unwrap();
    let mut recreated = NodeSetupForTests::default();
    assert_eq!(recreated.accept_for_tests(root, intent.clone()).unwrap(), status);
    assert_eq!(std::fs::read(root.join("node-setup.json")).unwrap(), bytes);
    let mut changed = intent.clone();
    changed.choices.storage = SetupStorage::Diskless;
    assert!(recreated.accept_for_tests(root, changed).is_err());
    let mut wrong_node = intent.clone();
    wrong_node.discovery_id = uuid::Uuid::new_v4().to_string();
    assert!(recreated.accept_for_tests(root, wrong_node).is_err());
    std::fs::write(
        root.join(format!("ssh/cancelled-{}.json", intent.operation_id)),
        "{}",
    )
    .unwrap();
    assert!(recreated.accept_for_tests(root, intent).is_err());
    assert_eq!(std::fs::read(root.join("node-setup.json")).unwrap(), bytes);
}

#[tokio::test]
async fn unbound_setup_survives_a_worker_restart_without_creating_disks_or_services() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-setup-unbound");
    let root = directory.path();
    let intent = request(root);
    authorize(root, &intent);
    NodeSetupForTests::default()
        .accept_for_tests(root, intent.clone())
        .unwrap();
    let profile = profile(root);
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    NodeSetupForTests::advance_for_tests(profile).await.unwrap();
    let status = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(status.phase, NodeSetupPhase::WaitingForBinding);
    assert_eq!(status.operation_id, intent.operation_id);
    assert!(status.node_id.is_none() && status.disks.is_empty());
    assert!(!root.join("disks").exists());
    assert!(!root.join("node-binding.json").exists());
    assert!(status.services.iter().all(|step| step.operation_id.is_none()));
}

#[tokio::test]
async fn diskless_setup_requires_the_exact_confirmed_admission_and_does_not_start_services() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-setup-diskless");
    let root = directory.path();
    let mut intent = request(root);
    intent.choices.storage = SetupStorage::Diskless;
    for service in &mut intent.choices.services {
        service.enabled = false;
    }
    authorize(root, &intent);
    NodeSetupForTests::default()
        .accept_for_tests(root, intent.clone())
        .unwrap();
    let mut authority = node_setup::TestAuthority::start(root, &intent).await;
    let profile = profile(root);
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    let status = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(status.phase, NodeSetupPhase::Ready);
    assert_eq!(status.node_id, Some(2));
    assert_eq!(
        status.cluster_id.as_ref(),
        Some(&authority.binding.bootstrap.cluster_id)
    );
    assert!(!root.join("disks").exists());
    assert!(status
        .services
        .iter()
        .all(|service| service.operation_id.is_none()));
    authority.registry.nodes[0].operation_id = uuid::Uuid::new_v4().to_string();
    authority
        .kv
        .put(
            0,
            0,
            crowdb_console_shared::deployment::registry::NODES_KEY,
            &serde_json::to_vec(&authority.registry).unwrap(),
            None,
        )
        .await
        .unwrap();
    NodeSetupForTests::advance_for_tests(profile).await.unwrap();
    let rejected = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(rejected.phase, NodeSetupPhase::Failed);
    assert!(rejected.error.unwrap().contains("confirmed admission"));
    assert!(!root.join("disks").exists());
}

#[tokio::test]
async fn explicit_cleanup_retires_only_the_matching_setup_and_never_replays_it() {
    use std::os::unix::fs::PermissionsExt;
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-setup-cleanup");
    let root = directory.path();
    let intent = request(root);
    authorize(root, &intent);
    let mut setup = NodeSetupForTests::default();
    setup.accept_for_tests(root, intent.clone()).unwrap();
    let authority = node_setup::TestAuthority::start(root, &intent).await;
    let accepted = root.join("accepted-node.json");
    std::fs::write(&accepted, serde_json::to_vec(&serde_json::json!({"node_id":2,"bootstrap":authority.binding.bootstrap,"prepared":true,"admission":null})).unwrap()).unwrap();
    std::fs::set_permissions(&accepted, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(setup
        .prepare_cleanup_for_tests(root, &authority.binding.bootstrap, false)
        .is_err());
    let mut wrong = authority.binding.bootstrap.clone();
    wrong.cluster_id = uuid::Uuid::new_v4().to_string();
    assert!(setup.prepare_cleanup_for_tests(root, &wrong, true).is_err());
    assert!(root.join("node-setup.json").exists());
    setup
        .prepare_cleanup_for_tests(root, &authority.binding.bootstrap, true)
        .unwrap();
    assert!(!root.join("node-setup.json").exists());
    assert!(root
        .join(format!("retired-node-setup-{}.json", intent.operation_id))
        .exists());
    assert!(setup.accept_for_tests(root, intent.clone()).is_err());
    assert!(NodeSetupForTests::status_for_tests(root).unwrap().is_none());
    let mut next = intent;
    next.operation_id = uuid::Uuid::new_v4().to_string();
    authorize(root, &next);
    setup.accept_for_tests(root, next).unwrap();
}

#[tokio::test]
async fn default_file_waits_for_metadata_then_reuses_one_durable_request_and_group() {
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
    use crowdb_protocol::{
        common::{GroupValue, NodeValue, RackValue},
        key::{KvGroupKey, TextKey},
    };
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-setup-default");
    let root = directory.path();
    let mut intent = request(root);
    for service in &mut intent.choices.services {
        service.enabled = false;
    }
    authorize(root, &intent);
    NodeSetupForTests::default()
        .accept_for_tests(root, intent.clone())
        .unwrap();
    let authority = node_setup::TestAuthority::start(root, &intent).await;
    let hardware = HardwareClient::new(CrowdbKvClient::new(ClientConfig::new(
        authority.binding.management_seeds.clone(),
    )));
    hardware
        .add_rack(
            1,
            &RackValue {
                node_ids: vec![2],
                ..RackValue::default()
            },
        )
        .await
        .unwrap();
    hardware.add_node(1, 2, &NodeValue::default()).await.unwrap();
    let mut profile = profile(root);
    profile.paths.run_root = root.join("run");
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    let waiting = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(waiting.phase, NodeSetupPhase::Running);
    assert!(waiting.error.unwrap().contains("ordinary metadata group"));
    assert!(hardware.list_disk_groups_on_node(1, 2).await.unwrap().is_empty());
    assert!(!root.join("disks").exists());
    authority
        .kv
        .put(
            0,
            0,
            KvGroupKey {
                store_id: 0,
                group_id: 1,
            }
            .to_path()
            .as_bytes(),
            &serde_json::to_vec(&GroupValue {
                store_id: 0,
                group_id: 1,
            })
            .unwrap(),
            None,
        )
        .await
        .unwrap();
    std::fs::create_dir(&profile.paths.run_root).unwrap();
    let listener = tokio::net::UnixListener::bind(profile.paths.run_root.join("node-control.sock")).unwrap();
    let observe = tokio::spawn(observe_file_requests(listener));
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    NodeSetupForTests::advance_for_tests(profile).await.unwrap();
    let requests = observe.await.unwrap();
    assert_eq!(requests[0], requests[1]);
    assert_eq!(requests[0].capacity_bytes, 16 * 1024 * 1024 * 1024);
    assert_eq!(requests[0].reserve_bytes, 1024 * 1024 * 1024);
    assert!(!requests[0].sparse);
    let placement = requests[0].admission.as_ref().unwrap();
    let groups = hardware.list_disk_groups_on_node(1, 2).await.unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].dg_id, placement.disk_group_id);
    assert!(
        groups[0].value.disk_ids.is_empty(),
        "preparation acceptance is not disk admission"
    );
    let binding = hardware
        .get_bind(1, 2, placement.disk_group_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((binding.store_id, binding.group_id), (0, 1));
}

fn profile(root: &Path) -> DeploymentProfile {
    let mut profile = DeploymentProfile::load(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml"),
    )
    .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile
}

async fn observe_file_requests(
    listener: tokio::net::UnixListener,
) -> Vec<crowdb_protocol::mgmt::node::storage::PrepareDiskFile> {
    use crowdb_protocol::mgmt::node::NodeControl;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut requests = Vec::new();
    for _ in 0..2 {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        let NodeControl::PrepareDiskFile { request } = serde_json::from_slice(&bytes).unwrap() else {
            panic!("unexpected setup control");
        };
        requests.push(request);
        stream.write_all(br#"{"ok":true,"value":{}}"#).await.unwrap();
    }
    requests
}
