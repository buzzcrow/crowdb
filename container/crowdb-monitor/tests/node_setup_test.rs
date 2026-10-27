// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{DeploymentProfile, NodeIdentity, NodeSetupForTests};
use crowdb_protocol::mgmt::node::setup::{NodeSetupPhase, NodeSetupRequest, SetupStorage};
use std::path::Path;
#[path = "common/node_setup.rs"]
mod node_setup;
#[path = "common/node_setup_control.rs"]
mod node_setup_control;
#[path = "common/node_setup_groups.rs"]
mod node_setup_groups;
#[path = "common/run_root.rs"]
mod run_root;

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
fn accepted_deployment_choices_cannot_be_overridden_by_admission_or_a_changed_setup_journal() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-setup-sealed-policy");
    let root = directory.path();
    let inputs = crowdb_protocol::mgmt::node::deployment::NodeDeploymentOverrides::default();
    let policy = crowdb_monitor::accept_node_policy(root, &inputs, &inputs).unwrap();
    let mut intent = request(root);
    intent.choices = policy.setup;
    authorize(root, &intent);
    let mut conflicting = intent.clone();
    conflicting.choices.storage = SetupStorage::Diskless;
    let mut setup = NodeSetupForTests::default();
    let before = std::fs::read(root.join("node-policy.json")).unwrap();
    let error = setup.accept_for_tests(root, conflicting.clone()).unwrap_err();
    assert!(error.to_string().contains("accepted deployment policy"));
    assert!(!root.join("node-setup.json").exists());
    assert!(!root.join("disks").exists());
    setup.accept_for_tests(root, intent).unwrap();
    let path = root.join("node-setup.json");
    let mut journal: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    journal["request"]["choices"] = serde_json::to_value(conflicting.choices).unwrap();
    let corrupt = serde_json::to_vec(&journal).unwrap();
    std::fs::write(&path, &corrupt).unwrap();
    assert!(NodeSetupForTests::status_for_tests(root).is_err());
    assert_eq!(std::fs::read(path).unwrap(), corrupt);
    assert_eq!(std::fs::read(root.join("node-policy.json")).unwrap(), before);
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

#[test]
fn cancelled_unbound_setup_can_be_replaced_without_losing_its_journal() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-setup-cancelled");
    let root = directory.path();
    let original = request(root);
    authorize(root, &original);
    let mut setup = NodeSetupForTests::default();
    setup.accept_for_tests(root, original.clone()).unwrap();
    let mut next = original.clone();
    next.operation_id = uuid::Uuid::new_v4().to_string();
    next.choices.storage = SetupStorage::Diskless;
    std::fs::write(
        root.join("ssh/authorized_keys"),
        format!(
            "ssh-ed25519 fixture crowdb:{}\nssh-ed25519 fixture crowdb:{}\n",
            original.operation_id, next.operation_id
        ),
    )
    .unwrap();
    assert!(
        setup.accept_for_tests(root, next.clone()).is_err(),
        "a live accepted plan remains immutable"
    );
    std::fs::write(
        root.join(format!("ssh/cancelled-{}.json", original.operation_id)),
        "{}",
    )
    .unwrap();
    setup.accept_for_tests(root, next.clone()).unwrap();
    assert_eq!(
        NodeSetupForTests::status_for_tests(root)
            .unwrap()
            .unwrap()
            .operation_id,
        next.operation_id
    );
    assert!(root
        .join(format!("cancelled-node-setup-{}.json", original.operation_id))
        .exists());
    assert!(setup.accept_for_tests(root, original).is_err());
}

#[test]
fn oversized_progress_never_becomes_an_unreadable_private_control_reply() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-setup-progress-bound");
    let root = directory.path();
    let intent = request(root);
    authorize(root, &intent);
    NodeSetupForTests::default()
        .accept_for_tests(root, intent)
        .unwrap();
    let path = root.join("node-setup.json");
    let mut current: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    current["status"]["error"] = serde_json::Value::String("x".repeat(65536));
    std::fs::write(&path, serde_json::to_vec(&current).unwrap()).unwrap();
    let error = NodeSetupForTests::status_for_tests(root).unwrap_err();
    assert!(error.to_string().contains("control reply bound"), "{error}");
    assert!(!root.join("disks").exists());
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
    let mut choices = crowdb_protocol::mgmt::node::deployment::NodeDeploymentPolicy::default().setup;
    choices.storage = SetupStorage::Diskless;
    for service in &mut choices.services {
        service.enabled = false;
    }
    let inputs = crowdb_protocol::mgmt::node::deployment::NodeDeploymentOverrides {
        storage: Some(choices.storage.clone()),
        services: Some(choices.services.clone()),
        ..Default::default()
    };
    crowdb_monitor::accept_node_policy(
        root,
        &inputs,
        &crowdb_protocol::mgmt::node::deployment::NodeDeploymentOverrides::default(),
    )
    .unwrap();
    let mut intent = request(root);
    intent.choices = choices;
    authorize(root, &intent);
    NodeSetupForTests::default()
        .accept_for_tests(root, intent.clone())
        .unwrap();
    let mut authority = node_setup::TestAuthority::start(root, &intent).await;
    let preparation_key = crowdb_console_shared::deployment::preparation::DEFAULT_PREPARATION_KEY;
    authority.kv.delete(0, 0, preparation_key, None).await.unwrap();
    let profile = profile(root);
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    let status = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(status.phase, NodeSetupPhase::Ready);
    assert!(matches!(
        authority
            .kv
            .get(
                0,
                0,
                preparation_key,
                crowdb_kv_client::ReadMode::Linearizable,
                None
            )
            .await
            .unwrap(),
        crowdb_kv_client::GetOutcome::NotFound,
    ));
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
    use crowdb_protocol::common::{NodeValue, RackValue};
    let directory = run_root::TestRunRoot::new("setup");
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
    node_setup_groups::publish_group(&authority, 8).await;
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
    node_setup_groups::publish_group(&authority, 1).await;
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
    assert_eq!(placement.zone_size_bytes, 256 * 1024 * 1024);
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

#[tokio::test]
async fn a_conflicting_reserved_disk_group_binding_cannot_redirect_default_storage() {
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
    use crowdb_protocol::{
        common::{NodeValue, RackValue},
        diskdb::rpc::DiskGroupValue,
    };
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("setup-binding");
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
    node_setup_groups::publish_group(&authority, 1).await;
    node_setup_groups::publish_group(&authority, 8).await;
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
    hardware
        .add_node(
            1,
            2,
            &NodeValue {
                disk_group_ids: vec![1],
                ..NodeValue::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_disk_group(1, 2, 1, &DiskGroupValue::default())
        .await
        .unwrap();
    hardware.set_bind(1, 2, 1, 0, 8).await.unwrap();
    NodeSetupForTests::advance_for_tests(profile(root)).await.unwrap();
    let observed = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert!(observed.error.unwrap().contains("conflicting metadata binding"));
    assert!(!root.join("disks").exists());
    let journal: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("node-setup.json")).unwrap()).unwrap();
    assert!(journal["file"].is_null());
    assert_eq!(hardware.get_bind(1, 2, 1).await.unwrap().unwrap().group_id, 8);
    assert_eq!(hardware.list_disk_groups_on_node(1, 2).await.unwrap().len(), 1);
}

#[tokio::test]
async fn normal_setup_defers_disks_and_service_commands_until_owned_metadata_is_available() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("setup-metadata-gate");
    let root = directory.path();
    let inputs = crowdb_protocol::mgmt::node::deployment::NodeDeploymentOverrides::default();
    let policy = crowdb_monitor::accept_node_policy(root, &inputs, &inputs).unwrap();
    let mut intent = request(root);
    intent.choices = policy.setup;
    authorize(root, &intent);
    NodeSetupForTests::default()
        .accept_for_tests(root, intent.clone())
        .unwrap();
    let _authority = node_setup::TestAuthority::start(root, &intent).await;
    NodeSetupForTests::advance_for_tests(profile(root)).await.unwrap();
    let observed = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(observed.phase, NodeSetupPhase::Running);
    assert!(observed.error.unwrap().contains("live kv-server"));
    assert!(observed
        .services
        .iter()
        .all(|step| step.phase == crowdb_protocol::mgmt::node::setup::SetupStepPhase::Pending));
    assert!(observed.disks.is_empty());
    assert!(!root.join("disks").exists());
}

fn profile(root: &Path) -> DeploymentProfile {
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile
}

#[tokio::test]
async fn recreated_setup_never_replaces_later_stop_delete_restart_or_reconfiguration() {
    use crowdb_protocol::mgmt::node::setup::{SetupServiceKind, SetupStepPhase};
    use crowdb_protocol::mgmt::node::NodeServiceAction;
    for action in [
        NodeServiceAction::Stop,
        NodeServiceAction::Delete,
        NodeServiceAction::Restart,
        NodeServiceAction::Start,
    ] {
        let directory = run_root::TestRunRoot::new("intent");
        let root = directory.path();
        let mut request = request(root);
        request.choices.storage = SetupStorage::Diskless;
        for service in &mut request.choices.services {
            service.enabled = service.kind == SetupServiceKind::Chunkdb;
        }
        authorize(root, &request);
        NodeSetupForTests::default()
            .accept_for_tests(root, request.clone())
            .unwrap();
        let authority = node_setup::TestAuthority::start(root, &request).await;
        let (original, newer) = node_setup_control::newer_intent(root, &authority, action).await;
        let mut profile = profile(root);
        profile.paths.run_root = root.join("run");
        let observe = node_setup_control::processes_once(&profile.paths.run_root);
        NodeSetupForTests::advance_for_tests(profile.clone())
            .await
            .unwrap();
        observe.await.unwrap();
        let status = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
        let step = status
            .services
            .iter()
            .find(|step| step.kind == SetupServiceKind::Chunkdb)
            .unwrap();
        assert_eq!(step.phase, SetupStepPhase::Warning);
        assert_eq!(step.operation_id.as_deref(), Some(original.as_str()));
        assert!(step.detail.as_ref().unwrap().contains("operator command"));
        // Historical plan completion is not a health assertion. Recreation and
        // exact acceptance cannot turn this newer command into the old Start.
        NodeSetupForTests::default()
            .accept_for_tests(root, request)
            .unwrap();
        NodeSetupForTests::advance_for_tests(profile).await.unwrap();
        assert_eq!(
            crowdb_console_shared::deployment::services::current(&authority.kv, 2, "chunkdb-2")
                .await
                .unwrap(),
            Some(newer)
        );
    }
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

#[tokio::test]
async fn service_failure_budget_survives_restart_and_yields_to_a_new_operator_command() {
    use crowdb_monitor::NodeServiceForTests;
    use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};
    let directory = run_root::TestRunRoot::new("budget");
    let root = directory.path();
    let mut request = request(root);
    request.choices.storage = SetupStorage::Diskless;
    for service in &mut request.choices.services {
        service.enabled = service.kind == crowdb_protocol::mgmt::node::setup::SetupServiceKind::Chunkdb;
    }
    authorize(root, &request);
    NodeSetupForTests::default()
        .accept_for_tests(root, request.clone())
        .unwrap();
    let authority = node_setup::TestAuthority::start(root, &request).await;
    let mut profile = profile(root);
    profile.paths.log_root = root.join("log");
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    profile.paths.run_root = root.join("run");
    let child = profile
        .services
        .iter_mut()
        .find(|service| service.id == "chunkdb")
        .unwrap();
    child.program = root.join("absent-binary");
    // Isolate launch-budget accounting from the separately tested dependency gate.
    child.dependencies = vec!["kv".into()];
    let mut intent = NodeServiceIntent {
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 2,
        service_id: "chunkdb-2".into(),
        kind: "chunkdb".into(),
        action: NodeServiceAction::Start,
        configuration: "[server]\nhttp_listen_addr = '127.0.0.1:12345'\n".into(),
        environment: std::collections::BTreeMap::default(),
    };
    let key = b"/deployment/services/2/chunkdb-2";
    authority
        .kv
        .put(0, 0, key, &serde_json::to_vec(&intent).unwrap(), None)
        .await
        .unwrap();
    for attempt in 1..=5 {
        // Recreating the manager cannot reset the persisted operation budget.
        let mut worker = NodeServiceForTests::new_for_tests(profile.clone()).await.unwrap();
        assert!(worker.pending_for_tests(&intent).unwrap());
        let error = worker.execute_for_tests(intent.clone()).await.unwrap_err();
        assert!(error.to_string().contains("No such file"), "{error}");
        let failures = worker.failures_for_tests().unwrap();
        assert_eq!(failures["chunkdb-2"]["attempts"], attempt);
        assert_eq!(failures["chunkdb-2"]["exhausted"], attempt == 5);
    }
    observe_exhausted_setup(root, &profile).await;
    let mut worker = NodeServiceForTests::new_for_tests(profile).await.unwrap();
    assert!(!worker.pending_for_tests(&intent).unwrap());
    assert!(worker
        .execute_for_tests(intent.clone())
        .await
        .unwrap_err()
        .to_string()
        .contains("restart budget"));
    let saved = std::fs::read(root.join("services/chunkdb-2/execution.json")).unwrap();
    intent.operation_id = uuid::Uuid::new_v4().to_string();
    intent.action = NodeServiceAction::Stop;
    // A different operation needs its own confirmed authority before effects.
    assert!(worker.execute_for_tests(intent.clone()).await.is_err());
    assert_eq!(
        std::fs::read(root.join("services/chunkdb-2/execution.json")).unwrap(),
        saved
    );
    authority
        .kv
        .put(0, 0, key, &serde_json::to_vec(&intent).unwrap(), None)
        .await
        .unwrap();
    assert!(worker.pending_for_tests(&intent).unwrap());
    worker.execute_for_tests(intent.clone()).await.unwrap();
    assert!(!worker.pending_for_tests(&intent).unwrap());
    assert!(worker
        .failures_for_tests()
        .unwrap()
        .as_object()
        .unwrap()
        .is_empty());
}

async fn observe_exhausted_setup(root: &Path, profile: &DeploymentProfile) {
    let failures = crowdb_monitor::NodeServiceForTests::new_for_tests(profile.clone())
        .await
        .unwrap()
        .failures_for_tests()
        .unwrap();
    std::fs::create_dir_all(&profile.paths.run_root).unwrap();
    let listener = tokio::net::UnixListener::bind(profile.paths.run_root.join("node-control.sock")).unwrap();
    let observe = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        assert!(matches!(
            serde_json::from_slice::<crowdb_protocol::mgmt::node::NodeControl>(&bytes).unwrap(),
            crowdb_protocol::mgmt::node::NodeControl::Processes { .. }
        ));
        stream
            .write_all(
                &serde_json::to_vec(
                    &serde_json::json!({"ok":true,"value":{"processes":{},"service_failures":failures}}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    });
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    observe.await.unwrap();
    let status = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(status.phase, NodeSetupPhase::Failed);
    let service = status
        .services
        .iter()
        .find(|step| step.kind == crowdb_protocol::mgmt::node::setup::SetupServiceKind::Chunkdb)
        .unwrap();
    assert_eq!(
        service.phase,
        crowdb_protocol::mgmt::node::setup::SetupStepPhase::Failed
    );
    assert!(service.detail.as_ref().unwrap().contains("No such file"));
    assert!(status.error.unwrap().contains("restart budget"));
}
