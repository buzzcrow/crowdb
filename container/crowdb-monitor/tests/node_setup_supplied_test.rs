// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_monitor::{DeploymentProfile, NodeIdentity, NodeSetupForTests};
use crowdb_protocol::{
    common::{BindMapValue, NodeValue, RackValue},
    diskdb::rpc::DiskGroupValue,
    mgmt::node::{
        setup::{
            NodeSetupChoices, NodeSetupPhase, NodeSetupRequest, SetupMedium, SetupStepPhase, SetupStorage,
        },
        storage::{
            DiskBackend, DiskInput, PrepareSuppliedDisk, SuppliedPreparation, SuppliedPreparationPhase,
        },
        NodeControl,
    },
};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};
#[path = "common/node_setup.rs"]
mod node_setup;
#[path = "common/node_setup_groups.rs"]
mod node_setup_groups;

struct TestSetup {
    root: crowdb_test_harness::test_dirs::TestDir,
    request: NodeSetupRequest,
    inputs: Vec<DiskInput>,
    profile: DeploymentProfile,
}

impl TestSetup {
    fn new() -> Self {
        // Keep the socket pathname short enough for CI checkout prefixes.
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("supplied");
        fs::create_dir(root.path().join("disks")).unwrap();
        for name in ["first", "second", "third"] {
            fs::File::create(root.path().join("disks").join(name))
                .unwrap()
                .set_len(65 * 1024 * 1024 + 512)
                .unwrap();
        }
        let disks = crowdb_monitor::discover_disks(root.path(), &[]).unwrap().disks;
        let inputs = disks
            .iter()
            .map(|disk| DiskInput {
                backend: DiskBackend::File,
                path: disk.path.clone(),
                backing_identity: disk.backing_identity.clone(),
                resource_identity: disk.resource_identity.clone(),
            })
            .collect();
        let mut choices = NodeSetupChoices::default();
        for service in &mut choices.services {
            service.enabled = false;
        }
        choices.storage = SetupStorage::Supplied {
            disks: disks
                .iter()
                .enumerate()
                .map(|(index, disk)| SetupMedium {
                    path: disk.path.clone(),
                    backend: DiskBackend::File,
                    backing_identity: disk.backing_identity.clone().unwrap(),
                    resource_identity: disk.resource_identity.clone(),
                    capacity_bytes: disk.capacity_bytes.unwrap(),
                    media_type: (index == 2).then_some(1),
                    initialize: true,
                })
                .collect(),
        };
        let request = NodeSetupRequest {
            version: 1,
            discovery_id: NodeIdentity::load_or_create(root.path())
                .unwrap()
                .uuid()
                .to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            choices,
        };
        fs::create_dir(root.path().join("ssh")).unwrap();
        fs::write(
            root.path().join("ssh/authorized_keys"),
            format!("ssh-ed25519 fixture crowdb:{}\n", request.operation_id),
        )
        .unwrap();
        let mut profile = DeploymentProfile::load(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml"),
        )
        .unwrap();
        profile.paths.data_root = root.path().to_path_buf();
        profile.paths.run_root = root.path().join("run");
        fs::create_dir(&profile.paths.run_root).unwrap();
        Self {
            root,
            request,
            inputs,
            profile,
        }
    }

    fn accept(&self) {
        NodeSetupForTests::default()
            .accept_with_inputs_for_tests(self.root.path(), self.request.clone(), &self.inputs)
            .unwrap();
    }

    async fn authority(&self) -> node_setup::TestAuthority {
        let authority = node_setup::TestAuthority::start(self.root.path(), &self.request).await;
        assert_eq!(authority.registry.nodes[0].physical_host_id, "owned-host");
        let hardware = HardwareClient::new(CrowdbKvClient::new(ClientConfig::new(
            authority.binding.management_seeds.clone(),
        )));
        hardware
            .add_rack(
                1,
                &RackValue {
                    node_ids: vec![2],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_node(
                1,
                2,
                &NodeValue {
                    disk_group_ids: vec![7],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_disk_group(1, 2, 7, &DiskGroupValue::default())
            .await
            .unwrap();
        hardware.set_bind(1, 2, 7, 0, 1).await.unwrap();
        node_setup_groups::publish_group(&authority, 1).await;
        authority
    }

    fn observe(&self, count: usize) -> tokio::task::JoinHandle<Vec<PrepareSuppliedDisk>> {
        let socket = self.profile.paths.run_root.join("node-control.sock");
        if socket.exists() {
            fs::remove_file(&socket).unwrap();
        }
        let listener = tokio::net::UnixListener::bind(socket).unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut requests = Vec::new();
            for _ in 0..count {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).await.unwrap();
                let NodeControl::PrepareSuppliedDisk { request } = serde_json::from_slice(&bytes).unwrap()
                else {
                    panic!("unexpected setup command")
                };
                requests.push(request);
                stream.write_all(br#"{"ok":true,"value":{}}"#).await.unwrap();
            }
            requests
        })
    }

    async fn plan(&self) -> Vec<PrepareSuppliedDisk> {
        let observer = self.observe(3);
        NodeSetupForTests::advance_for_tests(self.profile.clone())
            .await
            .unwrap();
        observer.await.unwrap()
    }

    fn outcome(&self, request: PrepareSuppliedDisk, phase: SuppliedPreparationPhase) {
        let path = self
            .root
            .path()
            .join(format!("disk-supplied-{}.json", request.record.operation_id));
        let state = SuppliedPreparation {
            version: 1,
            request,
            phase,
            verified_bytes: 0,
            error: (phase == SuppliedPreparationPhase::Failed).then(|| "not blank".into()),
        };
        fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[test]
fn supplied_setup_requires_explicit_verified_mapping_before_persisting_intent() {
    let test = TestSetup::new();
    assert!(NodeSetupForTests::default()
        .accept_with_inputs_for_tests(test.root.path(), test.request.clone(), &[])
        .is_err());
    assert!(!test.root.path().join("node-setup.json").exists());
    let mut changed = test.request.clone();
    if let SetupStorage::Supplied { disks } = &mut changed.choices.storage {
        disks[0].resource_identity = Some("file:1:2".into());
    }
    assert!(NodeSetupForTests::default()
        .accept_with_inputs_for_tests(test.root.path(), changed, &test.inputs)
        .is_err());
    assert!(!test.root.path().join("node-setup.json").exists());
    test.accept();
    let before = fs::read(test.root.path().join("node-setup.json")).unwrap();
    let state: serde_json::Value = serde_json::from_slice(&before).unwrap();
    assert_eq!(state["supplied_observations"].as_array().unwrap().len(), 3);
    NodeSetupForTests::default()
        .accept_with_inputs_for_tests(test.root.path(), test.request.clone(), &[])
        .unwrap();
    assert_eq!(
        fs::read(test.root.path().join("node-setup.json")).unwrap(),
        before
    );
}

#[tokio::test]
async fn media_class_placement_and_disk_operations_survive_repeated_background_passes() {
    let test = TestSetup::new();
    test.accept();
    let authority = test.authority().await;
    let first = test.plan().await;
    let second = test.plan().await;
    assert_eq!(first, second);
    assert_eq!(first[0].record.disk_group_id, 7);
    assert_eq!(first[1].record.disk_group_id, 7);
    assert_ne!(
        first[2].record.disk_group_id, 7,
        "two classes must not race for one initially empty group"
    );
    for request in first {
        request.validate().unwrap();
        let backing = request.record.physical_backing.as_ref().unwrap();
        assert_eq!(backing.physical_host_id, "owned-host");
        assert_eq!(backing.capacity_bytes, 65 * 1024 * 1024 + 512);
        assert_eq!(request.record.geometry.capacity_units, 64);
        assert!(
            request.record.sparse,
            "existing sparse files must not be labeled preallocated"
        );
        let key = crowdb_protocol::key::DiskKey {
            rack_id: 1,
            node_id: 2,
            disk_group_id: request.record.disk_group_id,
            disk_id: request.record.disk_id,
        };
        let hardware = HardwareClient::new(CrowdbKvClient::new(ClientConfig::new(
            authority.binding.management_seeds.clone(),
        )));
        assert!(
            hardware.get_disk_with_revision(&key).await.unwrap().is_none(),
            "dispatch alone cannot fabricate membership"
        );
        let binding = hardware
            .get_bind(1, 2, request.record.disk_group_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            BindMapValue {
                store_id: binding.store_id,
                group_id: binding.group_id
            },
            BindMapValue {
                store_id: 0,
                group_id: 1
            }
        );
    }
}

#[tokio::test]
async fn failed_medium_keeps_successful_siblings_and_only_explicit_retry_dispatches_it() {
    let test = TestSetup::new();
    test.accept();
    let _authority = test.authority().await;
    let requests = test.plan().await;
    for (index, request) in requests.iter().enumerate() {
        test.outcome(
            request.clone(),
            if index == 1 {
                SuppliedPreparationPhase::Failed
            } else {
                SuppliedPreparationPhase::Registered
            },
        );
    }
    NodeSetupForTests::advance_for_tests(test.profile.clone())
        .await
        .unwrap();
    let failed = NodeSetupForTests::status_for_tests(test.root.path())
        .unwrap()
        .unwrap();
    assert_eq!(failed.phase, NodeSetupPhase::Failed);
    assert_eq!(
        failed.disks.iter().map(|disk| disk.phase).collect::<Vec<_>>(),
        vec![
            SetupStepPhase::Ready,
            SetupStepPhase::Failed,
            SetupStepPhase::Ready
        ]
    );
    test.accept();
    let observer = test.observe(1);
    NodeSetupForTests::advance_for_tests(test.profile.clone())
        .await
        .unwrap();
    assert_eq!(observer.await.unwrap(), vec![requests[1].clone()]);
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(test.root.path().join("node-setup.json")).unwrap()).unwrap();
    assert!(state["retry_disks"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn changed_durable_placement_is_rejected_before_another_dispatch() {
    let test = TestSetup::new();
    test.accept();
    let _authority = test.authority().await;
    test.plan().await;
    let path = test.root.path().join("node-setup.json");
    let mut state: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    state["supplied"][0]["request"]["record"]["node_id"] = 99.into();
    fs::write(path, serde_json::to_vec(&state).unwrap()).unwrap();
    let error = NodeSetupForTests::advance_for_tests(test.profile.clone())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("sealed medium identity"), "{error}");
}
