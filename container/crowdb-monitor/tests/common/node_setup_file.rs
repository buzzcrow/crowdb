// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{DeploymentProfile, NodeIdentity, NodeSetupForTests};
use crowdb_protocol::mgmt::node::{
    setup::{NodeSetupChoices, NodeSetupRequest, SetupStorage},
    storage::FilePreparationPhase,
    NodeControl,
};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn prepare(
    root: &Path,
    services: bool,
) -> (
    NodeSetupRequest,
    super::node_setup::TestAuthority,
    DeploymentProfile,
) {
    let mut request = NodeSetupRequest {
        version: 1,
        discovery_id: NodeIdentity::load_or_create(root).unwrap().uuid().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: NodeSetupChoices::default(),
    };
    request.choices.storage = SetupStorage::Automatic {
        capacity_bytes: 64 * 1024 * 1024,
        reserve_bytes: i64::MAX as u64,
        sparse: false,
    };
    for service in &mut request.choices.services {
        service.enabled = services;
    }
    std::fs::create_dir(root.join("ssh")).unwrap();
    std::fs::write(
        root.join("ssh/authorized_keys"),
        format!("ssh-ed25519 fixture crowdb:{}\n", request.operation_id),
    )
    .unwrap();
    NodeSetupForTests::default()
        .accept_for_tests(root, request.clone())
        .unwrap();
    let authority = super::node_setup::TestAuthority::start(root, &request).await;
    publish_storage(&authority).await;
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.run_root = root.join("run");
    std::fs::create_dir(&profile.paths.run_root).unwrap();
    (request, authority, profile)
}

async fn publish_storage(authority: &super::node_setup::TestAuthority) {
    use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient};
    use crowdb_protocol::common::{NodeValue, RackValue};
    super::node_setup_groups::publish_group(authority, 1).await;
    let node = &authority.registry.nodes[0];
    let hardware = HardwareClient::new(PaxosKvClient::new(ClientConfig::new(
        authority.binding.management_seeds.clone(),
    )));
    hardware
        .add_rack(
            node.rack_id,
            &RackValue {
                node_ids: vec![node.node_id],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(node.rack_id, node.node_id, &NodeValue::default())
        .await
        .unwrap();
}

pub async fn fail_file_request(
    listener: tokio::net::UnixListener,
    root: std::path::PathBuf,
) -> crowdb_protocol::mgmt::node::storage::FilePreparation {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    let NodeControl::PrepareDiskFile { request } = serde_json::from_slice(&bytes).unwrap() else {
        panic!("unexpected setup command");
    };
    crowdb_monitor::accept_file_for_tests(&root, &request).unwrap();
    let result = crowdb_monitor::prepare_file_for_tests(&root, &request.operation_id).unwrap();
    assert_eq!(result.phase, FilePreparationPhase::Failed);
    stream.write_all(br#"{"ok":true,"value":{}}"#).await.unwrap();
    result
}
