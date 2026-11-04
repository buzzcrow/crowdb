// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "../src/node/durable.rs"]
mod durable;

use crowdb_monitor::{DeploymentProfile, NodeServiceForTests};
use crowdb_protocol::chunk_kv::*;
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, TextKey};
use crowdb_protocol::mgmt::{
    node::{NodeBinding, NodeServiceAction, NodeServiceIntent},
    SystemBootstrapIdentity,
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[tokio::test]
async fn a_local_chunk_kv_listener_cannot_admit_access_before_catalog_authority_is_available() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("access-dependency-gate");
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.path().to_path_buf();
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "access")
        .unwrap()
        .dependencies = vec!["chunk-kv".into()];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = axum::Router::new().route("/ready", axum::routing::get(|| async { "ok" }));
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let binding = NodeBinding {
        node_id: 1,
        bootstrap: SystemBootstrapIdentity {
            cluster_id: uuid::Uuid::new_v4().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            configuration_digest: "digest".into(),
        },
        management_seeds: vec!["http://127.0.0.1:1".into()],
    };
    durable::write(&root.path().join("node-binding.json"), &binding).unwrap();
    let binding: NodeBinding = durable::read(&root.path().join("node-binding.json"))
        .unwrap()
        .unwrap();
    let local = NodeServiceIntent {
        cluster_id: binding.bootstrap.cluster_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 1,
        service_id: "chunk-kv-1".into(),
        kind: "chunk-kv".into(),
        action: NodeServiceAction::Start,
        configuration: format!("http_listen_addr = \"{address}\"\n"),
        environment: BTreeMap::new(),
    };
    let access = NodeServiceIntent {
        kind: "access".into(),
        service_id: "access-1".into(),
        configuration: String::new(),
        ..local.clone()
    };
    let readiness = NodeServiceForTests::dependencies_ready_for_tests(&profile, &access, &[local]).await;
    server.abort();
    assert!(
        readiness.is_err(),
        "a local idle listener does not prove routed catalog reads can succeed"
    );
    assert!(!root.path().join("services/access-1/execution.json").exists());
}

#[path = "common/node_setup.rs"]
mod node_setup;
#[path = "common/service_dependency.rs"]
mod service_dependency;

#[tokio::test]
async fn healthy_diskdb_listener_must_publish_the_registered_disks_ownership_before_consumers_start() {
    use crowdb_kv_client::ServiceRegistryClient;
    use crowdb_protocol::mgmt::node::setup::{NodeSetupChoices, NodeSetupRequest};
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("diskdb-route-gate");
    let request = NodeSetupRequest {
        version: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: NodeSetupChoices::default(),
    };
    let authority = node_setup::TestAuthority::start(root.path(), &request).await;
    assert_eq!(authority.registry.nodes[0].node_id, 2);
    authority.kv.refresh_topology().await.unwrap();
    let kv = std::sync::Arc::new(crowdb_kv_client::CrowdbKvClient::new(
        crowdb_kv_client::ClientConfig::new(authority.binding.management_seeds.clone()),
    ));
    service_dependency::publish_online_disk(kv.clone()).await;
    let (address, server) = service_dependency::ready_listener().await;
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.path().to_path_buf();
    profile.paths.log_root = root.path().join("log");
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "chunkdb")
        .unwrap()
        .dependencies = vec!["diskdb".into()];
    let local = NodeServiceIntent {
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 2,
        service_id: "diskdb-2".into(),
        kind: "diskdb".into(),
        action: NodeServiceAction::Start,
        configuration: format!("[server]\nhttp_listen_addr = \"{address}\"\n"),
        environment: BTreeMap::new(),
    };
    let consumer = NodeServiceIntent {
        kind: "chunkdb".into(),
        service_id: "chunkdb-2".into(),
        configuration: String::new(),
        ..local.clone()
    };
    let registry = ServiceRegistryClient::from_shared(kv.clone());
    registry
        .register_diskdb(2, "127.0.0.1:7102", &[], &[])
        .await
        .unwrap();
    assert!(
        NodeServiceForTests::dependencies_ready_for_tests(&profile, &consumer, std::slice::from_ref(&local))
            .await
            .is_err(),
        "a ready listener with no published owner cannot route an allocation"
    );
    registry
        .heartbeat_diskdb(2, "127.0.0.1:7102", &[99], &[])
        .await
        .unwrap();
    assert!(NodeServiceForTests::dependencies_ready_for_tests(
        &profile,
        &consumer,
        std::slice::from_ref(&local)
    )
    .await
    .is_err());
    registry
        .heartbeat_diskdb(2, "127.0.0.1:7102", &[7], &[])
        .await
        .unwrap();
    assert!(
        NodeServiceForTests::dependencies_ready_for_tests(&profile, &consumer, std::slice::from_ref(&local))
            .await
            .is_err(),
        "an owned group without its registered disk loaded cannot allocate"
    );
    service_dependency::assert_initial_start_waits(&kv, &profile, &local, &consumer).await;
    registry
        .heartbeat_diskdb(
            2,
            "127.0.0.1:7102",
            &[7],
            &[crowdb_protocol::common::DiskGroupUsageSummary {
                disk_group_id: 7,
                usage_known: true,
                disk_count: 1,
                ..Default::default()
            }],
        )
        .await
        .unwrap();
    NodeServiceForTests::dependencies_ready_for_tests(&profile, &consumer, &[local])
        .await
        .unwrap();
    server.abort();
    assert!(!root.path().join("services/chunkdb-2/execution.json").exists());
}

#[tokio::test]
async fn access_readiness_follows_catalog_redirects_within_its_probe_deadline() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("access-route-redirect");
    let request = crowdb_protocol::mgmt::node::setup::NodeSetupRequest {
        version: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: crowdb_protocol::mgmt::node::setup::NodeSetupChoices::default(),
    };
    let authority = node_setup::TestAuthority::start(root.path(), &request).await;
    let (rpc, requests) = redirecting_rpc();
    publish_catalog(&authority.kv, rpc.port(), None).await;
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.path().to_path_buf();
    // Isolate the routed catalog dependency; listener probes have separate coverage.
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "access")
        .unwrap()
        .dependencies
        .clear();
    let intent = NodeServiceIntent {
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        node_id: 2,
        operation_id: uuid::Uuid::new_v4().to_string(),
        service_id: "access-2".into(),
        kind: "access".into(),
        action: NodeServiceAction::Restart,
        configuration: String::new(),
        environment: BTreeMap::new(),
    };
    let result = NodeServiceForTests::dependencies_ready_for_tests(&profile, &intent, &[]).await;
    result.unwrap();
    assert_eq!(requests.load(Ordering::Relaxed), 2);
    let (second_rpc, second_requests) = redirecting_rpc();
    publish_catalog(
        &authority.kv,
        rpc.port(),
        Some(u16::try_from(second_rpc.port()).unwrap()),
    )
    .await;
    NodeServiceForTests::dependencies_ready_for_tests(&profile, &intent, &[])
        .await
        .unwrap();
    assert_eq!(second_requests.load(Ordering::Relaxed), 2);
    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    publish_catalog(
        &authority.kv,
        rpc.port(),
        Some(unavailable.local_addr().unwrap().port()),
    )
    .await;
    let result = NodeServiceForTests::dependencies_ready_for_tests(&profile, &intent, &[]).await;
    rpc.stop();
    second_rpc.stop();
    assert!(
        result.is_err(),
        "a healthy first owner must not hide an unavailable second owner"
    );
    assert!(!root.path().join("services/access-2/execution.json").exists());
}

fn redirecting_rpc() -> (crowdb_rpc_ffi::RpcServer, Arc<AtomicUsize>) {
    let rpc = crowdb_rpc_ffi::RpcServer::new(None);
    rpc.listen("127.0.0.1", 0).unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = requests.clone();
    rpc.register_reply_for_tests(
        u16::try_from(crowdb_protocol::fb::FBMsgType::EChunkKvPointRequest.0).unwrap(),
        move |request| {
            let result = if observed.fetch_add(1, Ordering::Relaxed) == 0 {
                Err(RpcFailure {
                    code: ChunkKvRpcErrorCode::NotMyRange,
                    message: "owner grant is catching up".into(),
                    retry_after_ms: None,
                    latest_map_revision: Some(1),
                    owner_hint: None,
                })
            } else {
                Ok(OperationResult::Value(None))
            };
            let response = ChunkKvResponse {
                map_revision: 1,
                journal_position: None,
                result,
            };
            let (bytes, offset) = crowdb_protocol::chunk_kv_wire::encode_point_response(
                request.request_id,
                request.rpc_create_nano,
                &response,
            );
            (bytes[offset..].to_vec(), Vec::new())
        },
    );
    rpc.start();
    (rpc, requests)
}

async fn publish_catalog(kv: &crowdb_kv_client::CrowdbKvClient, port: i32, second: Option<u16>) {
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        checksum: [0; 32],
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id: Id128 { high: 1, low: 1 },
            range: KeyRange {
                start: vec![],
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 2,
                rpc_endpoint: format!("127.0.0.1:{port}"),
            },
            owner_epoch: 1,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            transition_id: None,
            artifact: PartitionArtifact {
                tree_id: 1,
                stream_name: crowdb_protocol::chunk_stream::StreamName { high: 1, low: 1 },
                tail_overlay: None,
            },
        }],
    };
    if let Some(port) = second {
        page.entries[0].range.end = Some(vec![128]);
        let mut entry = page.entries[0].clone();
        entry.partition_id.low = 2;
        entry.artifact.tree_id = 2;
        entry.artifact.stream_name.low = 2;
        entry.range = KeyRange {
            start: vec![128],
            end: None,
        };
        entry.owner.instance_id = 3;
        entry.owner.rpc_endpoint = format!("127.0.0.1:{port}");
        page.entries.push(entry);
    }
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        checksum: [0; 32],
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: vec![],
            page_checksum: page.checksum,
        }],
    };
    head.seal().unwrap();
    for (key, value) in [
        (
            ChunkKvRangeCatalogPageKey {
                generation: 1,
                page_index: 0,
            }
            .to_path(),
            serde_json::to_vec(&page).unwrap(),
        ),
        (
            ChunkKvRangeCatalogHeadKey.to_path(),
            serde_json::to_vec(&head).unwrap(),
        ),
    ] {
        kv.put(0, 0, key.as_bytes(), &value, None).await.unwrap();
    }
}
