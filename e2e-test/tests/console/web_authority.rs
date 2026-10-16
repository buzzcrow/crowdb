// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::fixture::{web_kv, TestKvCluster as KvCluster};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient};
use crowdb_protocol::common::{HwStatus, KvServerIdentity, NodeValue, RackValue, ReplicaValue};
use crowdb_protocol::key::InstanceKey;
use reqwest::StatusCode;

async fn snapshot(app: &str) -> (StatusCode, serde_json::Value) {
    hardware_request(app, reqwest::Method::GET, "/api/preview", None, false).await
}

async fn register(sysmd: &CrowdbSysmdClient, node: u64, instance: u64, endpoint: &str) {
    sysmd
        .register_kv_server(
            KvServerIdentity {
                instance_id: instance,
                node_id: Some(node),
            },
            endpoint,
            &[0],
            &[],
            "ok",
            "/tmp/bare-metal-kv",
        )
        .await
        .unwrap();
}

async fn initialized_authority(cluster: &KvCluster) -> CrowdbSysmdClient {
    let kv = CrowdbKvClient::new(ClientConfig::new(cluster.mgmt_endpoints.clone()));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let sysmd = CrowdbSysmdClient::new(kv);
    sysmd
        .add_rack(
            1,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: vec![1],
                name: "rack-a".into(),
            },
        )
        .await
        .unwrap();
    sysmd
        .add_node(
            1,
            1,
            &NodeValue {
                status: HwStatus::Up as i32,
                management_host: "node-a.example".into(),
                ssh_port: 2222,
                ssh_user: "operator".into(),
                ssh_credential_ref: Some("node-a-key".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    sysmd.add_store(0, &[1]).await.unwrap();
    register(&sysmd, 1, 7001, &cluster.mgmt_endpoints[0]).await;
    sysmd
}

fn application(_cluster: &KvCluster) -> String {
    format!("http://{}:9090", crate::common::management::addresses(5)[3])
}

fn application_peer() -> String {
    format!("http://{}:9090", crate::common::management::addresses(5)[4])
}

async fn hardware_request(
    app: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<serde_json::Value>,
    authenticated: bool,
) -> (StatusCode, serde_json::Value) {
    let client = crate::common::management::client();
    let mut request = client.request(method, format!("{app}{path}"));
    if authenticated {
        request = request.bearer_auth("bare-metal-test-token-123456789012345");
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    (
        status,
        if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}

#[tokio::test]
#[ignore = "requires release KV voters and two independent release Web servers"]
async fn bare_metal_hardware_routes_share_confirmed_group_zero_state() {
    let cluster = web_kv().await;
    initialized_authority(&cluster).await;
    let first = application(&cluster);
    let second = application_peer();
    let rack = serde_json::json!({"id": 8, "name": "rack-eight"});
    assert_eq!(
        hardware_request(&first, reqwest::Method::POST, "/api/racks", Some(rack), true)
            .await
            .0,
        StatusCode::CREATED
    );
    let (_, racks) = hardware_request(&second, reqwest::Method::GET, "/api/racks", None, false).await;
    assert!(racks
        .as_array()
        .unwrap()
        .iter()
        .any(|rack| rack["id"] == 8 && rack["name"] == "rack-eight"));
    let node = serde_json::json!({"id": 9, "rack_id": 8, "host": "node-nine.example", "ssh_port": 2222, "ssh_user": "operator", "ssh_credential_ref": "ops-key"});
    let mut secret_node = node.clone();
    secret_node["ssh_key"] = serde_json::json!("/tmp/private-key");
    assert_eq!(
        hardware_request(
            &first,
            reqwest::Method::POST,
            "/api/nodes",
            Some(secret_node),
            true
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        hardware_request(&first, reqwest::Method::POST, "/api/nodes", Some(node), true)
            .await
            .0,
        StatusCode::CREATED
    );
    let (_, nodes) =
        hardware_request(&second, reqwest::Method::GET, "/api/nodes?rack_id=8", None, false).await;
    assert_eq!(nodes[0]["host"], "node-nine.example");
    assert_eq!(nodes[0]["ssh_credential_ref"], "ops-key");
    assert!(nodes[0].get("ssh_key").is_none());
    assert_eq!(
        hardware_request(&first, reqwest::Method::GET, "/api/racks/8", None, false)
            .await
            .1["name"],
        "rack-eight"
    );
    assert_eq!(
        hardware_request(&first, reqwest::Method::GET, "/api/racks/8/nodes", None, false)
            .await
            .1[0]["id"],
        9
    );
    assert_eq!(
        hardware_request(&first, reqwest::Method::GET, "/api/nodes/9", None, false)
            .await
            .1["host"],
        "node-nine.example"
    );
    assert_eq!(
        hardware_request(&second, reqwest::Method::DELETE, "/api/racks/8", None, true)
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        hardware_request(
            &second,
            reqwest::Method::POST,
            "/api/racks",
            Some(serde_json::json!({"id": 8, "name": "changed"})),
            true
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    verify_storage_hardware(&first, &second).await;
    verify_hardware_deletion(&first, &second).await;
}

async fn verify_storage_hardware(first: &str, second: &str) {
    let groups = "/api/nodes/9/disk-groups";
    let group = serde_json::json!({"id": 4, "name": "hot"});
    assert_eq!(
        hardware_request(first, reqwest::Method::POST, groups, Some(group), true)
            .await
            .0,
        StatusCode::CREATED
    );
    assert_eq!(
        hardware_request(second, reqwest::Method::GET, groups, None, false)
            .await
            .1[0]["name"],
        "hot"
    );
    let disks = "/api/nodes/9/disk-groups/4/disks";
    assert_eq!(
        hardware_request(
            second,
            reqwest::Method::GET,
            "/api/nodes/9/disk-groups/4",
            None,
            false
        )
        .await
        .1["name"],
        "hot"
    );
    let disk = serde_json::json!({
        "disk_id": "0000000000000000-0000000000000009", "disk_type": "Ssd",
        "capacity_bytes": 4096, "zone_size_bytes": 4096, "unit_size_bytes": 4096,
        "device_path": "",
    });
    assert_eq!(
        hardware_request(second, reqwest::Method::POST, disks, Some(disk), true)
            .await
            .0,
        StatusCode::CREATED
    );
    assert_eq!(
        hardware_request(first, reqwest::Method::GET, disks, None, false)
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let (status, observed) = hardware_request(
        first,
        reqwest::Method::GET,
        "/api/nodes/9/disk-groups/4/disks/0000000000000000-0000000000000009",
        None,
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(observed["disk_id"], "0000000000000000-0000000000000009");
    assert!(observed.get("device_path").is_none());
    verify_storage_removal(first, second).await;
}

async fn verify_storage_removal(first: &str, second: &str) {
    assert_eq!(
        hardware_request(
            first,
            reqwest::Method::DELETE,
            "/api/nodes/9/disk-groups/4",
            None,
            true
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        hardware_request(
            second,
            reqwest::Method::DELETE,
            "/api/nodes/9/disk-groups/4/disks/0000000000000000-0000000000000009",
            None,
            true
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        hardware_request(
            first,
            reqwest::Method::DELETE,
            "/api/nodes/9/disk-groups/4",
            None,
            true
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
}

async fn verify_hardware_deletion(first: &str, second: &str) {
    assert_eq!(
        hardware_request(second, reqwest::Method::DELETE, "/api/nodes/9", None, true)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    let (_, nodes) = hardware_request(first, reqwest::Method::GET, "/api/nodes?rack_id=8", None, false).await;
    assert!(nodes.as_array().unwrap().is_empty());
    assert_eq!(
        hardware_request(first, reqwest::Method::DELETE, "/api/racks/8", None, true)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
}

async fn unavailable(app: &str) {
    let (code, body) = snapshot(app).await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["reason"], "group0_unavailable");
    assert!(body.get("stores").is_none(), "stale topology: {body}");
    assert!(body["monitor"].is_null());
}

#[tokio::test]
#[ignore = "requires release KV voters and two independent release Web servers"]
async fn bare_metal_snapshot_requires_live_authority_without_a_docker_monitor() {
    let cluster = web_kv().await;
    let sysmd = initialized_authority(&cluster).await;
    let app = application(&cluster);
    let (code, view) = snapshot(&app).await;
    assert_eq!(code, StatusCode::OK, "{view}");
    assert_eq!(view["source"], "group0");
    assert_eq!(view["nodes"][0]["id"], 1);
    assert_eq!(view["racks"][0]["name"], "rack-a");
    assert_eq!(view["nodes"][0]["management_host"], "node-a.example");
    assert_eq!(view["nodes"][0]["ssh_port"], 2222);
    assert_eq!(view["nodes"][0]["ssh_user"], "operator");
    assert_eq!(view["nodes"][0]["ssh_credential_ref"], "node-a-key");
    assert_eq!(snapshot(&application_peer()).await.1["nodes"], view["nodes"]);
    assert!(view["monitor"].is_null());

    register(&sysmd, 1, 7002, &cluster.mgmt_endpoints[0]).await;
    unavailable(&app).await;
    sysmd.unregister_service("kv-server", 7002).await.unwrap();
    sysmd.unregister_service("kv-server", 7001).await.unwrap();
    unavailable(&app).await;
    register(&sysmd, 1, 7001, &cluster.mgmt_endpoints[0]).await;

    let (_, mut expired) = sysmd
        .read_all_kv_server_instances()
        .await
        .unwrap()
        .into_iter()
        .find(|(id, _)| *id == 7001)
        .unwrap();
    expired.last_heartbeat_ms = 1;
    let key = InstanceKey {
        service: "kv-server".into(),
        instance_id: 7001,
    }
    .to_path();
    sysmd
        .kv()
        .put(0, 0, key.as_bytes(), &serde_json::to_vec(&expired).unwrap(), None)
        .await
        .unwrap();
    unavailable(&app).await;
    register(&sysmd, 1, 7001, &cluster.mgmt_endpoints[0]).await;
    assert_eq!(snapshot(&app).await.0, StatusCode::OK);

    for index in 0..3 {
        crate::common::control::node("kill", index).await;
    }
    unavailable(&app).await;
    assert_eq!(
        hardware_request(&app, reqwest::Method::GET, "/api/racks", None, false)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
#[ignore = "requires release KV voters and two independent release Web servers"]
async fn snapshot_validates_later_replica_hosts_as_well_as_original_store_hosts() {
    let cluster = web_kv().await;
    let sysmd = initialized_authority(&cluster).await;
    let app = application(&cluster);
    sysmd.add_group(0, 7).await.unwrap();
    sysmd
        .add_replica(&ReplicaValue {
            store_id: 0,
            group_id: 7,
            replica_id: 2,
            node_id: 2,
            voting: true,
            ..Default::default()
        })
        .await
        .unwrap();
    unavailable(&app).await;
    register(&sysmd, 2, 7002, &cluster.mgmt_endpoints[0]).await;
    assert_eq!(snapshot(&app).await.0, StatusCode::OK);
}
