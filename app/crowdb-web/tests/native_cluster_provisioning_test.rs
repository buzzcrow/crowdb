// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cold normal-mode acceptance; kept outside the fast page suite.

use axum::{body::Body, http::Request};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

struct TestServices(AppState);
impl Drop for TestServices {
    fn drop(&mut self) {
        let pids: Vec<_> = self
            .0
            .config
            .read()
            .unwrap()
            .servers
            .iter()
            .filter_map(|entry| entry.pid)
            .collect();
        let started = std::time::Instant::now();
        // Stop consumers before their KV authority so shutdown can flush normally.
        for pid in pids.into_iter().rev() {
            crowdb_console_shared::lifecycle::stop_pid_with_timeout(pid, std::time::Duration::from_secs(2))
                .unwrap();
            assert!(!crowdb_console_shared::lifecycle::process_is_alive(pid));
        }
        eprintln!("[PHASE] owned teardown: {}ms", started.elapsed().as_millis());
    }
}

async fn call(app: &axum::Router, method: &str, path: &str, body: Value) -> Value {
    let started = std::time::Instant::now();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        app.clone().oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        ),
    )
    .await
    .expect("bounded management response")
    .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    eprintln!("[PHASE] {method} {path}: {}ms", started.elapsed().as_millis());
    assert!(
        status.is_success(),
        "{path}: {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

async fn deploy(app: &axum::Router, node: u64, kind: &str) {
    let defaults = call(app, "GET", "/api/deployment-defaults", Value::Null).await;
    let mut body = defaults[kind].clone();
    let path = match kind {
        "kv" => {
            body = json!({"rest_port": body["http_port"], "rpc_port": body["rpc_port"]});
            format!("/api/nodes/{node}/server/deploy")
        }
        "diskdb" => {
            body = json!({"rpc_port": body["rpc_port"]});
            format!("/api/nodes/{node}/diskdb/deploy")
        }
        _ => {
            body["kind"] = json!(kind);
            body["test_single_node"] = json!(false);
            if kind == "diskio" {
                body["disk_group_id"] = json!(node);
            }
            if kind == "chunk-kv" {
                body["metadata_store_id"] = json!(0);
                body["bootstrap_group_id"] = json!(1);
            }
            format!("/api/nodes/{node}/services/deploy")
        }
    };
    call(app, "POST", &path, body).await;
}

async fn pending_group(app: &axum::Router) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes/1/disk-groups")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":1,"name":"storage"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::BAD_GATEWAY);
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("deploy a registered DiskDB service"));
}

async fn assert_services(app: &axum::Router) {
    let services = call(app, "GET", "/api/servers", Value::Null).await;
    assert_eq!(services.as_array().unwrap().len(), 18);
    for node in 1..=3 {
        for kind in ["kv", "diskdb", "chunkdb", "diskio", "chunk-kv", "access-server"] {
            assert!(
                services
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["node_id"] == node
                        && entry["service_type"] == kind
                        && entry["pid"].as_u64().is_some()),
                "Node {node} missing {kind}"
            );
        }
    }
}

async fn s3_request(app: &axum::Router, method: &str, path: &str, body: Vec<u8>) -> Vec<u8> {
    let started = std::time::Instant::now();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        app.clone().oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::from(body))
                .unwrap(),
        ),
    )
    .await
    .expect("bounded native S3 response")
    .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    eprintln!("[PHASE] {method} {path}: {}ms", started.elapsed().as_millis());
    assert!(
        status.is_success(),
        "{path}: {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    bytes.to_vec()
}

async fn upload_native_multipart(app: &axum::Router, object: &str) -> Vec<u8> {
    let initialized = s3_request(app, "POST", &format!("{object}?uploads"), vec![]).await;
    let xml = String::from_utf8(initialized).unwrap();
    let upload = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let mut payload = vec![0x51; 5 * 1024 * 1024];
    let tail = vec![0xa3; 1024 * 1024];
    s3_request(
        app,
        "PUT",
        &format!("{object}?uploadId={upload}&partNumber=1"),
        payload.clone(),
    )
    .await;
    s3_request(
        app,
        "PUT",
        &format!("{object}?uploadId={upload}&partNumber=2"),
        tail.clone(),
    )
    .await;
    // Completion validates the actual part digests, not synthetic location records.
    let parts = s3_request(app, "GET", &format!("{object}?uploadId={upload}"), vec![]).await;
    let parts = String::from_utf8(parts).unwrap();
    let etags: Vec<_> = parts
        .split("<ETag>")
        .skip(1)
        .map(|part| part.split_once("</ETag>").unwrap().0)
        .collect();
    assert_eq!(etags.len(), 2);
    let completion = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etags[0], etags[1]);
    s3_request(
        app,
        "POST",
        &format!("{object}?uploadId={upload}"),
        completion.into_bytes(),
    )
    .await;
    payload.extend(tail);
    payload
}

async fn assert_native_locations(app: &axum::Router) {
    let bucket = "/api/access/s3/native-locations";
    s3_request(app, "PUT", bucket, vec![]).await;
    let object = format!("{bucket}/multipart.bin");
    let payload = upload_native_multipart(app, &object).await;
    assert_eq!(s3_request(app, "GET", &object, vec![]).await, payload);
    let inspect = "/api/access/s3-inspect/locations?bucket=native-locations&key=multipart.bin&limit=1";
    let first = call(app, "GET", inspect, Value::Null).await;
    assert_eq!(first["logical_length"], payload.len().to_string());
    assert_eq!(first["locations"].as_array().unwrap().len(), 1);
    assert_eq!(first["locations"][0]["logical_offset"], "0");
    assert_eq!(
        first["locations"][0]["logical_length"],
        (5 * 1024 * 1024).to_string()
    );
    let cursor = first["next_cursor"]
        .as_str()
        .expect("multipart location continuation");
    let cursor: String =
        percent_encoding::utf8_percent_encode(cursor, percent_encoding::NON_ALPHANUMERIC).collect();
    let next_path = format!("{inspect}&cursor={cursor}");
    let second = call(app, "GET", &next_path, Value::Null).await;
    assert_eq!(second["generation"], first["generation"]);
    assert!(second["next_cursor"].is_null());
    assert_eq!(
        second["locations"][0]["logical_offset"],
        (5 * 1024 * 1024).to_string()
    );
    assert_eq!(
        second["locations"][0]["logical_length"],
        (1024 * 1024).to_string()
    );
    assert_eq!(
        first["locations"][0]["chunk_id"],
        second["locations"][0]["chunk_id"]
    );
    let end = first["locations"][0]["offset"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap()
        + first["locations"][0]["length"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
    assert!(
        second["locations"][0]["offset"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= end
    );
    for page in [&first, &second] {
        let id = page["locations"][0]["chunk_id"].as_str().unwrap();
        let detail = call(app, "GET", &format!("/api/chunks/{id}"), Value::Null).await;
        assert_eq!(detail["chunk"]["id_hex"], id);
        assert!(!detail["chunk"]["strips"].as_array().unwrap().is_empty());
    }
    s3_request(app, "PUT", &object, b"replacement".to_vec()).await;
    let response = app
        .clone()
        .oneshot(Request::builder().uri(next_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
}

#[tokio::test]
#[ignore = "Cold normal three-node chain; requires all six installed native server binaries"]
async fn one_rack_three_nodes_provision_all_services_without_metadata_repairs() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("native-console-provisioning");
    let mut state = AppState::with_runtime_root(ConsoleConfig::default(), root.path().to_owned());
    // Speed up only DDB observation cadence; all service deployment policies
    // below retain normal multi-node protection, never test_single_node.
    state.test_mode = true;
    let _services = TestServices(state.clone());
    let app = router(state.clone());
    call(
        &app,
        "POST",
        "/api/racks",
        json!({"id":1,"name":"native acceptance"}),
    )
    .await;
    for node in 1..=3 {
        call(
            &app,
            "POST",
            "/api/nodes",
            json!({"id":node,"rack_id":1,"host":"127.0.0.1","ssh_port":22,"ssh_user":""}),
        )
        .await;
        deploy(&app, node, "kv").await;
    }
    call(&app, "POST", "/api/cluster/init", json!({"nodes":[1,2,3]})).await;
    call(
        &app,
        "POST",
        "/api/stores/0/groups",
        json!({"group_id":1,"replica_id":10,"nodes":[1,2,3]}),
    )
    .await;
    // A group can predate DDB registration. Repeating the same create after
    // registration reconciles its owner, without an administrative bind/owner write.
    pending_group(&app).await;
    let hardware = crowdb_kv_client::HardwareClient::from_shared(state.kv_client().await);
    let binding = hardware.get_bind(1, 1, 1).await.unwrap().unwrap();
    assert_eq!((binding.store_id, binding.group_id), (0, 1));
    assert!(hardware.get_owner(1, 1, 1).await.unwrap().is_none());
    for node in 1..=3 {
        deploy(&app, node, "diskdb").await;
    }
    for node in 1..=3 {
        call(
            &app,
            "POST",
            &format!("/api/nodes/{node}/disk-groups"),
            json!({"id":node,"name":"storage"}),
        )
        .await;
        let device = root.path().join(format!("disk-{node}.img"));
        std::fs::File::create(&device)
            .unwrap()
            .set_len(8 * 1024 * 1024 * 1024)
            .unwrap();
        call(
            &app,
            "POST",
            &format!("/api/nodes/{node}/disk-groups/{node}/disks"),
            json!({
                "disk_id":format!("{node:032x}"),"disk_type":"Hdd","capacity_bytes":8u64*1024*1024*1024,
                "zone_size_bytes":1024*1024*1024,"unit_size_bytes":1024*1024,"device_path":device,
            }),
        )
        .await;
    }
    for node in 1..=3 {
        assert!(hardware.get_owner(1, node, node).await.unwrap().is_some());
    }
    // All Nodes exist before sealing the fixed CDB service ownership plan.
    for kind in ["chunkdb", "diskio", "chunk-kv", "access-server"] {
        for node in 1..=3 {
            deploy(&app, node, kind).await;
        }
    }
    assert_services(&app).await;
    let namespaces = call(&app, "GET", "/api/access/iceberg/v1/namespaces", Value::Null).await;
    assert!(namespaces["namespaces"].is_array());
    // Listing validates native signing and automatic catalog provisioning.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/access/s3/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("ListAllMyBucketsResult"));
    assert_native_locations(&app).await;
}
