// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cold normal-mode acceptance; kept outside the fast page suite.

use axum::{body::Body, http::Request};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

#[path = "common/native_balance.rs"]
mod native_balance;
#[path = "common/native_chunks.rs"]
mod native_chunks;
#[path = "common/native_journal.rs"]
mod native_journal;
#[path = "common/native_load.rs"]
mod native_load;

struct TestServices(AppState);

async fn assert_initialization_budget(state: &AppState) {
    let initialization = state.kv_client().await.metrics();
    assert!(
        initialization.not_leader_hint_followed < 100,
        "cold initialization must not spin on leader hints: {initialization:?}"
    );
}

impl Drop for TestServices {
    fn drop(&mut self) {
        if std::thread::panicking() {
            preserve_failure_logs(&self.0);
        }
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
    // Each concurrent fixture reserves its listeners through the shared test allocator.
    for field in ["http_port", "rpc_port", "s3_port", "health_port"] {
        if body.get(field).is_some() {
            let port = if kind == "diskdb" {
                crowdb_protocol::port::alloc::alloc_test_port_range(crowdb_protocol::ServicePort::Web, 3)[2]
            } else {
                crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::Web)
            };
            body[field] = json!(port);
        }
    }
    let path = match kind {
        "paxos-kv" => {
            body = json!({"rest_port": body["http_port"], "rpc_port": body["rpc_port"],
                "kv_backend": "block", "wal_backend": "block-device"});
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

async fn group_before_owner(app: &axum::Router) {
    call(
        app,
        "POST",
        "/api/nodes/1/disk-groups",
        json!({"id":1,"store_id":1,"group_id":1,"name":"storage"}),
    )
    .await;
}

async fn assert_mixed_geometry_rejected(app: &axum::Router, state: &AppState) {
    for kind in ["chunkdb", "chunk-kv"] {
        let mut body = call(app, "GET", "/api/deployment-defaults?node_id=1", Value::Null).await;
        body = body[kind].clone();
        body["kind"] = json!(kind);
        body["test_single_node"] = json!(false);
        if kind == "chunk-kv" {
            body["metadata_store_id"] = json!(0);
            body["bootstrap_group_id"] = json!(1);
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/nodes/1/services/deploy")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        let error = String::from_utf8_lossy(&bytes);
        assert!(error.contains("uniform allocation unit"), "{error}");
        assert!(error.contains("131072") && error.contains("1048576"), "{error}");
    }
    assert_eq!(
        state.config.read().unwrap().servers.len(),
        6,
        "no incompatible service spawned"
    );
}

async fn add_native_disk(app: &axum::Router, node: u64, root: &std::path::Path, inspection_only: bool) {
    let device = root.join(format!("disk-{node}.img"));
    // Repeated production splits reserve whole 256-MiB mirrored Chunks.
    // Keep the ordinary browser geometry, but provision the slow fixture for
    // every retained writer and its split/transfer preparation artifacts.
    let gib = if std::env::var_os("CROWDB_NATIVE_COUNT_ACCEPTANCE").is_some() {
        256
    } else if node == 1 {
        80
    } else if inspection_only {
        256
    } else {
        8
    };
    let capacity = gib * 1024 * 1024 * 1024;
    let unit_size = if node != 1 && std::env::var_os("CROWDB_NATIVE_MIXED_UNITS").is_some() {
        1024u64 * 1024
    } else {
        128u64 * 1024
    };
    std::fs::File::create(&device).unwrap().set_len(capacity).unwrap();
    call(
        app,
        "POST",
        &format!("/api/nodes/{node}/disk-groups/{node}/disks"),
        json!({"disk_id":format!("{node:032x}"),"disk_type":"Hdd","capacity_bytes":capacity,
            "zone_size_bytes":1024*1024*1024,"unit_size_bytes":unit_size,"device_path":device}),
    )
    .await;
}

async fn assert_services(app: &axum::Router) {
    let services = call(app, "GET", "/api/servers", Value::Null).await;
    assert_eq!(services.as_array().unwrap().len(), 18);
    for node in 1..=3 {
        for kind in [
            "paxos-kv",
            "diskdb",
            "chunkdb",
            "diskio",
            "chunk-kv",
            "access-server",
        ] {
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
    let mut payload = vec![0x51; 8 * 1024 * 1024];
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
    assert_eq!(
        etags.len(),
        2,
        "ListParts response after two successful uploads: {parts}"
    );
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

async fn assert_native_locations(app: &axum::Router, state: &AppState) {
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
        (8 * 1024 * 1024).to_string()
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
        (8 * 1024 * 1024).to_string()
    );
    assert_eq!(
        second["locations"][0]["logical_length"],
        (1024 * 1024).to_string()
    );
    assert_ne!(
        first["locations"][0]["chunk_id"],
        second["locations"][0]["chunk_id"]
    );
    assert_eq!(first["locations"][0]["offset"], "0");
    for page in [&first, &second] {
        let id = page["locations"][0]["chunk_id"].as_str().unwrap();
        let detail = call(app, "GET", &format!("/api/chunks/{id}"), Value::Null).await;
        assert_eq!(detail["chunk"]["id_hex"], id);
        assert!(!detail["chunk"]["strips"].as_array().unwrap().is_empty());
    }
    let id = first["locations"][0]["chunk_id"].as_str().unwrap();
    let chunkdb = crowdb_chunkdb_client::ChunkdbClient::new(
        crowdb_kv_client::ServiceRegistryClient::from_shared(state.kv_client().await),
        std::sync::Arc::new(crowdb_chunkdb_client::ChunkdbRpcTransport::new()),
    );
    let chunk = chunkdb
        .query_chunk(crowdb_protocol::chunkdb::rpc::QueryChunkRequest {
            chunk_id: Some(crowdb_protocol::common::ChunkId {
                high: u64::from_str_radix(&id[..16], 16).unwrap(),
                low: u64::from_str_radix(&id[16..], 16).unwrap(),
            }),
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    let physical = first["locations"][0]["length"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_ne!(physical % 1024, 0);
    assert_eq!(chunk.acknowledged_cursor, physical);
    s3_request(app, "PUT", &object, b"replacement".to_vec()).await;
    let response = app
        .clone()
        .oneshot(Request::builder().uri(next_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
}

async fn assert_native_browser_diagnostics(app: axum::Router, chunks: Option<Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let ui = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("ui");
    let mut command = tokio::process::Command::new("pixi");
    if let Some(chunks) = chunks {
        command.env("CROWDB_NATIVE_CHUNK_FIXTURE", chunks.to_string());
    }
    if let Ok(grep) = std::env::var("CROWDB_NATIVE_UI_E2E_GREP") {
        command.args([
            "run",
            "npx",
            "playwright",
            "test",
            "--config=e2e/nativeDiagnostics.config.ts",
            "--grep",
            &grep,
        ]);
    } else {
        command.args([
            "run",
            "npx",
            "playwright",
            "test",
            "--config=e2e/nativeDiagnostics.config.ts",
        ]);
    }
    let status = command
        .env("CROWDB_WEB_E2E_BASE_URL", base)
        .current_dir(ui)
        .status()
        .await
        .unwrap();
    server.abort();
    assert!(status.success(), "native browser diagnostics failed");
}

#[tokio::test]
#[cfg_attr(
    target_os = "macos",
    ignore = "Requires installed native KV and DiskIO binaries"
)]
async fn node_diskio_discovers_disk_groups_created_after_deployment() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("late-diskio-groups");
    let state = AppState::with_runtime_root(ConsoleConfig::default(), root.path().to_owned());
    let _services = TestServices(state.clone());
    let app = router(state.clone());
    assert_eq!(
        call(&app, "GET", "/api/group0-readiness", Value::Null).await["ready"],
        false
    );
    call(&app, "POST", "/api/racks", json!({"id":1})).await;
    call(
        &app,
        "POST",
        "/api/nodes",
        json!({"id":1,"rack_id":1,"host":"127.0.0.1"}),
    )
    .await;
    deploy(&app, 1, "paxos-kv").await;
    call(&app, "POST", "/api/cluster/init", json!({"nodes":[1]})).await;
    assert_eq!(
        call(&app, "GET", "/api/group0-readiness", Value::Null).await["ready"],
        true
    );
    call(
        &app,
        "POST",
        "/api/stores/0/groups",
        json!({"group_id":1,"replica_id":10,"nodes":[1]}),
    )
    .await;
    let mut body =
        call(&app, "GET", "/api/deployment-defaults?node_id=1", Value::Null).await["diskio"].clone();
    body["rpc_port"] = json!(crowdb_protocol::port::alloc::alloc_test_port(
        crowdb_protocol::ServicePort::Web
    ));
    body["kind"] = json!("diskio");
    call(&app, "POST", "/api/nodes/1/services/deploy", body).await;
    let registry = crowdb_kv_client::ServiceRegistryClient::from_shared(state.kv_client().await);
    let initial = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            let instances = registry.read_all_diskio_instances().await.unwrap();
            if !instances.is_empty() {
                break instances;
            }
        }
    })
    .await
    .expect("node-local DiskIO registers after deployment");
    assert_eq!(initial.len(), 1);
    assert!(initial[0]
        .1
        .extra
        .as_ref()
        .unwrap()
        .diskdb
        .as_ref()
        .unwrap()
        .owned_dg_ids
        .is_empty());
    create_late_diskio_groups(&app).await;
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            interval.tick().await;
            let instances = registry.read_all_diskio_instances().await.unwrap();
            let owned = &instances[0]
                .1
                .extra
                .as_ref()
                .unwrap()
                .diskdb
                .as_ref()
                .unwrap()
                .owned_dg_ids;
            if owned == &[11, 12] {
                break;
            }
        }
    })
    .await
    .expect("node-local DiskIO discovers both late groups");
    assert_eq!(
        call(&app, "GET", "/api/chunk-storage-readiness", Value::Null).await["ready"],
        true
    );
}

async fn create_late_diskio_groups(app: &axum::Router) {
    for group in [11, 12] {
        call(
            app,
            "POST",
            "/api/nodes/1/disk-groups",
            json!({"id":group,"store_id":0,"group_id":1}),
        )
        .await;
        call(
            app,
            "POST",
            &format!("/api/nodes/1/disk-groups/{group}/disks"),
            json!({"disk_id":format!("{group:032x}"),"disk_type":"Hdd","capacity_bytes":1_073_741_824_u64,
                "zone_size_bytes":1_073_741_824_u64,"unit_size_bytes":1_048_576,"device_path":""}),
        )
        .await;
    }
}

#[tokio::test]
#[cfg_attr(
    target_os = "macos",
    ignore = "Cold normal three-node chain; requires all six installed native server binaries"
)]
async fn one_rack_three_nodes_provision_all_services_without_metadata_repairs() {
    native_cluster(false).await;
}

#[tokio::test]
#[cfg_attr(
    target_os = "macos",
    ignore = "Native Page and Iceberg inspection; requires all six installed server binaries"
)]
async fn native_page_and_iceberg_inspection() {
    native_cluster(true).await;
}

async fn native_cluster(inspection_only: bool) {
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
        deploy(&app, node, "paxos-kv").await;
    }
    call(&app, "POST", "/api/cluster/init", json!({"nodes":[1,2,3]})).await;
    assert_initialization_budget(&state).await;
    // An ordinary data destination may live outside the system Store.
    call(&app, "POST", "/api/stores", json!({"store_id":1,"nodes":[1,2,3]})).await;
    call(
        &app,
        "POST",
        "/api/stores/1/groups",
        json!({"group_id":1,"replica_id":10,"nodes":[1,2,3]}),
    )
    .await;
    // A group can predate DDB registration. Live DiskDB instances acquire
    // ownership after registration, without an administrative owner write.
    group_before_owner(&app).await;
    let hardware = crowdb_kv_client::HardwareClient::from_shared(state.kv_client().await);
    let binding = hardware.get_bind(1, 1, 1).await.unwrap().unwrap();
    assert_eq!((binding.store_id, binding.group_id), (1, 1));
    // Adding a lower Store destination must not rewrite an established binding.
    call(
        &app,
        "POST",
        "/api/stores/0/groups",
        json!({"group_id":1,"replica_id":20,"nodes":[1,2,3]}),
    )
    .await;
    assert!(hardware.get_owner(1, 1, 1).await.unwrap().is_none());
    for node in 1..=3 {
        deploy(&app, node, "diskdb").await;
    }
    for node in 1..=3 {
        call(
            &app,
            "POST",
            &format!("/api/nodes/{node}/disk-groups"),
            json!({"id":node,"store_id":u64::from(node == 1),"group_id":1,"name":"storage"}),
        )
        .await;
        add_native_disk(&app, node, root.path(), inspection_only).await;
    }
    assert_bound_groups(&app, &hardware).await;
    if std::env::var_os("CROWDB_NATIVE_MIXED_UNITS").is_some() {
        assert_mixed_geometry_rejected(&app, &state).await;
        return;
    }
    if !deploy_native_services(&app).await {
        return;
    }
    assert_services(&app).await;
    if std::env::var_os("CROWDB_NATIVE_JOURNAL_WINDOWS").is_some() {
        native_journal::TestNativeJournal::seed(&app, &state).await;
        assert_native_browser_diagnostics(app.clone(), None).await;
        return;
    }
    if inspection_only {
        native_balance::TestNativeBalance::settle_for_inspection(&state).await;
        let chunks = Some(native_chunks::seed(&state).await);
        assert_native_browser_diagnostics(app.clone(), chunks).await;
        return;
    }
    if std::env::var_os("CROWDB_NATIVE_COUNT_ACCEPTANCE").is_some() {
        assert_native_balance(&app, &state).await;
        return;
    }
    if std::env::var_os("CROWDB_NATIVE_LOAD_ACCEPTANCE").is_some() {
        native_load::TestNativeLoad::verify(&app, &state).await;
        return;
    }
    assert_native_access(&app).await;
    assert_native_locations(&app, &state).await;
    if std::env::var_os("CROWDB_NATIVE_UI_E2E").is_some() {
        assert_native_browser_diagnostics(app.clone(), None).await;
    }
    assert_native_restarts(&app, &state).await;
    assert_native_locations(&app, &state).await;
    if std::env::var_os("CROWDB_NATIVE_UI_E2E").is_some() {
        let chunks = native_chunk_windows(&state).await;
        assert_native_browser_diagnostics(app.clone(), chunks).await;
    }
}

async fn native_chunk_windows(state: &AppState) -> Option<Value> {
    if std::env::var_os("CROWDB_NATIVE_DATA_WINDOWS").is_some() {
        Some(native_chunks::seed(state).await)
    } else {
        None
    }
}

async fn assert_native_balance(app: &axum::Router, state: &AppState) {
    let browser = std::env::var_os("CROWDB_NATIVE_TRANSITION_ACCEPTANCE")
        .map(|_| tokio::spawn(assert_native_browser_diagnostics(app.clone(), None)));
    native_balance::TestNativeBalance::verify(state).await;
    if let Some(browser) = browser {
        browser.await.unwrap();
    }
}

async fn assert_native_access(app: &axum::Router) {
    let namespaces = call(app, "GET", "/api/access/iceberg/v1/namespaces", Value::Null).await;
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
}

async fn assert_bound_groups(app: &axum::Router, hardware: &crowdb_kv_client::HardwareClient) {
    for node in 1..=3 {
        wait_for_native_owner(hardware, node).await;
        let binding = hardware.get_bind(1, node, node).await.unwrap().unwrap();
        let expected_store = u64::from(node == 1);
        assert_eq!((binding.store_id, binding.group_id), (expected_store, 1));
        // An exact-ID retry preserves both the selected binding and owner.
        let owner = hardware.get_owner(1, node, node).await.unwrap();
        if node == 3 {
            // Reproduce persisted partial state using real authoritative records.
            hardware.remove_bind(1, node, node).await.unwrap();
            hardware.remove_owner(1, node, node).await.unwrap();
            assert!(hardware.get_bind(1, node, node).await.unwrap().is_none());
            assert!(hardware.get_owner(1, node, node).await.unwrap().is_none());
            assert_missing_binding_requires_repair(app, node, expected_store).await;
        }
        call(
            app,
            "POST",
            &format!("/api/nodes/{node}/disk-groups"),
            json!({"id":node,"store_id":expected_store,"group_id":1,"name":"storage"}),
        )
        .await;
        wait_for_native_owner(hardware, node).await;
        let renewed = hardware.get_owner(1, node, node).await.unwrap().unwrap();
        let previous = owner.unwrap();
        assert_eq!((renewed.rack_id, renewed.node_id, renewed.dg_id), (1, node, node));
        if node != 3 {
            assert_eq!(renewed.instance_id, previous.instance_id);
        }
        let repaired = hardware.get_bind(1, node, node).await.unwrap().unwrap();
        assert_eq!((repaired.store_id, repaired.group_id), (expected_store, 1));
        assert!(renewed.lease_expiry_ms >= previous.lease_expiry_ms);
    }
}

async fn assert_missing_binding_requires_repair(app: &axum::Router, node: u64, store: u64) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/nodes/{node}/disk-groups"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"id":node,"store_id":store,"group_id":1,"name":"storage"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("Existing DiskGroup binding differs or is absent"));
    call(
        app,
        "PUT",
        &format!("/api/disk-groups/1/{node}/{node}/bind"),
        json!({"store_id":store,"group_id":1}),
    )
    .await;
}

async fn wait_for_native_owner(hardware: &crowdb_kv_client::HardwareClient, node: u64) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            interval.tick().await;
            if hardware.get_owner(1, node, node).await.unwrap().is_some() {
                break;
            }
        }
    })
    .await
    .expect("live DiskDB acquires the configured disk group");
}

async fn assert_native_restarts(app: &axum::Router, state: &AppState) {
    for kind in [
        "paxos-kv",
        "diskdb",
        "chunkdb",
        "diskio",
        "chunk-kv",
        "access-server",
    ] {
        let before = call(app, "GET", "/api/servers", Value::Null).await;
        let entry = before
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["service_type"] == kind && entry["node_id"].as_u64() == Some(1))
            .expect("actual service on Node 1");
        let id = entry["id"].as_str().unwrap();
        let old_pid = u32::try_from(entry["pid"].as_u64().unwrap()).unwrap();
        let path = match kind {
            "paxos-kv" => "/api/nodes/1/server/restart".to_owned(),
            "diskdb" => "/api/nodes/1/diskdb/restart".to_owned(),
            _ => format!("/api/services/{id}/restart"),
        };
        let restarted = call(app, "POST", &path, json!({})).await;
        let new_pid = u32::try_from(restarted["pid"].as_u64().unwrap()).unwrap();
        assert_ne!(new_pid, old_pid);
        assert!(!crowdb_console_shared::lifecycle::process_is_alive(old_pid));
        assert!(crowdb_console_shared::lifecycle::process_is_alive(new_pid));
        let after = call(app, "GET", "/api/servers", Value::Null).await;
        assert_eq!(after.as_array().unwrap().len(), 18);
        let matches: Vec<_> = after
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["id"] == id)
            .collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["pid"], new_pid);
        let metrics = state.kv_client().await.metrics();
        eprintln!(
            "[PHASE] {kind} restart client state: hints={} unknown={} transport={} exhausted={} leader={:?}",
            metrics.not_leader_hint_followed,
            metrics.unknown_leader_wait,
            metrics.transport_error_retry,
            metrics.retries_exhausted,
            state.monitor_cache.group0_leader_endpoint().await
        );
        assert_eq!(
            s3_request(
                app,
                "GET",
                "/api/access/s3/native-locations/multipart.bin",
                vec![]
            )
            .await,
            b"replacement",
            "durable S3 object remains readable after {kind} restarts"
        );
    }
    assert_eq!(
        s3_request(
            app,
            "GET",
            "/api/access/s3/native-locations/multipart.bin",
            vec![]
        )
        .await,
        b"replacement"
    );
}

fn preserve_failure_logs(state: &AppState) {
    for entry in &state.config.read().unwrap().servers {
        if let Some(pid) = entry.pid {
            let status = std::fs::read_to_string(format!("/proc/{pid}/stat"));
            eprintln!("[PHASE] failed native service {} pid={pid}: {status:?}", entry.id);
        }
    }
    let root = &state.runtime_root;
    let target = crowdb_test_harness::test_dirs::artifacts_root()
        .join(format!("native-restart-failure-{}", std::process::id()));
    for node in 1..=3 {
        let source = root.join(format!("N-{node}/log"));
        let destination = target.join(format!("N-{node}"));
        std::fs::create_dir_all(&destination).unwrap();
        if let Ok(entries) = std::fs::read_dir(source) {
            for entry in entries {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_file() {
                    std::fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
                }
            }
        }
    }
    for (id, launch) in &state.config.read().unwrap().local_launches {
        let source = std::path::Path::new(&launch.workdir).join("log");
        let destination = target.join(id);
        std::fs::create_dir_all(&destination).unwrap();
        if let Ok(entries) = std::fs::read_dir(source) {
            for entry in entries {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_file() {
                    std::fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
                }
            }
        }
    }
    eprintln!("[PHASE] native failure logs: {}", target.display());
}

async fn deploy_native_services(app: &axum::Router) -> bool {
    // All Nodes exist before sealing the fixed CDB service ownership plan.
    for kind in ["diskio", "chunkdb", "chunk-kv", "access-server"] {
        if kind == "chunkdb" {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    interval.tick().await;
                    if call(app, "GET", "/api/chunk-storage-readiness", Value::Null).await["ready"] == true {
                        break;
                    }
                }
            })
            .await
            .expect("live DiskIO ownership before ChunkDB");
        }
        for node in 1..=3 {
            deploy(app, node, kind).await;
        }
        if kind == "chunkdb" && std::env::var_os("CROWDB_NATIVE_PLAN_PREREQUISITES").is_some() {
            assert_native_browser_diagnostics(app.clone(), None).await;
            return false;
        }
    }
    true
}
