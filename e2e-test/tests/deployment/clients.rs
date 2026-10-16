// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::ChunkdbRpcTransport;
use crowdb_protocol::chunkdb::rpc::{ChunkType, ListChunksRequest, Strip, StripType};
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires the complete release deployment and pinned host SDKs"]
async fn official_clients_and_protocol_layout_survive_restart() {
    let root = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let address = crate::common::management::addresses(1)[0].clone();
    let phase = std::env::var("CROWDB_E2E_PHASE").unwrap();
    let credentials = std::fs::read_to_string(root.join("client.env")).unwrap();
    let runtime: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("deployment-runtime.json")).unwrap()).unwrap();
    assert_eq!(runtime["modes"]["secrets/server.env"], "600");
    assert_eq!(runtime["modes"]["secrets/client.env"], "600");
    assert_eq!(runtime["modes"]["crash"], "700");
    for role in ["monitor", "kv"] {
        assert_eq!(runtime["cwd"][role], "/opt/crowdb/data/crash");
    }
    assert!(runtime["ports"].as_object().unwrap().is_empty());
    verify_public_services(&address).await;
    let clients = PathBuf::from(std::env::var("CROWDB_E2E_CLIENT_ROOT").unwrap());
    tokio::task::spawn_blocking(move || {
        for (environment, script) in [
            ("s3-e2e", "s3-client.py"),
            ("s3-e2e", "s3-cli-client.py"),
            ("iceberg-e2e", "iceberg-client.py"),
        ] {
            let mut command = std::process::Command::new("timeout");
            command.args(["120", "pixi", "run", "-e", environment, "python"]);
            command.arg(clients.join("deployment").join(script)).arg(&phase);
            for line in credentials.lines() {
                let (key, value) = line.split_once('=').expect("client environment assignment");
                command.env(key, value);
            }
            command.env("CROWDB_PREVIEW_S3_ENDPOINT", format!("http://{address}:9091"));
            command.env("CROWDB_PREVIEW_ICEBERG_URI", format!("http://{address}:9092"));
            command.env(
                "CROWDB_E2E_SOURCE_ROOT",
                env!("CARGO_MANIFEST_DIR").trim_end_matches("/e2e-test"),
            );
            assert!(
                command.status().unwrap().success(),
                "host {script} {phase} failed"
            );
        }
    })
    .await
    .unwrap();
    verify_layout().await;
    super::witness_with_write(true, std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write")).await;
}

async fn verify_public_services(address: &str) {
    let client = crate::common::management::client();
    let web = format!("http://{address}:9090");
    let authority: serde_json::Value = client
        .get(format!("{web}/api/authority"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(authority["source"], "group0");
    assert_eq!(authority["available"], true);
    let preview: serde_json::Value = client
        .get(format!("{web}/api/preview"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(preview["source"], "group0");
    assert!(!preview["services"].as_array().unwrap().is_empty());
    let stores: serde_json::Value = client
        .get(format!("{web}/api/stores"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(stores
        .as_array()
        .unwrap()
        .iter()
        .any(|store| store["store_id"] == 0));
    for path in ["/api/stores", "/api/racks", "/api/node/admit"] {
        assert_eq!(
            client
                .post(format!("{web}{path}"))
                .json(&serde_json::json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        client
            .get(format!("http://{address}:9093/v1/dataset/open"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
}

async fn verify_layout() {
    let endpoint = format!("{}:12200", crate::common::management::addresses(1)[0]);
    let transport = ChunkdbRpcTransport::new();
    let mut start_token = None;
    let mut seen = [false; 2];
    loop {
        let listed = transport
            .send_list_chunks(
                &endpoint,
                &ListChunksRequest {
                    start_token,
                    max_keys: 256,
                    ..ListChunksRequest::default()
                },
            )
            .await
            .unwrap();
        for chunk in listed.chunks {
            let index = match ChunkType::try_from(chunk.chunk_type) {
                Ok(ChunkType::S3) => 0,
                Ok(ChunkType::IcebergTable) => 1,
                _ => continue,
            };
            seen[index] = true;
            let id = chunk.id.expect("protocol chunk must have an ID");
            assert_eq!(id.high >> 56, u64::try_from(chunk.chunk_type).unwrap());
            assert!(!chunk.strips.is_empty(), "protocol chunk must have a strip");
            for strip in chunk.strips {
                assert_eq!(strip.strip_type, StripType::Mirror as i32);
                assert_eq!(strip.capacity, 1024);
                assert!(!strip.placement_repair_required);
                let Some(Strip::MirrorStrip(mirror)) = strip.strip else {
                    panic!("single-node protocol strip must use mirror I/O");
                };
                assert_eq!(mirror.segments.len(), 1);
            }
        }
        let Some(next) = listed.next_token else {
            break;
        };
        start_token = Some(next);
    }
    assert!(
        seen.into_iter().all(|found| found),
        "both protocol chunk types must exist"
    );
}
