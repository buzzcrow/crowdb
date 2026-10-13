// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_kv::DomainMonitorDescriptor;
use crowdb_protocol::key::{ChunkKvSplitKey, DomainMonitorKey, TextKey};
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "requires the complete release single-node profile"]
async fn single_node_disables_local_chunk_kv_split_planning() {
    super::witness_with_write(true, true).await;
    let address = crate::common::management::addresses(1)[0].clone();
    let client = CrowdbKvClient::new(ClientConfig::new(vec![format!("http://{address}:10000")]));
    let key = DomainMonitorKey {
        domain: "chunk-kv".into(),
    }
    .to_path();
    let GetOutcome::Found { value, .. } = client
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("single-node Chunk-KV monitor descriptor is missing");
    };
    let monitor: DomainMonitorDescriptor = serde_json::from_slice(&value).unwrap();
    assert!(monitor.chunk_kv_range_balance.is_none());
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let observed = client
            .scan(
                0,
                0,
                ChunkKvSplitKey::text_prefix_all().as_bytes(),
                &[],
                &[],
                1,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await
            .unwrap();
        assert!(
            observed.items.is_empty(),
            "disabled balancing created a split transition"
        );
        assert!(!observed.truncated && !observed.timed_out);
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mounts: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("mounts.json")).unwrap()).unwrap();
    let logs = std::path::PathBuf::from(mounts["host_data"][0].as_str().unwrap()).join("log/kv");
    let mut count = 0;
    for entry in std::fs::read_dir(logs).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            count += 1;
            let body = std::fs::read(path).unwrap();
            assert!(!body
                .windows(b"local split planned".len())
                .any(|window| window == b"local split planned"));
        }
    }
    assert!(count > 0, "the actual KV service must produce diagnostic logs");
    super::witness_with_write(true, false).await;
}
