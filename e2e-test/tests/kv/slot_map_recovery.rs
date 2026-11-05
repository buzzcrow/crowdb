// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::slot_map::{paginated_services, TestGroup0};
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, ChunkSlotMapClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_slot::ChunkSlot;
use crowdb_protocol::key::{ChunkServiceSlotsKey, ChunkSlotMapHeadKey, TextKey};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires the owned release-container Group-0 slot map profile"]
async fn interrupted_large_bootstrap_resumes_after_crash() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let map = paginated_services();
    let head = ChunkSlotMapHeadKey::Service.to_path();
    let phase = std::env::var("CROWDB_E2E_PHASE").unwrap();
    if phase == "write" {
        let mut bindings = map.bindings().to_vec();
        bindings.sort_by_key(|binding| {
            ChunkServiceSlotsKey {
                instance_id: binding.owner,
            }
            .to_path()
        });
        let intent = serde_json::json!({
            "initializing": { "head": map.head(), "bindings": bindings }, "next": 63
        });
        let mut operations: Vec<_> = bindings[..63]
            .iter()
            .map(|binding| BatchOp::Put {
                key: Bytes::from(
                    ChunkServiceSlotsKey {
                        instance_id: binding.owner,
                    }
                    .to_path(),
                ),
                value: Bytes::from(serde_json::to_vec(binding).unwrap()),
            })
            .collect();
        operations.push(BatchOp::Put {
            key: Bytes::from(head.clone()),
            value: Bytes::from(serde_json::to_vec(&intent).unwrap()),
        });
        test.kv
            .batch_write_cas(0, 0, &operations, head.as_bytes(), 0)
            .await
            .unwrap();
        assert!(client.read_service().await.is_err());
        return;
    }
    assert_eq!(phase, "verify");
    let GetOutcome::Found { value, .. } = test
        .kv
        .get(0, 0, head.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("acknowledged bootstrap progress was lost on restart");
    };
    let intent: serde_json::Value = serde_json::from_slice(&value).unwrap();
    assert_eq!(intent["next"], 63);
    assert_eq!(intent["initializing"]["bindings"].as_array().unwrap().len(), 300);
    assert!(client.read_service().await.is_err());
    client.initialize_service(&map).await.unwrap();
    client.initialize_service(&map).await.unwrap();
    let actual = client.read_service().await.unwrap();
    assert_eq!(actual.bindings().len(), 300);
    assert_eq!(
        actual
            .bindings()
            .iter()
            .filter(|binding| binding.slots.is_empty())
            .count(),
        299
    );
    assert!(ChunkSlot::all().all(|slot| actual.owner(slot) == 1));
}
