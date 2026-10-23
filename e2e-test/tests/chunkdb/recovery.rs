// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_kv_client::{GetOutcome, ReadMode, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState, DeleteChunkRequest, QueryChunkRequest, SealChunkRequest,
};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires the owned protected Chunkdb crash fixture"]
async fn crash_recovery_preserves_exact_chunk_records_and_placement() {
    let client = ChunkdbClient::new(
        ServiceRegistryClient::new(crate::common::hardware::kv_client()),
        Arc::new(ChunkdbRpcTransport::new()),
    );
    let path =
        std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap()).join("chunkdb-witness.json");
    if std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write") {
        let chunk = super::lifecycle::allocate_protected(&client).await;
        std::fs::write(
            path,
            serde_json::to_vec(&json!({
                "record": record(&chunk).await, "chunk": chunk
            }))
            .unwrap(),
        )
        .unwrap();
    } else {
        let witness: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let before: Chunk = serde_json::from_value(witness["chunk"].clone()).unwrap();
        let id = before.id.unwrap();
        let after = client
            .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap()
            .chunk
            .unwrap();
        assert_eq!(
            after, before,
            "acknowledged placement survives all dependency crashes"
        );
        assert_eq!(
            record(&after).await,
            witness["record"],
            "exact metadata bytes and revision remain durable"
        );
        let sealed = client
            .seal_chunk(SealChunkRequest {
                chunk_id: Some(id),
                seal_bytes: 100 * 1024 - 17,
            })
            .await
            .unwrap()
            .chunk
            .unwrap();
        assert_eq!(sealed.acknowledged_cursor, 100 * 1024 - 17);
        assert_eq!(sealed.strips.len(), 1);
        let deleted = client
            .delete_chunk(DeleteChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap()
            .chunk
            .unwrap();
        assert_eq!(deleted.state, ChunkState::Deleted as i32);
        assert!(deleted.strips.is_empty());
    }
}

async fn record(chunk: &Chunk) -> serde_json::Value {
    let id = chunk.id.unwrap();
    let mut key = b"/chunk/".to_vec();
    key.extend_from_slice(&id.high.to_be_bytes());
    key.extend_from_slice(&id.low.to_be_bytes());
    let kv = crate::common::hardware::kv_client();
    let GetOutcome::Found { value, revision } =
        kv.get(0, 1, &key, ReadMode::Linearizable, None).await.unwrap()
    else {
        panic!("acknowledged chunk metadata is missing")
    };
    json!({ "bytes": value.as_ref(), "revision": revision })
}
