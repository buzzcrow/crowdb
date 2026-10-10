// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Packaged three-voter KV CRUD, persistence and isolation acceptance.

mod common;

use bytes::Bytes;
use crowdb_kv::rpc::{KvBatchItem, KvBatchWriteRequest, KvDeleteRequest, KvGetRequest, KvSetRequest};

use common::{run_kv_op_with_retry, KvOp};

#[tokio::test]
#[ignore = "requires the owned container fixture; run pixi run test-container-e2e"]
#[allow(clippy::too_many_lines)]
async fn e2e_three_node_cluster_kv_put_batch_delete() {
    let group_id = 1;
    let nodes = common::nodes();
    if std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("verify") {
        common::verify_persisted(&nodes).await;
        return;
    }

    for node in &nodes {
        let remotes = common::remotes(node, group_id).await;
        assert_eq!(
            remotes["remotes"].as_array().unwrap().len(),
            2,
            "node {} should have 2 remotes",
            node.node_id
        );
    }

    let resp = run_kv_op_with_retry(
        &nodes,
        group_id,
        &KvOp::Put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"hello"),
            value: Bytes::from_static(b"world"),
            seq: 1,
            ttl_ms: 0,
            client_id: 100,
            request_id: 1001,
            request_create_ms: 10001,
            group_id,
        }),
    )
    .await;
    assert!(resp.ok, "put should succeed: {}", resp.error);

    // Get: read the value we just wrote through the leader. Verifies the
    // Paxos chosen value reached the local-replica learner store on the
    // node serving the read. The handler returns ok=true with the bytes
    // in `value` for a hit (see `kv_service::get`).
    let resp = run_kv_op_with_retry(
        &nodes,
        group_id,
        &KvOp::Get(KvGetRequest {
            version: 1,
            key: Bytes::from_static(b"hello"),
            request_id: 1011,
            request_create_ms: 10011,
            group_id,
            read_mode: 0,
            min_slot: 0,
        }),
    )
    .await;
    assert!(resp.ok, "get should succeed: {}", resp.error);
    assert_eq!(resp.value, Bytes::from_static(b"world"));

    let resp = run_kv_op_with_retry(
        &nodes,
        group_id,
        &KvOp::BatchWrite(KvBatchWriteRequest {
            version: 1,
            items: vec![
                KvBatchItem {
                    key: Bytes::from_static(b"hello"),
                    value: Bytes::from_static(b"updated"),
                    is_delete: false,
                },
                KvBatchItem {
                    key: Bytes::from_static(b"foo"),
                    value: Bytes::from_static(b"bar"),
                    is_delete: false,
                },
            ],
            seq: 2,
            client_id: 100,
            request_id: 1002,
            request_create_ms: 10002,
            group_id,
        }),
    )
    .await;
    assert!(resp.ok, "batch should succeed: {}", resp.error);

    let resp = run_kv_op_with_retry(
        &nodes,
        group_id,
        &KvOp::Delete(KvDeleteRequest {
            version: 1,
            key: Bytes::from_static(b"hello"),
            seq: 3,
            client_id: 100,
            request_id: 1003,
            request_create_ms: 10003,
            group_id,
        }),
    )
    .await;
    assert!(resp.ok, "delete should succeed: {}", resp.error);

    // Get-after-Delete: the chosen tombstone propagated to the learner.
    // `kv_get` for a missing key returns `ok=false, not_found=true` (see
    // `PxKvStore::kv_get`); only the `not_found` flag is asserted here.
    let resp = run_kv_op_with_retry(
        &nodes,
        group_id,
        &KvOp::Get(KvGetRequest {
            version: 1,
            key: Bytes::from_static(b"hello"),
            request_id: 1013,
            request_create_ms: 10013,
            group_id,
            read_mode: 0,
            min_slot: 0,
        }),
    )
    .await;
    assert!(
        resp.not_found,
        "deleted key must read as not_found: value={:?}",
        resp.value
    );
    assert!(resp.value.is_empty());
    common::write_witness(&nodes).await;
}
