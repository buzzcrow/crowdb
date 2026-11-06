// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/owner_fence_server.rs"]
mod owner_fence_server;

use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, Error, KvCompareObservation, PaxosKvRpcTransport};
use crowdb_protocol::chunk_slot::{ChunkServiceIncarnation, ChunkSlotAuthority};
use owner_fence_server::TestOwnerFenceServer;
use std::time::Duration;

fn observed(error: Error, key: &[u8]) -> KvCompareObservation {
    let Error::CasFailed {
        current_revision,
        observation: Some(observation),
    } = error
    else {
        panic!("expected an observed comparison: {error:?}");
    };
    assert_eq!(observation.key.as_ref(), key);
    assert_eq!(observation.revision, current_revision);
    *observation
}

#[tokio::test]
async fn four_real_conflicts_reuse_versioned_values_without_any_get() {
    let test = TestOwnerFenceServer::start().await;
    test.store.set_get_delay_for_tests(Duration::from_secs(60));
    tokio::time::timeout(Duration::from_secs(5), async {
        let key = b"counter\0\xff";
        let mut revision = 0;
        for count in 1_u64..=4 {
            let value = count.to_le_bytes();
            let competitor = test.client.put(1, 1, key, &value, None).await.unwrap();
            let error = test
                .client
                .put_cas(1, 1, key, b"stale", revision)
                .await
                .unwrap_err();
            let observation = observed(error, key);
            assert_eq!(observation.revision, competitor.revision);
            assert_eq!(observation.value.as_ref(), value);
            revision = observation.revision;
        }
        test.client
            .put_cas(1, 1, key, &5_u64.to_le_bytes(), revision)
            .await
            .unwrap();
    })
    .await
    .expect("CAS observations must not need a delayed GET");
}

#[tokio::test]
async fn absent_and_existing_empty_values_remain_distinct_through_batch_delete() {
    let test = TestOwnerFenceServer::start().await;
    let key = b"empty";
    let raw = PaxosKvRpcTransport::new()
        .send_put_cas(&test.endpoint, key, b"bad", 99, 901, 1, 1, 0, 1)
        .await
        .unwrap();
    assert!(!raw.ok && !raw.not_found);
    assert_eq!(raw.revision, 0);
    assert_eq!(raw.compare_observation.unwrap().key.as_ref(), key);
    let missing = observed(test.client.put_cas(1, 1, key, b"bad", 99).await.unwrap_err(), key);
    assert_eq!(missing.revision, 0);
    assert!(missing.value.is_empty());
    let written = test
        .client
        .put_cas(1, 1, key, b"", missing.revision)
        .await
        .unwrap();
    let deletes = [BatchOp::Delete {
        key: Bytes::from_static(key),
    }];
    let empty = observed(
        test.client
            .batch_write_cas(1, 1, &deletes, key, 0)
            .await
            .unwrap_err(),
        key,
    );
    assert_eq!(empty.revision, written.revision);
    assert_ne!(empty.revision, 0);
    assert!(empty.value.is_empty());
    test.client
        .batch_write_cas(1, 1, &deletes, key, empty.revision)
        .await
        .unwrap();
    let retired = observed(
        test.client
            .put_cas(1, 1, key, b"bad", empty.revision)
            .await
            .unwrap_err(),
        key,
    );
    assert_eq!(retired.revision, 0);
    test.client
        .put_cas(1, 1, key, b"recreated", retired.revision)
        .await
        .unwrap();
}

#[tokio::test]
async fn owner_and_record_conditions_identify_the_key_that_actually_failed() {
    let test = TestOwnerFenceServer::start().await;
    let fence = b"/chunkdb/ownership-fence/0";
    let current = ChunkSlotAuthority::new(4, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), 1)
        .unwrap()
        .to_fence_value();
    let stale = ChunkSlotAuthority::new(3, ChunkServiceIncarnation::try_from([2; 16]).unwrap(), 1)
        .unwrap()
        .to_fence_value();
    let owner = test.client.put_cas(1, 1, fence, &current, 0).await.unwrap();
    let record = b"record";
    let version = test.client.put(1, 1, record, b"v1", None).await.unwrap();
    let ops = [BatchOp::Put {
        key: Bytes::from_static(record),
        value: Bytes::from_static(b"v2"),
    }];
    let failed_owner = observed(
        test.client
            .batch_write_owned(1, 1, &ops, fence, &stale, Some((record, 0)))
            .await
            .unwrap_err(),
        fence,
    );
    assert_eq!(failed_owner.revision, owner.revision);
    assert_eq!(failed_owner.value.as_ref(), current);
    let failed_record = observed(
        test.client
            .batch_write_owned(1, 1, &ops, fence, &current, Some((record, 0)))
            .await
            .unwrap_err(),
        record,
    );
    assert_eq!(failed_record.revision, version.revision);
    assert_eq!(failed_record.value.as_ref(), b"v1");
    test.client
        .batch_write_owned(
            1,
            1,
            &ops,
            fence,
            &current,
            Some((record, failed_record.revision)),
        )
        .await
        .unwrap();
}
