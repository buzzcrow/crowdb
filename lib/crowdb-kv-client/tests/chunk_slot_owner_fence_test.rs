// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/owner_fence_server.rs"]
mod owner_fence_server;

use bytes::Bytes;
use crowdb_kv::rpc::{KvBatchItem, KvErrorCode};
use crowdb_kv_client::{BatchOp, Error, GetOutcome, KvRpcTransport, ReadMode};
use crowdb_protocol::chunk_slot::{ChunkServiceIncarnation, ChunkSlotAuthority};
use owner_fence_server::TestOwnerFenceServer;

const FENCE: &[u8] = b"/chunkdb/ownership-fence/0";

#[tokio::test]
async fn cancelled_handover_drains_one_slot_without_blocking_another() {
    let servers = TestOwnerFenceServer::start().await;
    let client = &servers.client;
    let old = ChunkSlotAuthority::new(4, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), 1)
        .unwrap()
        .to_fence_value();
    let new = ChunkSlotAuthority::new(5, ChunkServiceIncarnation::try_from([2; 16]).unwrap(), 2)
        .unwrap()
        .to_fence_value();
    let initial = client.put_cas(1, 1, FENCE, &old, 0).await.unwrap();
    let other = b"/chunkdb/ownership-fence/1";
    let other_revision = client.put_cas(1, 1, other, &old, 0).await.unwrap().revision;
    let group = servers.store.get_group(1).unwrap();
    let admitted = group.hold_owner_write_for_tests(Bytes::from_static(FENCE));
    let endpoint = servers.endpoint.clone();
    let claim = tokio::spawn(async move {
        KvRpcTransport::new()
            .send_put_cas(&endpoint, FENCE, &new, initial.revision, 901, 1, 1, 0, 1)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !group.owner_change_pending_for_tests(&Bytes::from_static(FENCE)) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!claim.is_finished());
    let batch = [BatchOp::Put {
        key: Bytes::from_static(b"unrelated"),
        value: Bytes::from_static(b"committed"),
    }];
    assert!(matches!(
        client.batch_write_owned(1, 1, &batch, FENCE, &old, None).await,
        Err(Error::CasBusy)
    ));
    client
        .batch_write_owned(1, 1, &batch, other, &old, None)
        .await
        .unwrap();
    assert!(
        matches!(client.get(1, 1, other, ReadMode::Linearizable, None).await.unwrap(), GetOutcome::Found { revision, .. } if revision == other_revision)
    );
    claim.abort();
    drop(admitted);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let outcome = client
                .get(1, 1, FENCE, ReadMode::Linearizable, None)
                .await
                .unwrap();
            if matches!(outcome, GetOutcome::Found { value, .. } if value.as_ref() == new) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        client.batch_write_owned(1, 1, &batch, FENCE, &old, None).await,
        Err(Error::CasFailed { .. })
    ));
}

#[tokio::test]
async fn process_restart_revokes_old_writes_and_preserves_record_cas() {
    let servers = TestOwnerFenceServer::start().await;
    let client = &servers.client;
    let old = ChunkSlotAuthority::new(4, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), 7)
        .unwrap()
        .to_fence_value();
    let new = ChunkSlotAuthority::new(4, ChunkServiceIncarnation::try_from([2; 16]).unwrap(), 8)
        .unwrap()
        .to_fence_value();
    let initial = client.put_cas(1, 1, FENCE, &old, 0).await.unwrap();
    let record = b"chunk-info";
    let batch = [BatchOp::Put {
        key: Bytes::from_static(record),
        value: Bytes::from_static(b"old"),
    }];
    let write = client
        .batch_write_owned(1, 1, &batch, FENCE, &old, Some((record, 0)))
        .await
        .unwrap();
    assert!(matches!(
        client
            .batch_write_owned(1, 1, &batch, FENCE, &old, Some((record, 0)))
            .await,
        Err(Error::CasFailed { .. })
    ));
    // Ordinary owner writes have not changed the fence's revision.
    client.put_cas(1, 1, FENCE, &new, initial.revision).await.unwrap();
    assert!(matches!(
        client
            .batch_write_owned(1, 1, &batch, FENCE, &old, Some((record, write.revision)))
            .await,
        Err(Error::CasFailed { .. })
    ));
    let batch = [BatchOp::Put {
        key: Bytes::from_static(record),
        value: Bytes::from_static(b"new"),
    }];
    client
        .batch_write_owned(1, 1, &batch, FENCE, &new, Some((record, write.revision)))
        .await
        .unwrap();
    assert!(matches!(
        client
            .get(1, 1, record, ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::Found { value, .. } if value.as_ref() == b"new"
    ));
}

#[tokio::test]
async fn ordinary_and_malformed_writes_cannot_change_slot_authority() {
    let servers = TestOwnerFenceServer::start().await;
    let client = &servers.client;
    let value = ChunkSlotAuthority::new(4, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), 1)
        .unwrap()
        .to_fence_value();
    client.put_cas(1, 1, FENCE, &value, 0).await.unwrap();
    assert!(client.put(1, 1, FENCE, b"bypass", None).await.is_err());
    assert!(client.delete(1, 1, FENCE, None).await.is_err());
    for path in [
        FENCE,
        b"/chunkdb/ownership-fence/01",
        b"/chunkdb/ownership-fence/1024",
    ] {
        assert!(matches!(
            client.put_cas(1, 1, path, b"invalid", 0).await,
            Err(Error::CasFailed { .. })
        ));
    }
    for batch in [
        vec![BatchOp::Delete {
            key: Bytes::from_static(FENCE),
        }],
        vec![BatchOp::Put {
            key: Bytes::from_static(FENCE),
            value: Bytes::from_static(b"invalid"),
        }],
        vec![
            BatchOp::Put {
                key: Bytes::from_static(b"record"),
                value: Bytes::from_static(b"value"),
            },
            BatchOp::Put {
                key: Bytes::from_static(FENCE),
                value: Bytes::copy_from_slice(&value),
            },
        ],
    ] {
        let key = match &batch[0] {
            BatchOp::Put { key, .. } | BatchOp::Delete { key } => key,
        };
        assert!(client.batch_write(1, 1, &batch).await.is_err());
        assert!(matches!(
            client.batch_write_cas(1, 1, &batch, key, 0).await,
            Err(Error::CasFailed { .. })
        ));
    }
    assert!(matches!(
        client
            .get(1, 1, FENCE, ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::Found { value: actual, .. } if actual.as_ref() == value
    ));
}

#[tokio::test]
async fn raw_owner_rpc_rejects_malformed_authority_and_cross_namespace_mutation() {
    let servers = TestOwnerFenceServer::start().await;
    let endpoint = &servers.endpoint;
    let transport = KvRpcTransport::new();
    let value = ChunkSlotAuthority::new(4, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), 1)
        .unwrap()
        .to_fence_value();
    let client = &servers.client;
    client.put_cas(1, 1, FENCE, &value, 0).await.unwrap();
    client.seed_leader(1, 0, endpoint.clone());
    assert!(matches!(
        client.put_cas(1, 0, FENCE, &value, 0).await,
        Err(Error::CasFailed { .. })
    ));
    let record = KvBatchItem {
        key: Bytes::from_static(b"record"),
        value: Bytes::from_static(b"value"),
        is_delete: false,
    };
    let response = transport
        .send_batch_write_owned(
            endpoint,
            std::slice::from_ref(&record),
            FENCE,
            &value,
            None,
            900,
            99,
            99,
            0,
            0,
        )
        .await
        .unwrap();
    assert_eq!(response.error_code, KvErrorCode::KvErrorCasFailed as i32);
    let requests = [
        (
            b"/chunkdb/ownership-fence/01".as_slice(),
            value.as_slice(),
            vec![record.clone()],
        ),
        (FENCE, b"old-owner".as_slice(), vec![record.clone()]),
        (
            FENCE,
            value.as_slice(),
            vec![
                record.clone(),
                KvBatchItem {
                    key: Bytes::from_static(b"/diskdb/ownership-fence/1/1/1"),
                    value: Bytes::from_static(b"bypass"),
                    is_delete: false,
                },
            ],
        ),
        (
            FENCE,
            value.as_slice(),
            vec![
                record,
                KvBatchItem {
                    key: Bytes::from_static(FENCE),
                    value: Bytes::copy_from_slice(&value),
                    is_delete: false,
                },
            ],
        ),
    ];
    for (index, (key, expected, items)) in requests.iter().enumerate() {
        let id = u64::try_from(index).unwrap() + 1;
        let response = transport
            .send_batch_write_owned(endpoint, items, key, expected, None, 900, id, id, 0, 1)
            .await
            .unwrap();
        assert!(!response.ok);
        assert_eq!(response.error_code, KvErrorCode::KvErrorCasFailed as i32);
    }
    assert!(matches!(
        client
            .get(1, 1, b"record", ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
}
