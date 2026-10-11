// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/owner_fence_server.rs"]
mod owner_fence_server;

use bytes::Bytes;
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
