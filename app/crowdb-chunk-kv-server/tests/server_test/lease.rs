// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use serving_fixture::TestServingLease;

#[tokio::test]
async fn graceful_drain_waits_for_wal_io_and_publishes_its_complete_prefix() {
    let fixture = TestServingLease::start().await;
    fixture.store.pause_writes();
    let request = PointRequest {
        routing: routing(1),
        operation: PointOperation::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        },
    };
    let pending = fixture.service.handle_point(request, 1_000, 50_000);
    tokio::pin!(pending);
    fixture.wait_for_write(pending.as_mut()).await;
    let draining = fixture.service.drain_writers();
    tokio::pin!(draining);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), draining.as_mut())
            .await
            .is_err()
    );
    fixture.store.resume_writes();
    let (_, drained) = tokio::join!(pending, draining);
    drained.unwrap();
    assert_eq!(
        fixture.partition.lifecycle(),
        PartitionLifecycle::TransferQuiesced
    );
    let name = fixture.partition.snapshot().stream_name;
    let published = fixture.store.load_tail_for_tests(name).await.unwrap().unwrap();
    assert_eq!(
        published.record().logical_end,
        Some(fixture.partition.snapshot().journal_durable_offset)
    );
    assert!(published.record().logical_end.unwrap() > 0);
    assert_eq!(
        fixture
            .store
            .record_store(7)
            .unwrap()
            .candidates(name)
            .await
            .unwrap()
            .len(),
        0
    );
    fixture.service.drain_writers().await.unwrap();
}

#[tokio::test]
async fn process_revocation_during_wal_io_prevents_acknowledgment() {
    let fixture = TestServingLease::start().await;
    fixture.store.pause_writes();
    let request = PointRequest {
        routing: routing(1),
        operation: PointOperation::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        },
    };
    let pending = fixture.service.handle_point(request.clone(), 1_000, 50_000);
    tokio::pin!(pending);
    fixture.wait_for_write(pending.as_mut()).await;
    fixture.service.authority().process().fence();
    fixture.store.resume_writes();
    let response = pending.await;
    assert!(
        matches!(response.result, Err(ref error) if error.code == ChunkKvRpcErrorCode::LeaseExpired),
        "{response:?}"
    );
    assert!(response.journal_position.is_none());
    assert!(fixture
        .partition
        .get(EPOCH, b"key", None)
        .await
        .unwrap()
        .is_some());
    let retry = fixture.service.handle_point(request, 1_100, 50_100).await;
    assert!(
        matches!(retry.result, Err(ref error) if error.code == ChunkKvRpcErrorCode::LeaseExpired),
        "{retry:?}"
    );
}

#[tokio::test]
async fn revoked_inflight_wal_write_cannot_acknowledge_under_a_replacement_grant() {
    let fixture = TestServingLease::start().await;
    fixture.store.pause_writes();
    let request = PointRequest {
        routing: routing(1),
        operation: PointOperation::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        },
    };
    let pending = fixture.service.handle_point(request.clone(), 1_000, 50_000);
    tokio::pin!(pending);
    fixture.wait_for_write(pending.as_mut()).await;
    fixture.replace_authority();
    fixture.store.resume_writes();
    let response = pending.await;
    assert!(
        matches!(response.result, Err(ref error) if error.code == ChunkKvRpcErrorCode::LeaseExpired),
        "{response:?}"
    );
    assert!(response.journal_position.is_none());
    assert!(fixture
        .partition
        .get(EPOCH, b"key", None)
        .await
        .unwrap()
        .is_some());
    let retry = fixture.service.handle_point(request, 1_100, 50_100).await;
    assert!(retry.result.is_ok(), "{retry:?}");
    assert!(retry.journal_position.is_some());
}

#[tokio::test]
async fn revoked_batch_does_not_admit_remaining_members_under_a_new_grant() {
    let fixture = TestServingLease::start().await;
    fixture.store.pause_writes();
    let request = TestServingLease::batch_request();
    let pending = fixture
        .service
        .handle_batch_mutation(request.clone(), 1_000, 50_000);
    tokio::pin!(pending);
    fixture.wait_for_write(pending.as_mut()).await;
    fixture.replace_authority();
    fixture.store.resume_writes();
    let response = pending.await;
    assert!(
        response.result.as_ref().unwrap().iter().all(|item| {
            matches!(&item.result, Err(error) if error.code == ChunkKvRpcErrorCode::LeaseExpired)
                && item.journal_position.is_none()
        }),
        "{response:?}"
    );
    assert!(fixture
        .partition
        .get(EPOCH, &[b'k', 1], None)
        .await
        .unwrap()
        .is_some());
    assert!(fixture
        .partition
        .get(EPOCH, &[b'k', 2], None)
        .await
        .unwrap()
        .is_none());
    let retried = fixture
        .service
        .handle_batch_mutation(request, 1_100, 50_100)
        .await;
    assert!(retried
        .result
        .unwrap()
        .iter()
        .all(|item| item.result.is_ok() && item.journal_position.is_some()));
}

#[tokio::test]
async fn continuous_renewal_preserves_a_batch_waiting_on_its_first_wal_write() {
    let fixture = TestServingLease::start().await;
    fixture.store.pause_writes();
    let request = TestServingLease::batch_request();
    let pending = fixture.service.handle_batch_mutation(request, 1_000, 50_000);
    tokio::pin!(pending);
    fixture.wait_for_write(pending.as_mut()).await;
    fixture.renew_authority();
    fixture.store.resume_writes();
    let response = pending.await;
    assert!(
        response
            .result
            .as_ref()
            .unwrap()
            .iter()
            .all(|item| { item.result.is_ok() && item.journal_position.is_some() }),
        "{response:?}"
    );
    for sequence in 1..=2 {
        assert!(fixture
            .partition
            .get(EPOCH, &[b'k', sequence], None)
            .await
            .unwrap()
            .is_some());
    }
}
