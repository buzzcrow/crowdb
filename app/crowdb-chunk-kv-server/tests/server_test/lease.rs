// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use serving_fixture::TestServingLease;

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
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            () = fixture.store.wait_for_write() => {}
            response = &mut pending => panic!("write completed before its gate: {response:?}"),
        }
    })
    .await
    .unwrap();
    fixture.service.authority().clear();
    let mut grant = ServingGrant {
        instance_id: INSTANCE_ID,
        lease_sequence: 2,
        catalog_generation: 1,
        issued_at_ms: 1_100,
        expires_at_ms: 13_100,
        assignments: vec![ServingAssignment {
            partition_id: Id128 { high: 1, low: 2 },
            owner_epoch: EPOCH,
        }],
        assignment_digest: [0; 32],
    };
    grant.seal();
    fixture
        .service
        .authority()
        .install(grant, &policy(), 1_100, 50_100)
        .unwrap();
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
