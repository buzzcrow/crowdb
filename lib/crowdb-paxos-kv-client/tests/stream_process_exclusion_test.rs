// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/stream_lease.rs"]
mod fixture;

use crowdb_paxos_kv_client::{
    GetOutcome, ReadMode, StreamProcessAuthority, StreamProcessLeaseClient, StreamProcessLeaseError,
    StreamProcessLeaseTiming,
};
use crowdb_protocol::{
    chunk_stream::StreamPurpose,
    key::{StreamProcessLeaseKey, TextKey},
    timing::{MAX_WRITE_CLOCK_SKEW_MS, MAX_WRITE_REQUEST_AGE_MS},
};
use fixture::TestStreamLeaseKv;

#[tokio::test]
async fn confirmed_exclusion_fences_paused_cas_stale_renewal_and_same_boot_retry() {
    let fixture = TestStreamLeaseKv::start().await;
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let first = client
        .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_000, 100_000)
        .await
        .unwrap();
    let owner = StreamProcessAuthority::new(
        first.clone(),
        StreamProcessLeaseTiming::default(),
        1_000,
        std::time::Instant::now(),
    )
    .unwrap();
    let context = owner.capture(std::time::Instant::now()).unwrap();
    let key = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id: 7,
    }
    .to_path();
    let GetOutcome::Found { revision, .. } = fixture
        .kv
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("lease is absent");
    };
    let mut paused = first.clone();
    paused.expires_at_ms += 100_000;
    let safe = first.expires_at_ms + MAX_WRITE_CLOCK_SKEW_MS + MAX_WRITE_REQUEST_AGE_MS;
    assert_eq!(
        client.confirm_exclusion(first.identity, safe - 1).await.unwrap(),
        None
    );
    assert_eq!(
        client.confirm_exclusion(first.identity, safe).await.unwrap(),
        Some(safe)
    );
    assert!(fixture
        .kv
        .put_cas(
            0,
            0,
            key.as_bytes(),
            &serde_json::to_vec(&paused).unwrap(),
            revision
        )
        .await
        .is_err());
    assert!(matches!(
        client.renew(first.identity, 1_001, 100_000).await,
        Err(StreamProcessLeaseError::Expired)
    ));
    assert!(matches!(
        client
            .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_001, 100_000)
            .await,
        Err(StreamProcessLeaseError::Expired)
    ));
    let excluded = client.load(StreamPurpose::Wal, 7).await.unwrap().unwrap();
    assert!(excluded.revoked);
    assert!(owner.observe(excluded, 1_001, std::time::Instant::now()).is_err());
    assert!(owner
        .validate_context(&context, std::time::Instant::now())
        .is_err());
    assert_eq!(
        client.confirm_exclusion(first.identity, safe).await.unwrap(),
        Some(safe)
    );
    let next = client
        .claim(
            StreamPurpose::Wal,
            7,
            [2; 16],
            Some(first.identity),
            safe,
            100_000,
        )
        .await
        .unwrap();
    assert!(!next.revoked);
    assert_eq!(next.identity.process_epoch, 2);
    assert_eq!(next.prior_exclusion_deadline_ms, first.expires_at_ms);
    assert_eq!(
        client.confirm_exclusion(first.identity, safe).await.unwrap(),
        Some(safe)
    );
}

#[tokio::test]
async fn renewal_that_wins_authority_moves_the_exclusion_boundary() {
    let fixture = TestStreamLeaseKv::start().await;
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let first = client
        .claim(StreamPurpose::Stream, 8, [1; 16], None, 1_000, 1_000)
        .await
        .unwrap();
    let renewed = client.renew(first.identity, 1_001, 100_000).await.unwrap();
    let margin = MAX_WRITE_CLOCK_SKEW_MS + MAX_WRITE_REQUEST_AGE_MS;
    assert_eq!(
        client
            .confirm_exclusion(first.identity, first.expires_at_ms + margin)
            .await
            .unwrap(),
        None
    );
    assert!(
        !client
            .load(StreamPurpose::Stream, 8)
            .await
            .unwrap()
            .unwrap()
            .revoked
    );
    assert_eq!(
        client
            .confirm_exclusion(first.identity, renewed.expires_at_ms + margin)
            .await
            .unwrap(),
        Some(renewed.expires_at_ms + margin)
    );
}
