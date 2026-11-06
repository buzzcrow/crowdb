// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/stream_lease.rs"]
mod fixture;

use crowdb_paxos_kv_client::{
    StreamProcessAuthority, StreamProcessLeaseClient, StreamProcessLeaseError, StreamProcessLeaseTiming,
};
use crowdb_protocol::{
    chunk_stream::StreamPurpose,
    key::{StreamProcessLeaseKey, TextKey},
};
use fixture::TestStreamLeaseKv;

#[tokio::test]
async fn a_fixed_boot_is_idempotent_and_replaced_processes_cannot_renew_or_reclaim() {
    let fixture = TestStreamLeaseKv::start().await;
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let first = client
        .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_000, 100)
        .await
        .unwrap();
    assert_eq!(
        client
            .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_010, 100)
            .await
            .unwrap(),
        first
    );
    let renewed = client.renew(first.identity, 1_050, 100).await.unwrap();
    let next = client
        .claim(StreamPurpose::Wal, 7, [2; 16], Some(first.identity), 1_060, 100)
        .await
        .unwrap();
    assert_eq!(next.identity.process_epoch, 2);
    assert_eq!(next.prior_exclusion_deadline_ms, renewed.expires_at_ms);
    assert!(matches!(
        client.renew(first.identity, 1_061, 100).await,
        Err(StreamProcessLeaseError::Replaced)
    ));
    assert!(matches!(
        client
            .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_061, 100)
            .await,
        Err(StreamProcessLeaseError::Replaced)
    ));
    assert_eq!(
        client
            .claim(StreamPurpose::Wal, 7, [2; 16], Some(first.identity), 1_061, 100)
            .await
            .unwrap(),
        next
    );
    assert!(
        client
            .renew(next.identity, 1_070, 100)
            .await
            .unwrap()
            .expires_at_ms
            > next.expires_at_ms
    );
}

#[tokio::test]
async fn simultaneous_claimants_cannot_steal_a_changed_predecessor() {
    let fixture = TestStreamLeaseKv::start().await;
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let (left, right) = tokio::join!(
        client.claim(StreamPurpose::Stream, 9, [1; 16], None, 1_000, 100),
        client.claim(StreamPurpose::Stream, 9, [2; 16], None, 1_000, 100),
    );
    let (winner, loser_nonce) = match (left, right) {
        (Ok(winner), Err(StreamProcessLeaseError::Replaced)) => (winner, [2; 16]),
        (Err(StreamProcessLeaseError::Replaced), Ok(winner)) => (winner, [1; 16]),
        outcome => panic!("two claims did not choose exactly one boot: {outcome:?}"),
    };
    assert!(matches!(
        client
            .claim(StreamPurpose::Stream, 9, loser_nonce, None, 1_010, 100)
            .await,
        Err(StreamProcessLeaseError::Replaced)
    ));
    assert_eq!(client.load(StreamPurpose::Stream, 9).await.unwrap(), Some(winner));
    assert!(client.load(StreamPurpose::Wal, 9).await.unwrap().is_none());
}

#[tokio::test]
async fn expiry_requires_a_new_boot_and_preserves_the_old_exclusion_boundary() {
    let fixture = TestStreamLeaseKv::start().await;
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let first = client
        .claim(StreamPurpose::Stream, 7, [1; 16], None, 1_000, 100)
        .await
        .unwrap();
    assert!(matches!(
        client.renew(first.identity, 1_100, 100).await,
        Err(StreamProcessLeaseError::Expired)
    ));
    assert!(matches!(
        client
            .claim(StreamPurpose::Stream, 7, [1; 16], None, 1_100, 100)
            .await,
        Err(StreamProcessLeaseError::Expired)
    ));
    let next = client
        .claim(
            StreamPurpose::Stream,
            7,
            [2; 16],
            Some(first.identity),
            1_100,
            100,
        )
        .await
        .unwrap();
    assert_eq!(next.prior_exclusion_deadline_ms, first.expires_at_ms);
}

#[tokio::test]
async fn replacement_retains_grant_fences_and_rejects_partial_records() {
    let fixture = TestStreamLeaseKv::start().await;
    let kv = fixture.kv.clone();
    let client = StreamProcessLeaseClient::new(kv.clone());
    let mut first = client
        .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_000, 100)
        .await
        .unwrap();
    first.grant_sequence = 9;
    first.grant_expires_at_ms = 2_000;
    let key = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id: 7,
    }
    .to_path();
    kv.put(0, 0, key.as_bytes(), &serde_json::to_vec(&first).unwrap(), None)
        .await
        .unwrap();
    let renewed = client.renew(first.identity, 1_020, 100).await.unwrap();
    assert_eq!(renewed.grant_sequence, first.grant_sequence);
    assert_eq!(renewed.grant_expires_at_ms, first.grant_expires_at_ms);
    let next = client
        .claim(StreamPurpose::Wal, 7, [2; 16], Some(first.identity), 1_050, 100)
        .await
        .unwrap();
    assert_eq!(next.prior_exclusion_deadline_ms, 2_000);
    assert_eq!(next.grant_sequence, 9);
    let mut partial = serde_json::to_value(&next).unwrap();
    partial
        .as_object_mut()
        .unwrap()
        .remove("prior_exclusion_deadline_ms");
    kv.put(0, 0, key.as_bytes(), &serde_json::to_vec(&partial).unwrap(), None)
        .await
        .unwrap();
    assert!(matches!(
        client.load(StreamPurpose::Wal, 7).await,
        Err(StreamProcessLeaseError::Invalid(_))
    ));
}

#[tokio::test]
async fn confirmed_group0_replacement_revokes_the_old_local_context() {
    let fixture = TestStreamLeaseKv::start().await;
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let first = client
        .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_000, 12_000)
        .await
        .unwrap();
    let now = std::time::Instant::now();
    let owner =
        StreamProcessAuthority::new(first.clone(), StreamProcessLeaseTiming::default(), 1_000, now).unwrap();
    let context = owner.capture(now).unwrap();
    let next = client
        .claim(
            StreamPurpose::Wal,
            7,
            [2; 16],
            Some(first.identity),
            1_100,
            12_000,
        )
        .await
        .unwrap();
    assert!(matches!(
        owner.observe(next, 1_100, std::time::Instant::now()),
        Err(StreamProcessLeaseError::Replaced)
    ));
    assert!(matches!(
        owner.validate_context(&context, std::time::Instant::now()),
        Err(StreamProcessLeaseError::Expired)
    ));
    assert!(owner.status(std::time::Instant::now()).fenced);
}
