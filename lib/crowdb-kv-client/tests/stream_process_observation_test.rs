// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/stream_lease.rs"]
mod fixture;

use crowdb_kv_client::{GetOutcome, ReadMode, StreamProcessLeaseClient, StreamProcessLeaseError};
use crowdb_protocol::{chunk_stream::StreamPurpose, common::ChunkKvExtra, key::InstanceKey};
use fixture::TestStreamLeaseKv;

#[tokio::test]
async fn replaced_boot_cannot_overwrite_or_withdraw_its_successor_observation() {
    let fixture = TestStreamLeaseKv::start().await;
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let first = client
        .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_000, 100)
        .await
        .unwrap();
    let mut observation = ChunkKvExtra {
        process_epoch: 1,
        ..ChunkKvExtra::default()
    };
    client
        .publish_chunk_kv_observation(first.identity, "first", &observation, 1_001)
        .await
        .unwrap();
    let second = client
        .claim(StreamPurpose::Wal, 7, [2; 16], Some(first.identity), 1_002, 100)
        .await
        .unwrap();
    observation.process_epoch = second.identity.process_epoch;
    client
        .publish_chunk_kv_observation(second.identity, "second", &observation, 1_003)
        .await
        .unwrap();
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 7,
    }
    .to_path();
    let before = fixture
        .kv
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap();
    observation.process_epoch = 1;
    assert!(matches!(
        client
            .publish_chunk_kv_observation(first.identity, "stale", &observation, 1_004)
            .await,
        Err(StreamProcessLeaseError::Replaced)
    ));
    assert!(matches!(
        client.withdraw_chunk_kv_observation(first.identity, 1_004).await,
        Err(StreamProcessLeaseError::Replaced)
    ));
    let after = fixture
        .kv
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap();
    match (before, after) {
        (
            GetOutcome::Found {
                value: before,
                revision: before_revision,
            },
            GetOutcome::Found {
                value: after,
                revision: after_revision,
            },
        ) => {
            assert_eq!(before, after);
            assert_eq!(before_revision, after_revision);
        }
        other => panic!("successor observation changed: {other:?}"),
    }
    client
        .withdraw_chunk_kv_observation(second.identity, 1_005)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .kv
            .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
    assert_eq!(client.load(StreamPurpose::Wal, 7).await.unwrap().unwrap(), second);
}

#[tokio::test]
async fn expiry_and_wrong_observation_epoch_cannot_publish_discovery() {
    let fixture = TestStreamLeaseKv::start().await;
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let lease = client
        .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_000, 100)
        .await
        .unwrap();
    let mut observation = ChunkKvExtra {
        process_epoch: 2,
        ..ChunkKvExtra::default()
    };
    assert!(matches!(
        client
            .publish_chunk_kv_observation(lease.identity, "wrong", &observation, 1_001)
            .await,
        Err(StreamProcessLeaseError::Invalid(_))
    ));
    observation.process_epoch = 1;
    assert!(matches!(
        client
            .publish_chunk_kv_observation(lease.identity, "expired", &observation, 1_100)
            .await,
        Err(StreamProcessLeaseError::Expired)
    ));
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 7,
    }
    .to_path();
    assert!(matches!(
        fixture
            .kv
            .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
}
