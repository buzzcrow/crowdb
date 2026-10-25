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

#[tokio::test]
async fn observation_waits_for_an_inflight_renewal_instead_of_exhausting_busy_retries() {
    exercise_inflight_lease_update(true).await;
}

#[tokio::test]
async fn renewal_waits_for_an_inflight_observation_instead_of_exhausting_busy_retries() {
    exercise_inflight_lease_update(false).await;
}

async fn exercise_inflight_lease_update(renewal_first: bool) {
    use crowdb_protocol::key::{StreamProcessLeaseKey, TextKey};
    use std::{sync::Arc, time::Duration};

    let fixture = TestStreamLeaseKv::start().await;
    let client = Arc::new(StreamProcessLeaseClient::new(fixture.kv.clone()));
    let lease = client
        .claim(StreamPurpose::Wal, 7, [1; 16], None, 1_000, 10_000)
        .await
        .unwrap();
    let group = fixture.store.get_group(0).unwrap();
    let permits = group.try_acquire_all_inflight_permits();
    let observation = ChunkKvExtra {
        process_epoch: 1,
        ..ChunkKvExtra::default()
    };
    let updating = Arc::clone(&client);
    let first_observation = observation.clone();
    let first = tokio::spawn(async move {
        if renewal_first {
            updating.renew(lease.identity, 1_001, 10_000).await.map(|_| ())
        } else {
            updating
                .publish_chunk_kv_observation(lease.identity, "live", &first_observation, 1_001)
                .await
        }
    });
    let key = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id: 7,
    }
    .to_path();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !group.conditional_write_pending_for_tests(key.as_bytes()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let second = async {
        if renewal_first {
            client
                .publish_chunk_kv_observation(lease.identity, "live", &observation, 1_002)
                .await
        } else {
            client.renew(lease.identity, 1_002, 10_000).await.map(|_| ())
        }
    };
    let release = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(permits);
    };
    let (result, ()) = tokio::join!(second, release);
    first.await.unwrap().unwrap();
    assert!(
        result.is_ok(),
        "short lease-key contention must not starve either update: {result:?}"
    );
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 7,
    }
    .to_path();
    let record = fixture
        .kv
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap();
    let GetOutcome::Found { value, .. } = record else {
        panic!("published observation missing");
    };
    let value: crowdb_protocol::common::InstanceValue = serde_json::from_slice(&value).unwrap();
    assert_eq!(value.rpc_endpoint, "live");
    assert_eq!(value.last_heartbeat_ms, if renewal_first { 1_002 } else { 1_001 });
    assert_eq!(
        client
            .load(StreamPurpose::Wal, 7)
            .await
            .unwrap()
            .unwrap()
            .expires_at_ms,
        if renewal_first { 11_001 } else { 11_002 }
    );
}
