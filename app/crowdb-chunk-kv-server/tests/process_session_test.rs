// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/process_kv.rs"]
mod fixture;

use crowdb_chunk_kv_server::serving::process::ProcessLeaseSession;
use crowdb_paxos_kv_client::{GetOutcome, ReadMode, StreamProcessLeaseClient, StreamProcessLeaseError};
use crowdb_protocol::{
    chunk_stream::StreamPurpose,
    common::{ChunkKvExtra, InstanceValue},
    key::{InstanceKey, StreamProcessLeaseKey, TextKey},
};
use fixture::TestProcessKv;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn process_renewal_runs_without_data_requests_and_drop_permanently_fences_it() {
    let fixture = TestProcessKv::start().await;
    let session = ProcessLeaseSession::start(fixture.kv.clone(), 7, &TestProcessKv::policy())
        .await
        .unwrap();
    let authority = session.authority();
    let admitted = authority.capture(Instant::now()).unwrap();
    let expiry = authority.status(Instant::now()).lease.expires_at_ms;
    tokio::time::timeout(Duration::from_secs(2), async {
        while authority.status(Instant::now()).lease.expires_at_ms <= expiry {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(authority.capture(Instant::now()).unwrap().deadline() > admitted.deadline());
    assert!(authority.validate_context(&admitted, Instant::now()).is_ok());
    drop(session);
    assert!(authority.capture(Instant::now()).is_err());
    assert!(authority.status(Instant::now()).fenced);
}

#[tokio::test]
async fn background_renewal_detects_a_replacement_and_cannot_reclaim_it() {
    let fixture = TestProcessKv::start().await;
    let session = ProcessLeaseSession::start(fixture.kv.clone(), 7, &TestProcessKv::policy())
        .await
        .unwrap();
    let authority = session.authority();
    let old = authority.capture(Instant::now()).unwrap();
    let client = StreamProcessLeaseClient::new(fixture.kv.clone());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    let replacement = client
        .claim(StreamPurpose::Wal, 7, [2; 16], Some(old.identity()), now, 1_000)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !authority.status(Instant::now()).fenced {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(authority.validate_context(&old, Instant::now()).is_err());
    tokio::time::timeout(Duration::from_secs(1), session.wait_for_fence())
        .await
        .unwrap();
    assert_eq!(
        client.load(StreamPurpose::Wal, 7).await.unwrap().unwrap(),
        replacement
    );
}

#[tokio::test]
async fn heartbeat_does_not_spend_its_cas_budget_on_its_own_pending_renewal() {
    let fixture = TestProcessKv::start().await;
    let session = ProcessLeaseSession::start(fixture.kv.clone(), 7, &TestProcessKv::policy())
        .await
        .unwrap();
    let group = fixture.store.get_group(0).unwrap();
    let permits = group.try_acquire_all_inflight_permits();
    wait_for_pending_renewal(&group).await;
    let observation = ChunkKvExtra {
        process_epoch: 1,
        ..ChunkKvExtra::default()
    };
    let release = async {
        // Exceed the existing bounded CAS backoff while staying inside the
        // original process lease. This is one slow commit, not a lost boot.
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(permits);
    };
    let (published, ()) = tokio::join!(session.publish_observation("live", &observation), release);
    assert!(
        published.is_ok(),
        "own pending renewal must not exhaust the heartbeat CAS budget: {published:?}"
    );
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 7,
    }
    .to_path();
    let GetOutcome::Found { value, .. } = fixture
        .kv
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("heartbeat was not durably published");
    };
    let value: InstanceValue = serde_json::from_slice(&value).unwrap();
    assert_eq!(value.rpc_endpoint, "live");
    assert!(session.authority().capture(Instant::now()).is_ok());
}

#[tokio::test]
async fn continuous_observations_do_not_starve_background_renewal() {
    let fixture = TestProcessKv::start().await;
    let session = ProcessLeaseSession::start(fixture.kv.clone(), 7, &TestProcessKv::policy())
        .await
        .unwrap();
    let authority = session.authority();
    let original = authority.capture(Instant::now()).unwrap();
    let expiry = authority.status(Instant::now()).lease.expires_at_ms;
    let observation = ChunkKvExtra {
        process_epoch: 1,
        ..ChunkKvExtra::default()
    };
    while Instant::now() <= original.deadline() + Duration::from_millis(20) {
        session.publish_observation("live", &observation).await.unwrap();
    }
    assert!(authority.capture(Instant::now()).is_ok());
    assert!(authority.status(Instant::now()).lease.expires_at_ms > expiry);
}

#[tokio::test]
async fn repeated_slow_renewals_leave_a_turn_for_queued_heartbeats() {
    let fixture = TestProcessKv::start().await;
    let policy = TestProcessKv::policy();
    let session = ProcessLeaseSession::start(fixture.kv.clone(), 7, &policy)
        .await
        .unwrap();
    let authority = session.authority();
    let group = fixture.store.get_group(0).unwrap();
    let semaphore = group.inflight_semaphore();
    let count = u32::try_from(group.inflight_window_size()).unwrap();
    let mut permits = semaphore.acquire_many(count).await.unwrap();
    wait_for_pending_renewal(&group).await;
    let observation = ChunkKvExtra {
        process_epoch: 1,
        ..ChunkKvExtra::default()
    };
    let (finished, completion) = tokio::sync::oneshot::channel();
    let publish = async {
        let result = async {
            let mut previous_expiry = authority.status(Instant::now()).lease.expires_at_ms;
            for endpoint in ["slow-1", "slow-2", "slow-3"] {
                session.publish_observation(endpoint, &observation).await?;
                let expiry = authority.status(Instant::now()).lease.expires_at_ms;
                assert!(
                    expiry > previous_expiry,
                    "renewal did not advance between observations"
                );
                previous_expiry = expiry;
            }
            Ok::<_, StreamProcessLeaseError>(())
        }
        .await;
        let _ = finished.send(());
        result
    };
    let slow_metadata = async {
        // Each conditional write takes four timer periods. Reacquiring the
        // whole window queues behind the admitted write, then holds the next
        // one. Six writes allow three renewal/observation pairs; a timer-only
        // loop would spend all six on renewals and leave publication to expire.
        for _ in 0..6 {
            wait_for_pending_renewal(&group).await;
            tokio::time::sleep(Duration::from_millis(policy.heartbeat_interval_ms * 4)).await;
            drop(permits);
            permits = semaphore.acquire_many(count).await.unwrap();
        }
        let _ = completion.await;
        drop(permits);
    };
    let (published, ()) = tokio::join!(publish, slow_metadata);
    assert!(
        published.is_ok(),
        "slow renewals starved discovery: {published:?}"
    );
    assert!(authority.capture(Instant::now()).is_ok());
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 7,
    }
    .to_path();
    let GetOutcome::Found { value, .. } = fixture
        .kv
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("slow heartbeat was not durably published");
    };
    let value: InstanceValue = serde_json::from_slice(&value).unwrap();
    assert_eq!(value.rpc_endpoint, "slow-3");
}

#[tokio::test]
async fn cancelled_queued_heartbeat_is_not_published_after_renewal() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };

    let fixture = TestProcessKv::start().await;
    let session = ProcessLeaseSession::start(fixture.kv.clone(), 7, &TestProcessKv::policy())
        .await
        .unwrap();
    let group = fixture.store.get_group(0).unwrap();
    let permits = group.try_acquire_all_inflight_permits();
    wait_for_pending_renewal(&group).await;
    let observation = ChunkKvExtra {
        process_epoch: 1,
        ..ChunkKvExtra::default()
    };
    let mut publication = Box::pin(session.publish_observation("cancelled", &observation));
    poll_fn(|context| {
        assert!(publication.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(publication);
    drop(permits);
    session.flush_observations_for_tests().await.unwrap();
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
async fn queued_heartbeat_cannot_extend_the_original_fence_deadline() {
    let fixture = TestProcessKv::start().await;
    let mut policy = TestProcessKv::policy();
    policy.lease_duration_ms = 200;
    let session = ProcessLeaseSession::start(fixture.kv.clone(), 7, &policy)
        .await
        .unwrap();
    let authority = session.authority();
    let deadline = authority.capture(Instant::now()).unwrap().deadline();
    let group = fixture.store.get_group(0).unwrap();
    let permits = group.try_acquire_all_inflight_permits();
    wait_for_pending_renewal(&group).await;
    let observation = ChunkKvExtra {
        process_epoch: 1,
        ..ChunkKvExtra::default()
    };
    let result = tokio::time::timeout_at(
        (deadline + Duration::from_millis(100)).into(),
        session.publish_observation("unavailable", &observation),
    )
    .await
    .expect("queued heartbeat exceeded the original process deadline");
    assert!(matches!(result, Err(StreamProcessLeaseError::Expired)));
    assert!(authority.capture(Instant::now()).is_err());
    drop(permits);
    session.wait_for_fence().await;
    assert!(authority.status(Instant::now()).fenced);
}

async fn wait_for_pending_renewal(group: &crowdb_paxos_kv::cluster::group::PxGroup) {
    let lease_key = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id: 7,
    }
    .to_path();
    tokio::time::timeout(Duration::from_millis(500), async {
        while !group.conditional_write_pending_for_tests(lease_key.as_bytes()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background renewal did not reach conditional admission");
}
