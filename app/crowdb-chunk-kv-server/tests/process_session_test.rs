// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/process_kv.rs"]
mod fixture;

use crowdb_chunk_kv_server::serving::process::ProcessLeaseSession;
use crowdb_kv_client::StreamProcessLeaseClient;
use crowdb_protocol::chunk_stream::StreamPurpose;
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
