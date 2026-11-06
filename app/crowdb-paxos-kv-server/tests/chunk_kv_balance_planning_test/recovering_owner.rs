// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_protocol::{
    chunk_stream::{StreamProcessIdentity, StreamProcessLease, StreamPurpose},
    common::InstanceValue,
    key::{InstanceKey, StreamProcessLeaseKey},
};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn renewing_recovery_process_is_not_dead_when_discovery_is_stale() {
    let fixture = TestBalancePlanning::new().await;
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 1,
    }
    .to_path();
    let row = fixture.control.get(path.as_bytes()).await.unwrap();
    let mut instance: InstanceValue = serde_json::from_slice(row.value.as_deref().unwrap()).unwrap();
    instance.last_heartbeat_ms = 0;
    fixture
        .control
        .put_batch(vec![(
            Bytes::from(path),
            Bytes::from(serde_json::to_vec(&instance).unwrap()),
        )])
        .await
        .unwrap();
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
    let path = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id: 1,
    }
    .to_path();
    let mut lease = StreamProcessLease {
        identity: StreamProcessIdentity {
            purpose: StreamPurpose::Wal,
            instance_id: 1,
            process_epoch: 2,
            boot_nonce: [9; 16],
        },
        expires_at_ms: now + 60_000,
        prior_exclusion_deadline_ms: now - 1,
        grant_sequence: 0,
        grant_expires_at_ms: 0,
        revoked: false,
    };
    lease.validate().unwrap();
    for expiry in [now + 60_000, now - 1] {
        lease.expires_at_ms = expiry;
        fixture
            .control
            .put_batch(vec![(
                Bytes::from(path.clone()),
                Bytes::from(serde_json::to_vec(&lease).unwrap()),
            )])
            .await
            .unwrap();
        fixture.tick().await;
        let transfers = fixture
            .control
            .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
            .await
            .unwrap();
        if expiry > now {
            assert!(
                transfers.is_empty(),
                "a live recovery boot must retain its assigned WAL"
            );
        } else {
            assert_eq!(
                transfers.len(),
                1,
                "expired processes still require dead-owner failover"
            );
        }
    }
}
