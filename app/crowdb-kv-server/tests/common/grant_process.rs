// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_kv_server::background::domain_monitor::ChunkKvRangeMonitorDriver;
use crowdb_protocol::{
    chunk_stream::{StreamProcessLease, StreamPurpose},
    key::StreamProcessLeaseKey,
};

async fn replace_json<T: serde::Serialize>(control: &Group0ControlPlane, path: String, value: &T) {
    let revision = control.get(path.as_bytes()).await.unwrap().revision;
    control
        .compare_and_put(
            Bytes::from(path),
            Bytes::from(serde_json::to_vec(value).unwrap()),
            revision,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn grant_and_process_expiry_fence_publish_together_and_reject_stale_observations() {
    let (control, _) = assert_ready_transfer_publication().await;
    let path = ServingGrantKey { instance_id: 2 }.to_path();
    let grant: ServingGrant = get_json(&control, &path).await;
    let lease_path = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id: 2,
    }
    .to_path();
    let mut lease: StreamProcessLease = get_json(&control, &lease_path).await;
    assert_eq!(lease.grant_sequence, grant.lease_sequence);
    assert!(lease.grant_expires_at_ms >= grant.expires_at_ms);
    assert_eq!(
        control.get(path.as_bytes()).await.unwrap().revision,
        control.get(lease_path.as_bytes()).await.unwrap().revision
    );
    lease.identity.process_epoch += 1;
    lease.identity.boot_nonce = [2; 16];
    lease.prior_exclusion_deadline_ms = lease.expires_at_ms;
    replace_json(&control, lease_path.clone(), &lease).await;
    let mut policy = descriptor();
    policy.domain = "chunk-kv".into();
    policy.service_registry_name = "chunk-kv".into();
    policy.failure_policy = DomainFailurePolicy::OperatorOnly;
    ChunkKvRangeMonitorDriver::new()
        .tick(&control, &policy)
        .await
        .unwrap();
    let unchanged: ServingGrant = get_json(&control, &path).await;
    assert_eq!(
        unchanged, grant,
        "old observation cannot grant the replacement boot"
    );
    let instance_path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 2,
    }
    .to_path();
    let mut instance: InstanceValue = get_json(&control, &instance_path).await;
    instance
        .extra
        .as_mut()
        .unwrap()
        .chunk_kv
        .as_mut()
        .unwrap()
        .process_epoch = lease.identity.process_epoch;
    replace_json(&control, instance_path, &instance).await;
    ChunkKvRangeMonitorDriver::new()
        .tick(&control, &policy)
        .await
        .unwrap();
    let current: ServingGrant = get_json(&control, &path).await;
    assert_eq!(current.process_epoch, 2);
    assert!(current.lease_sequence > grant.lease_sequence);
    let accepted: StreamProcessLease = get_json(&control, &lease_path).await;
    assert_eq!(accepted.grant_sequence, current.lease_sequence);
    assert!(accepted.grant_expires_at_ms >= current.expires_at_ms);
}
