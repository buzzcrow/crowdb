// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::client_data::fixture_client;
use crowdb_kv_client::DomainMonitorClient;
use crowdb_protocol::chunk_kv::{
    DomainFailurePolicy, DomainMonitorDescriptor, EnsureDomainMonitorOutcome, EnsureDomainMonitorRequest,
};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires the owned release-container KV client profile"]
async fn concurrent_domain_monitor_ensure_reconciles_cas_busy() {
    let client = Arc::new(fixture_client().await);
    let request = Arc::new(EnsureDomainMonitorRequest {
        descriptor: DomainMonitorDescriptor {
            domain: "chunkdb".into(),
            service_registry_name: "chunkdb".into(),
            driver_version: 1,
            capability_version: 1,
            heartbeat_interval_ms: 5_000,
            suspect_after_ms: 10_000,
            dead_after_ms: 15_000,
            lease_duration_ms: 20_000,
            max_clock_skew_ms: 1_000,
            self_fence_margin_ms: 1_000,
            failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
            balance_policy: "uniform-12-v1".into(),
            chunk_kv_range_balance: None,
        },
    });

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let monitor = DomainMonitorClient::from_shared(Arc::clone(&client));
        let request = Arc::clone(&request);
        tasks.spawn(async move { monitor.ensure(&request).await });
    }
    let mut created = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap().unwrap() {
            EnsureDomainMonitorOutcome::Created => created += 1,
            EnsureDomainMonitorOutcome::AlreadyExists => {}
            outcome => panic!("unexpected ensure outcome: {outcome:?}"),
        }
    }
    assert_eq!(created, 1);
}
