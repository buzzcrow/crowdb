// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv_server::{
    DomainMonitorDriver, DomainMonitorRegistry, MonitorDescriptorStore, MonitorError, MonitorTick,
    PreparedMonitor,
};
use crowdb_protocol::chunk_kv::{
    DomainFailurePolicy, DomainMonitorDescriptor, EnsureDomainMonitorOutcome, EnsureDomainMonitorRequest,
};
use tokio::sync::Mutex;

fn descriptor(driver_version: u32) -> DomainMonitorDescriptor {
    DomainMonitorDescriptor {
        domain: "chunk-kv".into(),
        service_registry_name: "chunk-kv".into(),
        driver_version,
        capability_version: 1,
        heartbeat_interval_ms: 2_000,
        suspect_after_ms: 6_000,
        dead_after_ms: 10_000,
        lease_duration_ms: 12_000,
        max_clock_skew_ms: 1_000,
        self_fence_margin_ms: 1_000,
        failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
        balance_policy: "count-first-v1".into(),
    }
}

#[derive(Default)]
struct TestDescriptorStore {
    descriptor: Mutex<Option<DomainMonitorDescriptor>>,
}

#[async_trait]
impl MonitorDescriptorStore for TestDescriptorStore {
    async fn ensure(
        &self,
        descriptor: &DomainMonitorDescriptor,
    ) -> Result<EnsureDomainMonitorOutcome, MonitorError> {
        let mut current = self.descriptor.lock().await;
        match current.as_ref() {
            None => {
                *current = Some(descriptor.clone());
                Ok(EnsureDomainMonitorOutcome::Created)
            }
            Some(existing) if existing == descriptor => Ok(EnsureDomainMonitorOutcome::AlreadyExists),
            Some(_) => Ok(EnsureDomainMonitorOutcome::DescriptorConflict),
        }
    }
}

#[tokio::test]
async fn ensure_is_idempotent_and_fails_closed_on_driver_conflict() {
    let store = Arc::new(TestDescriptorStore::default());
    let registry = DomainMonitorRegistry::new(
        store,
        vec![crowdb_chunk_kv_server::monitor::SupportedMonitor {
            domain: "chunk-kv".into(),
            driver_version: 1,
            max_capability_version: 2,
        }],
    );
    let request = EnsureDomainMonitorRequest {
        descriptor: descriptor(1),
    };
    assert_eq!(
        registry.ensure(&request).await.unwrap(),
        EnsureDomainMonitorOutcome::Created
    );
    assert_eq!(
        registry.ensure(&request).await.unwrap(),
        EnsureDomainMonitorOutcome::AlreadyExists
    );
    let mut conflicting = request;
    conflicting.descriptor.balance_policy = "another-policy".into();
    assert_eq!(
        registry.ensure(&conflicting).await.unwrap(),
        EnsureDomainMonitorOutcome::DescriptorConflict
    );
    assert_eq!(
        registry
            .ensure(&EnsureDomainMonitorRequest {
                descriptor: descriptor(2),
            })
            .await
            .unwrap(),
        EnsureDomainMonitorOutcome::UnsupportedMonitorDomain
    );
}

struct TestDriver {
    fail_read: bool,
    plans: Arc<AtomicU64>,
    publishes: Arc<AtomicU64>,
}

#[async_trait]
impl DomainMonitorDriver for TestDriver {
    type Snapshot = u64;
    type Plan = u64;

    async fn observe(&self) -> Result<Self::Snapshot, MonitorError> {
        if self.fail_read {
            Err(MonitorError::ReadFailed("catalog unavailable".into()))
        } else {
            Ok(4)
        }
    }

    fn plan(&self, snapshot: Self::Snapshot) -> Result<Self::Plan, MonitorError> {
        self.plans.fetch_add(1, Ordering::Relaxed);
        Ok(snapshot + 1)
    }

    async fn publish(&self, _plan: Self::Plan) -> Result<(), MonitorError> {
        self.publishes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn standby_and_read_failure_never_publish() {
    let plans = Arc::new(AtomicU64::new(0));
    let publishes = Arc::new(AtomicU64::new(0));
    let standby = PreparedMonitor::new(TestDriver {
        fail_read: false,
        plans: Arc::clone(&plans),
        publishes: Arc::clone(&publishes),
    });
    assert_eq!(standby.tick(false).await.unwrap(), MonitorTick::Standby);
    let failing = PreparedMonitor::new(TestDriver {
        fail_read: true,
        plans: Arc::clone(&plans),
        publishes: Arc::clone(&publishes),
    });
    assert!(matches!(
        failing.tick(true).await,
        Err(MonitorError::ReadFailed(_))
    ));
    assert_eq!(plans.load(Ordering::Relaxed), 0);
    assert_eq!(publishes.load(Ordering::Relaxed), 0);
}
