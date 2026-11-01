// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/node_health.rs"]
mod fixture;

use crowdb_monitor::MonitorPhase;
use fixture::TestHealth;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn slow_service_authority_does_not_freeze_current_local_health() {
    let mut fixture = TestHealth::new().await;
    tokio::time::timeout(Duration::from_secs(1), fixture.worker.tick_for_tests())
        .await
        .expect("service authority must not block local health publication")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), fixture.entered.notified())
        .await
        .expect("the production authority read was actually started");
    let healthy = fixture.snapshot();
    assert_eq!(healthy.phase, MonitorPhase::Ready);
    assert_eq!(healthy.services.len(), 2);
    assert!(healthy.services.values().all(|service| service.healthy));

    fixture.ready.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(1), fixture.worker.tick_for_tests())
        .await
        .expect("an in-flight authority read must not hide changed local health")
        .unwrap();
    let unavailable = fixture.snapshot();
    assert_eq!(unavailable.phase, MonitorPhase::Restarting);
    assert!(unavailable.services.values().all(|service| !service.healthy));
    assert!(unavailable.revision > healthy.revision);
    for (id, service) in &unavailable.services {
        assert_eq!(service.pid, healthy.services[id].pid);
        assert_eq!(service.restart_attempts, 0);
    }
    fixture.worker.stop_kv_for_tests().await.unwrap();
}
