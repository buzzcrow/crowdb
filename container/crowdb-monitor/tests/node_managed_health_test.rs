// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/managed_health.rs"]
mod fixture;
#[path = "common/node_setup.rs"]
mod node_setup;

use fixture::TestManagedHealth;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

#[tokio::test]
async fn background_recovery_yields_while_a_new_managed_process_is_not_ready() {
    let mut test = TestManagedHealth::new().await;
    test.worker.stop_child_for_tests("chunkdb-2").await.unwrap();
    test.ready.store(false, Ordering::Release);
    let started = Instant::now();
    let result = test.worker.reconcile_for_tests().await;
    let elapsed = started.elapsed();
    let pending = test.execution();
    test.worker.tick_for_tests().await.unwrap();
    let unavailable = crowdb_monitor::StatusStore::open(&test.profile.paths.run_root)
        .unwrap()
        .read(Duration::from_secs(3))
        .unwrap();
    test.ready.store(true, Ordering::Release);
    test.worker.reconcile_for_tests().await.unwrap();
    let recovered = test.execution();
    test.stop().await;
    result.unwrap();
    assert!(
        elapsed < Duration::from_secs(3),
        "background recovery held health publication for {elapsed:?}"
    );
    assert!(!pending["complete"].as_bool().unwrap());
    assert_eq!(pending["attempts"], 1);
    assert!(!unavailable.services["chunkdb-2"].healthy);
    assert_eq!(
        unavailable.services["chunkdb-2"].pid,
        pending["pid"].as_u64().map(|pid| u32::try_from(pid).unwrap())
    );
    assert!(recovered["complete"].as_bool().unwrap());
    assert_eq!(recovered["pid"], pending["pid"]);
    assert_eq!(recovered["intent"]["operation_id"], test.intent.operation_id);
}

#[tokio::test]
async fn slow_probes_share_one_health_pass_and_do_not_hide_real_failures() {
    let mut test = TestManagedHealth::new().await;
    test.slow.store(true, Ordering::Release);
    let started = Instant::now();
    test.worker.tick_for_tests().await.unwrap();
    let elapsed = started.elapsed();
    let store = crowdb_monitor::StatusStore::open(&test.profile.paths.run_root).unwrap();
    let unavailable = store.read(Duration::from_secs(3)).unwrap();
    test.slow.store(false, Ordering::Release);
    test.worker.tick_for_tests().await.unwrap();
    let recovered = store.read(Duration::from_secs(3)).unwrap();
    test.stop().await;
    assert!(
        elapsed < Duration::from_secs(2),
        "probe deadlines accumulated: {elapsed:?}"
    );
    assert_eq!(unavailable.services.len(), 3);
    assert!(unavailable.services.values().all(|service| !service.healthy));
    assert!(recovered.services.values().all(|service| service.healthy));
    for (id, service) in &unavailable.services {
        assert_eq!(service.pid, recovered.services[id].pid);
    }
}
