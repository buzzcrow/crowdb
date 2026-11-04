// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/service_inventory.rs"]
#[allow(dead_code)] // Shared fixture also supplies entry points used by the inventory suite.
mod fixture;
#[path = "common/process_transport.rs"]
mod transport;

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use transport::{TestCounts, TestTransportSource};

async fn until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("process transport ownership did not converge");
}

#[tokio::test]
async fn process_transport_belongs_to_one_node_configuration_and_bootstrap() {
    let mut fixture = fixture::TestServiceInventory::new(false);
    let counts = Arc::new(TestCounts::default());
    let source = Arc::new(TestTransportSource {
        source: fixture.source.clone(),
        counts: counts.clone(),
    });
    fixture.observer = Some(crowdb_web::start_service_observer_for_tests(&fixture.state, source).unwrap());
    fixture.wait_pid(1, Some(101)).await;
    fixture.wait_pid(2, Some(102)).await;
    until(|| counts.reads[2].load(Ordering::Relaxed) >= 2).await;
    assert_eq!(counts.created[1].load(Ordering::Relaxed), 1);
    assert_eq!(counts.created[2].load(Ordering::Relaxed), 1);
    fixture.state.config.write().unwrap().nodes[1].host = "127.0.0.99".into();
    until(|| {
        counts.created[2].load(Ordering::Relaxed) == 2 && counts.dropped[2].load(Ordering::Relaxed) == 1
    })
    .await;
    assert_eq!(counts.created[1].load(Ordering::Relaxed), 1);
    let path = fixture.root.path().join("node-binding.json");
    let mut binding: crowdb_protocol::mgmt::node::NodeBinding =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    std::fs::remove_file(&path).unwrap();
    until(|| {
        counts.dropped[1].load(Ordering::Relaxed) == 1 && counts.dropped[2].load(Ordering::Relaxed) == 2
    })
    .await;
    binding.bootstrap.operation_id = "replacement-bootstrap".into();
    std::fs::write(&path, serde_json::to_vec(&binding).unwrap()).unwrap();
    fixture.wait_pid(1, Some(101)).await;
    fixture.wait_pid(2, Some(102)).await;
    assert_eq!(counts.created[1].load(Ordering::Relaxed), 2);
    assert_eq!(counts.created[2].load(Ordering::Relaxed), 3);
    fixture.observer.take();
    until(|| {
        (1..=2).all(|node| {
            counts.created[node].load(Ordering::Relaxed) == counts.dropped[node].load(Ordering::Relaxed)
        })
    })
    .await;
}
