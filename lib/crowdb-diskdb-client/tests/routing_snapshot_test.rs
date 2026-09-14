// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use crowdb_protocol::common::DiskId;

fn client() -> DiskdbClient {
    let service = ServiceRegistryClient::new(CrowdbKvClient::new(ClientConfig::new(Vec::new())));
    DiskdbClient::new(service, Arc::new(DiskdbRpcTransport::new()))
}

#[test]
fn clones_share_endpoint_and_disk_routes() {
    let first = client();
    let second = first.clone();
    first.replace_endpoints_for_tests(vec![(7, "one".into())]);
    let disk_id = DiskId { high: 0, low: 11 };
    first.learn_disk_route_for_tests(disk_id, 7);

    assert_eq!(second.endpoint_snapshot_for_tests(), vec![(7, "one".into())]);
    assert_eq!(second.disk_group_for_tests(disk_id), Some(7));

    second.replace_endpoints_for_tests(vec![(8, "two".into())]);
    assert_eq!(first.endpoint_snapshot_for_tests(), vec![(8, "two".into())]);
    assert_eq!(first.disk_group_for_tests(disk_id), None);
}

#[test]
fn endpoint_refresh_publishes_only_complete_snapshots() {
    let client = Arc::new(client());
    let old = vec![(1, "old-a".into()), (2, "old-b".into())];
    let new = vec![(3, "new-a".into()), (4, "new-b".into())];
    client.replace_endpoints_for_tests(old.clone());

    let writer = {
        let client = Arc::clone(&client);
        let old = old.clone();
        let new = new.clone();
        std::thread::spawn(move || {
            for index in 0..10_000 {
                client.replace_endpoints_for_tests(if index % 2 == 0 { new.clone() } else { old.clone() });
            }
        })
    };
    for _ in 0..10_000 {
        let observed = client.endpoint_snapshot_for_tests();
        assert!(
            observed == old || observed == new,
            "partial snapshot: {observed:?}"
        );
    }
    writer.join().expect("refresh thread panicked");
}
