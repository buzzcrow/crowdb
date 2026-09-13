// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunkdb::allocator::DiskdbClientPool;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};

fn pool() -> DiskdbClientPool {
    let service = ServiceRegistryClient::new(CrowdbKvClient::new(ClientConfig::new(Vec::new())));
    DiskdbClientPool::new(service)
}

#[test]
fn endpoint_refresh_is_one_complete_publication() {
    let pool = Arc::new(pool());
    let old = vec![(1, "old-a".into()), (2, "old-b".into())];
    let new = vec![(3, "new-a".into()), (4, "new-b".into())];
    pool.replace_endpoints_for_tests(old.clone());

    let writer = {
        let pool = Arc::clone(&pool);
        let old = old.clone();
        let new = new.clone();
        std::thread::spawn(move || {
            for index in 0..10_000 {
                pool.replace_endpoints_for_tests(if index % 2 == 0 { new.clone() } else { old.clone() });
            }
        })
    };
    for _ in 0..10_000 {
        let observed = pool.endpoint_snapshot_for_tests();
        assert!(
            observed == old || observed == new,
            "partial snapshot: {observed:?}"
        );
    }
    writer.join().expect("refresh thread panicked");
}
