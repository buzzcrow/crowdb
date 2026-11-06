// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::allocator::DiskdbClientPool;
use crowdb_paxos_kv_client::ServiceRegistryClient;
use std::sync::atomic::Ordering;

use crate::common::rpc_response_proxy::TestResponseProxy;

#[tokio::test]
#[ignore = "requires the owned Diskdb container fixture"]
async fn allocation_connect_failure_reroutes_but_lost_reply_does_not() {
    use crate::common::hardware::{DG_ID, UNIT_SIZE_BYTES};
    use crowdb_diskdb_client::{DiskdbClientError, DiskdbRpcTransport};
    use crowdb_protocol::common::ChunkId;

    let pool = DiskdbClientPool::new(ServiceRegistryClient::new(crate::common::hardware::kv_client()));
    pool.refresh_endpoints().await.expect("discover live owner");
    let endpoint = pool
        .endpoint_snapshot_for_tests()
        .into_iter()
        .find(|(group, _)| *group == DG_ID)
        .expect("DiskDB advertises group")
        .1;

    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stale = unavailable.local_addr().unwrap().to_string();
    drop(unavailable);
    pool.replace_endpoints_for_tests(vec![(DG_ID, stale)]);
    let allocated = pool
        .allocate_blocks(DG_ID, 1, 1, &ChunkId { high: 0, low: 41 })
        .await
        .expect("unsent request reroutes to discovered owner");
    assert_eq!(allocated.segments.len(), 1);

    // The proxy forwards the allocation, then closes without its response.
    // Group-0 still advertises the direct endpoint, so an unsafe replay would
    // allocate another set there.
    let proxy = TestResponseProxy::start(endpoint.clone()).await;
    proxy.armed.store(true, Ordering::SeqCst);
    pool.replace_endpoints_for_tests(vec![(DG_ID, proxy.endpoint.clone())]);
    let result = pool
        .allocate_blocks_reusing_disks(DG_ID, 1, 1, &ChunkId { high: 0, low: 42 })
        .await;
    assert!(
        matches!(result, Err(DiskdbClientError::Unreachable(_))),
        "{result:?}"
    );
    assert_eq!(proxy.forwarded_handshakes.load(Ordering::SeqCst), 1);
    assert_eq!(proxy.dropped.load(Ordering::SeqCst), 1);

    let transport = DiskdbRpcTransport::new();
    let info = transport
        .get_disk_group_info(&endpoint, DG_ID)
        .await
        .expect("query durable allocations");
    assert_eq!(
        info.group.expect("group exists").busy_bytes,
        2 * u64::from(UNIT_SIZE_BYTES),
        "one rerouted allocation and one allocation with a lost reply"
    );
}
