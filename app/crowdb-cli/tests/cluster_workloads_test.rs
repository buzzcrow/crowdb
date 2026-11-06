// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Opt-in process regression against an existing disposable normal OCI cluster.
//! The owning fixture stops `ChunkDB` and its upstream producers before comparing
//! whole-group busy bytes, while retaining KV, `DiskDB` and `DiskIO` processes.

use crowdb_console_shared::deployment::target;
use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient, ServiceRegistryClient};
use crowdb_protocol::{
    common::ChunkId,
    diskdb::rpc::{AllocateBlocksRequest, FreeBlocksRequest},
};
use std::{process::Command, sync::Arc};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CROWDB_TEST_CLUSTER_MANAGEMENT_ENDPOINT for a disposable normal OCI cluster"]
async fn mixed_disk_workload_normalizes_frees_predating_its_baseline() {
    let endpoint =
        std::env::var("CROWDB_TEST_CLUSTER_MANAGEMENT_ENDPOINT").expect("explicit disposable target");
    let selected = target::resolve(None, std::slice::from_ref(&endpoint), None)
        .await
        .unwrap();
    selected.require_native_transport().await.unwrap();
    let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
        selected.identity.group0_management_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, selected.identity.group0_rpc_endpoints[0].clone());
    selected.verify_native_authority(&kv).await.unwrap();
    let client = DiskdbClient::new(
        ServiceRegistryClient::from_shared(kv),
        Arc::new(DiskdbRpcTransport::new()),
    );
    client.refresh_endpoints().await.unwrap();
    let group = client.disk_group_ids()[0];
    let owner = uuid::Uuid::new_v4().as_u64_pair();
    let allocated = client
        .allocate_blocks(AllocateBlocksRequest {
            disk_group_id: group,
            unit_count: 256,
            count: 1,
            owner_chunk: Some(ChunkId {
                high: owner.0,
                low: owner.1,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(allocated.segments.len(), 1);
    let busy = client.query_disk_group(group).await.unwrap().disk_groups[0].busy_bytes;
    let freed = client
        .free_blocks(FreeBlocksRequest {
            segments: allocated.segments,
        })
        .await
        .unwrap();
    assert_eq!(freed.freed_count, 1);
    assert_eq!(
        client.query_disk_group(group).await.unwrap().disk_groups[0].busy_bytes,
        busy,
        "fixture must retain a pending free before compaction"
    );
    let binary =
        std::env::var_os("CROWDB_E2E_CLI_BIN").unwrap_or_else(|| env!("CARGO_BIN_EXE_crowdb-cli").into());
    let output = Command::new(binary)
        .env_remove("CROWDB_CLUSTER")
        .env_remove("CROWDB_SYSTEM_IP")
        .env_remove("CROWDB_SYSTEM_PORT")
        .args([
            "--management-endpoint",
            &endpoint,
            "bench",
            "diskdb",
            "mix",
            "--duration-secs",
            "1",
            "--concurrency",
            "1",
            "--metrics-interval",
            "0",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = String::from_utf8(output.stdout).unwrap();
    let counter = |key: &str| {
        report
            .split_whitespace()
            .find_map(|part| part.strip_prefix(key))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    assert!(counter("ops=") > 0);
    assert_eq!(counter("errors="), 0);
    assert!(counter("expected_delta=") > 0);
    assert_eq!(counter("busy_delta="), counter("expected_delta="));
}
