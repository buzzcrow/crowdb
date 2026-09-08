// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E tests for `LargeObjectWriter` using real kv-server + diskdb +
//! diskio + chunkdb subprocesses.
//!
//! These tests require all four binaries to be built. They are
//! automatically skipped if any binary is missing.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoClientConfig, ChunkIoWriter, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::EcScheme;
use crowdb_diskio_client::{DiskId, DiskIoRetCode, DiskioClient};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, Location, QueryChunkRequest, Strip};
use crowdb_protocol::common::DiskId as ProtoDiskId;
use crowdb_rpc_ffi::RpcServer;
use crowdb_test_harness::chunkdb::{self as cdb_harness, ChunkdbProcess};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::{self as ddb_harness, DiskdbProcess};
use crowdb_test_harness::diskio::{self as dio_harness, DiskioProcess, DiskioStartOpts};
use crowdb_test_harness::hardware::{make_disk_id, seed_hardware, DG_ID, NODE_ID, RACK_ID, UNIT_SIZE_BYTES};

/// 5-disk set for 4+1 EC (5 blocks per strip across 5 disks).
fn standard_disk_ids_5() -> Vec<ProtoDiskId> {
    vec![
        make_disk_id(0, 1),
        make_disk_id(0, 2),
        make_disk_id(0, 3),
        make_disk_id(0, 4),
        make_disk_id(0, 5),
    ]
}

/// Check that all required binaries are available.
fn check_all_binaries() -> bool {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err()
        && crowdb_test_harness::cluster::crowdb_kv_server_bin().is_none()
    {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return false;
    }
    if ddb_harness::crowdb_diskdb_bin().is_none() {
        eprintln!("skipping: crowdb-diskdb binary not found");
        return false;
    }
    if dio_harness::crowdb_diskio_bin().is_none() {
        eprintln!("skipping: crowdb-diskio binary not found");
        return false;
    }
    if cdb_harness::crowdb_chunkdb_bin().is_none() {
        eprintln!("skipping: crowdb-chunkdb binary not found");
        return false;
    }
    true
}

/// EC 4+1: 4 data blocks + 1 parity block per strip.
fn ec_4_1() -> EcScheme {
    EcScheme {
        data_num: 4,
        code_num: 1,
    }
}

/// Set up the full stack: kv cluster + hardware + diskdb + diskio +
/// chunkdb. Returns all the processes + RPC resources + chunkdb client.
struct E2eStack {
    cluster: KvCluster,
    _diskdb: DiskdbProcess,
    _diskio: DiskioProcess,
    _chunkdb: ChunkdbProcess,
    client: ChunkIoClient,
    rpc_server: Arc<RpcServer>,
    diskio_client: Arc<DiskioClient>,
    diskio_connection: crowdb_rpc_ffi::Connection,
}

async fn start_e2e_stack() -> E2eStack {
    start_e2e_stack_with_small_policy(SmallWritePolicy {
        mirror_copies: 1,
        ..SmallWritePolicy::default()
    })
    .await
}

async fn start_e2e_stack_with_small_policy(small_write: SmallWritePolicy) -> E2eStack {
    // 1. Start kv cluster.
    eprintln!("=== starting kv cluster ===");
    let cluster = KvCluster::start().await;
    eprintln!(
        "kv cluster started: group0={}, group1={}",
        cluster.group0_leader_endpoint, cluster.group1_leader_endpoint
    );

    // 2. Seed hardware with 5 disks.
    eprintln!("=== seeding hardware (5 disks) ===");
    let hw = cluster.make_hardware_client();
    let disk_ids = standard_disk_ids_5();
    seed_hardware(&hw, &disk_ids).await;
    eprintln!(
        "hardware seeded: rack={RACK_ID}, node={NODE_ID}, dg={DG_ID}, 5 disks, unit={}KB",
        UNIT_SIZE_BYTES / 1024
    );

    // 3. Start diskdb (block allocator).
    eprintln!("=== starting crowdb-diskdb ===");
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
    diskdb.wait_for_ready().await;
    eprintln!("crowdb-diskdb ready");

    // 4. Start diskio (block I/O, NullDisk backend).
    eprintln!("=== starting crowdb-diskio (mem) ===");
    let diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        no_o_direct: false,
    });

    let rpc_server = Arc::new(RpcServer::new(None));
    rpc_server.listen("127.0.0.1", 0).expect("listen for rpc client");
    rpc_server.start();
    std::thread::sleep(Duration::from_millis(50));

    let conn = rpc_server
        .connect("127.0.0.1", diskio.port)
        .expect("connect to diskio");
    let dio_client = Arc::new(DiskioClient::new());
    dio_client.attach(&conn);

    diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;
    eprintln!("crowdb-diskio ready (disks discovered)");

    // 5. Start chunkdb (chunk manager).
    eprintln!("=== starting crowdb-chunkdb ===");
    let chunkdb = ChunkdbProcess::start_with_unsafe_ec(&cluster.mgmt_endpoints, true);
    chunkdb.wait_for_ready().await;
    eprintln!("crowdb-chunkdb ready");

    // 6. Build the application-facing client from management seeds.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let client = loop {
        if let Ok(client) = ChunkIoClient::connect(ChunkIoClientConfig {
            management_seeds: cluster.mgmt_endpoints.clone(),
            small_write: small_write.clone(),
        })
        .await
        {
            break client;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "chunk IO client failed to discover services within 10s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    eprintln!("chunk IO client connected");

    // Give the chunkdb server time to discover the diskdb endpoint via
    // the service registry topology refresh (runs every 2s).
    tokio::time::sleep(Duration::from_secs(3)).await;
    eprintln!("chunkdb topology settled");

    E2eStack {
        cluster,
        _diskdb: diskdb,
        _diskio: diskio,
        _chunkdb: chunkdb,
        client,
        rpc_server,
        diskio_client: dio_client,
        diskio_connection: conn,
    }
}

async fn write_small_object(client: &ChunkIoClient, data: Bytes) -> Location {
    let mut writer = client.prepare_small_write(data.len()).await.unwrap();
    writer.on_data(data).await.unwrap();
    writer.on_finish().await.unwrap().remove(0)
}

async fn query_chunk(client: &ChunkdbClient, location: &Location) -> Chunk {
    client
        .query_chunk(QueryChunkRequest {
            chunk_id: location.chunk_id,
        })
        .await
        .unwrap()
        .chunk
        .expect("location chunk")
}

async fn assert_location_on_every_mirror(
    stack: &E2eStack,
    chunk: &Chunk,
    location: &Location,
    expected: &[u8],
) {
    let strip = chunk
        .strips
        .iter()
        .find(|strip| {
            let start = u64::from(strip.chunk_offset) * 1024;
            let end = start + u64::from(strip.capacity) * 1024;
            start <= location.offset && location.offset + location.length <= end
        })
        .expect("location strip");
    let Strip::MirrorStrip(mirror) = strip.strip.as_ref().expect("strip body") else {
        panic!("small-object strip must be mirrored");
    };
    let unit_bytes = u64::from(strip.unit_kb) * 1024;
    let relative = location.offset - u64::from(strip.chunk_offset) * 1024;
    for segment in &mirror.segments {
        let disk_id = segment.disk_id.expect("segment disk id");
        let read = stack
            .diskio_client
            .read(
                &stack.rpc_server,
                &stack.diskio_connection,
                DiskId::new(disk_id.high, disk_id.low),
                segment.zone_index,
                segment.unit_offset * unit_bytes + relative,
                u32::try_from(location.length).unwrap(),
                0,
            )
            .expect("send replica read");
        let (code, bytes) = DiskioClient::await_read_response(read).await.unwrap();
        assert_eq!(code, DiskIoRetCode::Success);
        assert_eq!(bytes.as_deref(), Some(expected));
    }
}

async fn wait_for_small_metrics(client: &ChunkIoClient, predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "small-write metrics did not converge: {:?}",
            client.small_write_metrics()
        )
    });
}

/// Generate deterministic test data of the given size.
fn make_test_data(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from((i * 17 + 37) % 256).unwrap())
        .collect()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn e2e_case1_single_chunk_multi_strip() {
    if !check_all_binaries() {
        return;
    }

    let stack = start_e2e_stack().await;
    let ec = ec_4_1();

    // 12 MB, 1 MB blocks, 4 data blocks per strip → 4 MB per strip.
    // 12 MB / 4 MB = 3 strips. max_chunk_size = 1 GB → all 3 strips
    // in 1 chunk.
    let config = Arc::new(ChunkClientConfig {
        max_chunk_size: 1024 * 1024 * 1024,
        prefetch_strips_per_chunk: 2,
        parity_depth: 2,
        chunk_preparation_depth: 1,
        read_buffer_size: 1024 * 1024,
        max_cached_buffer: 4 * 1024 * 1024,
        memory_budget: 0,
    });

    let data = make_test_data(12 * 1024 * 1024);
    eprintln!("=== writing 12 MB ===");
    let result = stack
        .client
        .prepare_large_write(
            Some(12 * 1024 * 1024_u64),
            LargeWritePolicy {
                ec_scheme: ec,
                client: config,
            },
        )
        .write_stream(data.as_slice())
        .await
        .expect("write_stream should succeed");
    let locs = result.locations;

    // 1 chunk → 1 Location.
    assert_eq!(locs.len(), 1, "Case 1: expected 1 Location (single chunk)");
    assert_eq!(
        locs[0].length,
        12 * 1024 * 1024,
        "Case 1: Location length should be 12 MB"
    );
    eprintln!("Case 1 OK: 1 Location, length = 12 MB");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn e2e_case2_chunk_rotation() {
    if !check_all_binaries() {
        return;
    }

    let stack = start_e2e_stack().await;
    let ec = ec_4_1();

    // 20 MB, 1 MB blocks, 4 data blocks per strip → 4 MB per strip.
    // max_chunk_size = 8 MB → 2 strips per chunk.
    // 20 MB / 4 MB = 5 strips → 3 chunks (2 × 2 strips + 1 × 1 strip).
    let config = Arc::new(ChunkClientConfig {
        max_chunk_size: 8 * 1024 * 1024,
        prefetch_strips_per_chunk: 2,
        parity_depth: 2,
        chunk_preparation_depth: 1,
        read_buffer_size: 1024 * 1024,
        max_cached_buffer: 4 * 1024 * 1024,
        memory_budget: 0,
    });

    let data = make_test_data(20 * 1024 * 1024);
    eprintln!("=== writing 20 MB ===");
    let result = stack
        .client
        .prepare_large_write(
            Some(20 * 1024 * 1024_u64),
            LargeWritePolicy {
                ec_scheme: ec,
                client: config,
            },
        )
        .write_stream(data.as_slice())
        .await
        .expect("write_stream should succeed");
    let locs = result.locations;

    // 5 strips / 2 strips per chunk = 2.5 → 3 chunks → 3 Locations.
    assert_eq!(locs.len(), 3, "Case 2: expected 3 Locations (rotation)");
    let total: u64 = locs.iter().map(|l| l.length).sum();
    assert_eq!(total, 20 * 1024 * 1024, "Case 2: total length should be 20 MB");

    // Chunks 1-2 = 8 MB each (2 strips), chunk 3 = 4 MB (1 strip).
    for (i, loc) in locs.iter().enumerate() {
        if i < 2 {
            assert_eq!(
                loc.length,
                8 * 1024 * 1024,
                "Case 2: chunk {i} should be 8 MB (2 strips)"
            );
        } else {
            assert_eq!(
                loc.length,
                4 * 1024 * 1024,
                "Case 2: chunk 2 (last) should be 4 MB (1 strip)"
            );
        }
    }
    eprintln!("Case 2 OK: 3 Locations, total = 20 MB");
}

#[tokio::test]
async fn small_object_e2e_elasticity_rotation_and_mirror_data() {
    if !check_all_binaries() {
        return;
    }
    let stack = start_e2e_stack_with_small_policy(SmallWritePolicy {
        memory_budget: 16 * 1024 * 1024,
        queue_capacity: 256,
        min_pipelines: 1,
        max_pipelines: 4,
        max_batch_objects: 1,
        scale_out_queue_bytes: 32 * 1024,
        scale_out_queue_objects: 2,
        scale_in_delay: Duration::from_millis(50),
        control_interval: Duration::from_millis(1),
        cooldown: Duration::from_millis(1),
        chunk_capacity: 2 * 1024 * 1024,
        mirror_copies: 1,
        ..SmallWritePolicy::default()
    })
    .await;

    let first_data = Bytes::from(vec![3; 700 * 1024]);
    let second_data = Bytes::from(vec![5; 400 * 1024]);
    let third_data = Bytes::from(vec![7; 700 * 1024]);
    let first = write_small_object(&stack.client, first_data.clone()).await;
    let second = write_small_object(&stack.client, second_data.clone()).await;
    let third = write_small_object(&stack.client, third_data.clone()).await;
    assert_eq!(first.chunk_id, second.chunk_id);
    assert_eq!(first.offset, 0);
    assert_eq!(second.offset, 1024 * 1024);
    assert_ne!(second.chunk_id, third.chunk_id);
    assert_eq!(third.offset, 0);

    let task_count = 64;
    let barrier = Arc::new(tokio::sync::Barrier::new(task_count + 1));
    let mut tasks = Vec::new();
    for value in 0..task_count {
        let client = stack.client.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            let object = Bytes::from(vec![u8::try_from(value).unwrap(); 64 * 1024]);
            let mut writer = client.prepare_small_write(object.len()).await.unwrap();
            writer.on_data(object.clone()).await.unwrap();
            barrier.wait().await;
            (object, writer.on_finish().await.unwrap().remove(0))
        }));
    }
    barrier.wait().await;
    wait_for_small_metrics(&stack.client, || stack.client.small_write_metrics().scale_out > 0).await;
    assert!(stack.client.small_write_metrics().active_pipelines > 1);

    let mut completed = Vec::new();
    for task in tasks {
        completed.push(task.await.unwrap());
    }
    wait_for_small_metrics(&stack.client, || {
        let metrics = stack.client.small_write_metrics();
        metrics.active_pipelines == 1 && metrics.draining_pipelines == 0 && metrics.scale_in > 0
    })
    .await;
    let metrics = stack.client.small_write_metrics();
    assert!(metrics.scale_out > 0);
    assert!(metrics.scale_in > 0);
    assert_eq!(metrics.draining_pipelines, 0);

    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        stack.cluster.mgmt_endpoints.clone(),
    )));
    let service = ServiceRegistryClient::from_shared(kv);
    let chunkdb = ChunkdbClient::new(service, Arc::new(ChunkdbRpcTransport::new()));
    chunkdb.refresh_endpoints().await.unwrap();
    let first_chunk = query_chunk(&chunkdb, &first).await;
    let second_chunk = query_chunk(&chunkdb, &third).await;
    assert_eq!(first_chunk.state, ChunkState::Sealed as i32);
    assert!(first_chunk.strips.len() >= 2);
    assert_location_on_every_mirror(&stack, &first_chunk, &first, &first_data).await;
    assert_location_on_every_mirror(&stack, &first_chunk, &second, &second_data).await;
    assert_location_on_every_mirror(&stack, &second_chunk, &third, &third_data).await;

    let (burst_data, burst_location) = &completed[0];
    let burst_chunk = query_chunk(&chunkdb, burst_location).await;
    assert_location_on_every_mirror(&stack, &burst_chunk, burst_location, burst_data).await;

    stack.client.shutdown_small_writes().await.unwrap();
    assert_eq!(stack.client.small_write_metrics().active_pipelines, 0);
}
