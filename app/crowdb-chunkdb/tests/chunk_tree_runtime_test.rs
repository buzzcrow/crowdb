// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Real KV, `ChunkDB` and `DiskDB` handlers with the existing volatile `DiskIO` wire
//! fixture. This proves the host factory/catalog/Tree path, not disk durability.

mod common;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster};
use common::diskio_stub::TestDiskio;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_chunk_kv::{
    MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange, RequestId,
};
use crowdb_chunk_kv_server::{BootstrapPartitionConfig, ChunkKvStorage};
use crowdb_chunk_stream::{StreamName, StreamPurpose, StreamRecord, StreamRecordState, StreamRegistry};
use crowdb_chunkdb::{metrics::ChunkdbMetrics, service::ChunkdbRpcService};
use crowdb_common::metrics::{MetricsRegistry, MetricsSampler};
use crowdb_paxos_kv_client::{StreamProcessAuthority, StreamProcessLeaseClient, StreamProcessLeaseTiming};
use crowdb_protocol::chunk_kv::Id128;
use crowdb_protocol::chunkdb::rpc::{ChunkState, ChunkType};
use crowdb_tree_ffi::{ChunkPageStoreOptions, Config};
use std::{sync::Arc, time::Instant};

async fn storage(
    cluster: &KvCluster,
    io: &ChunkIoClient,
    process: &Arc<StreamProcessAuthority>,
) -> ChunkKvStorage {
    let storage = ChunkKvStorage::from_parts_parallel_for_tests(
        cluster.make_crowdb_client(),
        io.clone(),
        0,
        300_000,
        2,
    )
    .await
    .unwrap();
    storage.bind_owner_process(process.clone()).unwrap();
    storage
}

struct TestRuntimeStack {
    cluster: KvCluster,
    _diskdb: DiskdbServer,
    _disks: Vec<TestDiskio>,
    harness: ChunkdbHarness,
    rpc: Arc<crowdb_rpc_ffi::RpcServer>,
    io: ChunkIoClient,
    process: Arc<StreamProcessAuthority>,
}
impl TestRuntimeStack {
    async fn start() -> Self {
        let cluster = KvCluster::start_pinned().await;
        seed_hardware(&cluster.make_hardware_client()).await;
        let diskdb = DiskdbServer::start(&cluster).await;
        let mut disks = Vec::new();
        for index in 0..4_u64 {
            disks.push(
                TestDiskio::start(
                    &cluster.make_service_registry_client(),
                    2000 + index,
                    100 + index,
                    10 + index,
                    1000 + index,
                )
                .await,
            );
        }
        let harness = ChunkdbHarness::start(&cluster).await;
        let rpc = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
        rpc.listen("127.0.0.1", 0).unwrap();
        let mut metrics = MetricsRegistry::new();
        let service = Arc::new(ChunkdbRpcService::new(
            harness.handler.clone(),
            Arc::new(ChunkdbMetrics::register(&mut metrics)),
            tokio::runtime::Handle::current(),
        ));
        service.register_handlers(&rpc);
        rpc.start();
        cluster
            .make_service_registry_client()
            .heartbeat_chunkdb(100, &format!("http://127.0.0.1:{}", rpc.port()))
            .await
            .unwrap();
        let slots = crowdb_paxos_kv_client::ChunkSlotMapClient::new(cluster.make_crowdb_client());
        slots
            .initialize_layout(&crowdb_protocol::chunk_slot::ChunkSlotBootstrap {
                service_instances: vec![100],
                storage_groups: vec![crowdb_protocol::chunk_slot::ChunkStorageGroup {
                    store_id: 0,
                    group_id: 1,
                }],
            })
            .await
            .unwrap();
        slots.initialize_service_epochs().await.unwrap();
        let io = ChunkIoClient::connect_with_kv(
            ChunkIoClientConfig {
                management_seeds: vec![],
                diskio_connections_per_endpoint: 1,
                diskio_rpc_workers: 1,
                small_write: SmallWritePolicy::new(ChunkType::Wal),
            },
            cluster.make_crowdb_client(),
        )
        .await
        .unwrap();
        let now = StreamRecord::creation_time_ms();
        let lease = StreamProcessLeaseClient::new(cluster.make_crowdb_client())
            .claim(StreamPurpose::Wal, 71, [7; 16], None, now, 300_000)
            .await
            .unwrap();
        let process = Arc::new(
            StreamProcessAuthority::new(lease, StreamProcessLeaseTiming::default(), now, Instant::now())
                .unwrap(),
        );
        Self {
            cluster,
            _diskdb: diskdb,
            _disks: disks,
            harness,
            rpc,
            io,
            process,
        }
    }
}
impl Drop for TestRuntimeStack {
    fn drop(&mut self) {
        self.rpc.stop();
    }
}

async fn checkpoint_values(
    host: &ChunkKvStorage,
    bootstrap: &BootstrapPartitionConfig,
) -> (
    Partition,
    Arc<crowdb_tree_ffi::PageStore>,
    crowdb_chunk_kv::Checkpoint,
) {
    host.streams()
        .registry()
        .create(StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: bootstrap.stream_name,
            metadata_group_id: 1,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("chunk-kv-partition".into()),
            consumer_id: Some(format!(
                "{:016x}{:016x}",
                bootstrap.partition_id.high, bootstrap.partition_id.low
            )),
            created_at_ms: StreamRecord::creation_time_ms(),
        })
        .await
        .unwrap();
    let stream = host
        .create_registered_stream(bootstrap.stream_name, 1)
        .await
        .unwrap();
    let pages = host
        .open_durable_tree_page_store(
            ChunkPageStoreOptions {
                tree_id: 42,
                owner_epoch: 1,
                open_generation: 0,
                pack_bytes: 0,
                iu_size: 0,
                max_concurrent_packs: 0,
                materialization_bytes_per_pass: 0,
                max_chunk_bytes: 0,
                mirror_copies: 0,
            },
            1,
        )
        .await
        .unwrap();
    let owned_pages = pages.clone();
    let config = Config {
        runtime: Some(host.tree_runtime().clone()),
        ..Default::default()
    };
    let partition = tokio::task::spawn_blocking(move || {
        Partition::open_native(
            PartitionId { high: 17, low: 18 },
            PartitionRange {
                start: Some(vec![]),
                end: None,
            },
            1,
            PartitionConfig {
                retained_results: 1,
                ..Default::default()
            },
            42,
            config,
            owned_pages,
            stream,
        )
    })
    .await
    .unwrap()
    .unwrap();
    let mut checkpoint = None;
    for sequence in 1..=4_u64 {
        partition
            .mutate(
                1,
                RequestId {
                    client_high: 99,
                    client_low: 1,
                    client_sequence: sequence,
                },
                MutationOperation::Put {
                    key: b"key".to_vec(),
                    value: vec![u8::try_from(sequence).unwrap(); 20_000],
                },
            )
            .await
            .unwrap();
        checkpoint = Some(partition.checkpoint(1).await.unwrap());
    }
    let checkpoint = checkpoint.unwrap();
    (partition, pages, checkpoint)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_host_recovers_root_wal_pair_and_collects_real_allocations() {
    let stack = TestRuntimeStack::start().await;
    let host = Arc::new(storage(&stack.cluster, &stack.io, &stack.process).await);
    let bootstrap = BootstrapPartitionConfig {
        partition_id: Id128 { high: 17, low: 18 },
        tree_id: 42,
        stream_name: StreamName { high: 19, low: 20 },
        owner_epoch: 1,
        metadata_group_id: 1,
    };
    let (partition, pages, checkpoint) = checkpoint_values(&host, &bootstrap).await;
    let service = Arc::new(crowdb_chunk_kv_server::ChunkKvService::new(stack.process.clone(), 8).unwrap());
    service.install_partition(&partition).unwrap();
    let mut sampler =
        crowdb_chunk_kv_server::metrics::TreeResources::new(host.tree_runtime().clone(), &service)
            .with_storage(&host);
    sampler.sample();
    let mut resource_log = Vec::new();
    sampler.flush(&mut resource_log, "test").unwrap();
    let resource_log = String::from_utf8(resource_log).unwrap();
    let pins = resource_log
        .lines()
        .find(|line| line.contains("host.tree.resource.chunk_pin_release_slots "))
        .unwrap();
    assert!(pins.contains("limit=1048576"), "{pins}");
    let registered: u64 = pins
        .split_whitespace()
        .find_map(|field| field.strip_prefix("used="))
        .unwrap()
        .parse()
        .unwrap();
    assert!(registered > 0, "{pins}");
    assert!(pins.contains("allocated_sample_max="), "{pins}");
    drop((sampler, service));
    assert!(checkpoint.replay_offset > 0);
    assert_eq!(pages.wal_replay_offset().unwrap(), checkpoint.replay_offset);
    assert!(pages.reclaim_chunk_generations_before(42, checkpoint.root_manifest_generation) > 0);
    let before = stack.harness.store.list_chunks(None, 100).await.unwrap();
    assert!(before
        .iter()
        .any(|chunk| chunk.chunk_type == ChunkType::BtreePage as i32));
    partition.suspend_for_shutdown(1).await.unwrap();
    tokio::task::spawn_blocking(move || drop((partition, pages)))
        .await
        .unwrap();
    host.drain_pin_releases().await;
    drop(host);

    let reopened_host = storage(&stack.cluster, &stack.io, &stack.process).await;
    let recovered = reopened_host.bootstrap_partition(&bootstrap).await.unwrap();
    recovered.activate_recovered(1).unwrap();
    assert_eq!(
        recovered.get(1, b"key", None).await.unwrap().unwrap().value,
        vec![4; 20_000]
    );
    let stable = recovered.checkpoint(1).await.unwrap();
    assert_eq!(stable.replay_offset, checkpoint.replay_offset);
    let frontier = recovered.snapshot();
    for _ in 0..64 {
        if !recovered.maintain_serving_metadata(1).await.unwrap() {
            break;
        }
    }
    let after = stack.harness.store.list_chunks(None, 100).await.unwrap();
    assert!(after
        .iter()
        .any(|chunk| chunk.chunk_type == ChunkType::BtreePage as i32
            && chunk.state == ChunkState::Deleted as i32
            && chunk.strips.is_empty()));
    assert_eq!(recovered.snapshot().applied_seq, frontier.applied_seq);
    assert_eq!(
        recovered.snapshot().journal_durable_offset,
        frontier.journal_durable_offset
    );
    recovered.suspend_for_shutdown(1).await.unwrap();
    tokio::task::spawn_blocking(move || drop(recovered))
        .await
        .unwrap();
    reopened_host.drain_pin_releases().await;
}

async fn runtime_tree_config(host: &ChunkKvStorage, tree_id: u64) -> Config {
    let pages = host
        .open_durable_tree_page_store(
            ChunkPageStoreOptions {
                tree_id,
                owner_epoch: 1,
                open_generation: 0,
                pack_bytes: 0,
                iu_size: 0,
                max_concurrent_packs: 0,
                materialization_bytes_per_pass: 0,
                max_chunk_bytes: 0,
                mirror_copies: 0,
            },
            1,
        )
        .await
        .unwrap();
    Config {
        page_store: Some(pages),
        runtime: Some(host.tree_runtime().clone()),
        ..Config::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunk_partition_snapshot_rebuilds_after_writer_close_without_reopening_source() {
    use crowdb_chunk_kv::{ChunkKvError, CrowdbPartitionTree, PartitionTree};
    let stack = TestRuntimeStack::start().await;
    let host = storage(&stack.cluster, &stack.io, &stack.process).await;
    let config = runtime_tree_config(&host, 42).await;
    let source = tokio::task::spawn_blocking(move || CrowdbPartitionTree::open(42, &config))
        .await
        .unwrap()
        .unwrap();
    source
        .apply(
            1,
            &MutationOperation::Put {
                key: b"a".to_vec(),
                value: b"left".to_vec(),
            },
        )
        .await
        .unwrap();
    source
        .apply(
            2,
            &MutationOperation::Put {
                key: b"z".to_vec(),
                value: vec![b'v'; 9000],
            },
        )
        .await
        .unwrap();
    let (_, coverage, snapshot) = source.checkpoint_snapshot(0).await.unwrap();
    source
        .apply(
            3,
            &MutationOperation::Put {
                key: b"z".to_vec(),
                value: b"new".to_vec(),
            },
        )
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || drop(source)).await.unwrap();
    assert_eq!(snapshot.get(b"z").await.unwrap().unwrap().value, vec![b'v'; 9000]);
    assert!(matches!(
        snapshot.advance_noop(4).await,
        Err(ChunkKvError::InvalidRequest(_))
    ));
    let config = runtime_tree_config(&host, 43).await;
    let (child, stats) = snapshot
        .rebuild_range(
            43,
            &PartitionRange {
                start: Some(b"m".to_vec()),
                end: None,
            },
            config,
        )
        .await
        .unwrap();
    assert_eq!(stats.entries_emitted, 1);
    assert_eq!(child.last_applied_seq(), coverage);
    assert_eq!(child.get(b"z").await.unwrap().unwrap().value, vec![b'v'; 9000]);
    tokio::task::spawn_blocking(move || drop((child, snapshot)))
        .await
        .unwrap();
    host.drain_pin_releases().await;
}

#[test]
fn diskio_fixture_only_acknowledges_valid_fixed_read_ages() {
    use common::diskio_stub::checked_read_age;
    use crowdb_protocol::diskio_fb::FBDiskIoRetCode;
    use crowdb_protocol::timing::{MAX_WRITE_CLOCK_SKEW_MS, MAX_WRITE_REQUEST_AGE_MS};
    let now = 1_000_000;
    assert_eq!(checked_read_age(now, true, 0, now), Ok(true));
    assert_eq!(checked_read_age(now, false, 16, now), Ok(true));
    assert_eq!(checked_read_age(0, false, 16, now), Ok(false));
    for (created, probe, size) in [
        (0, true, 0),
        (now, true, 1),
        (
            now - MAX_WRITE_REQUEST_AGE_MS - MAX_WRITE_CLOCK_SKEW_MS,
            false,
            16,
        ),
        (now + MAX_WRITE_CLOCK_SKEW_MS + 1, false, 16),
        (u64::MAX, false, 16),
    ] {
        assert_eq!(
            checked_read_age(created, probe, size, now),
            Err(FBDiskIoRetCode::OldRequest)
        );
    }
}
