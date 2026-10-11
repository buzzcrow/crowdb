// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Real-process chunk-client E2E fixture and disk read-back helpers.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_diskio_client::TestWireDiskioClient as DiskioClient;
use crowdb_protocol::common::DiskId as ProtoDiskId;
use crowdb_rpc_ffi::RpcServer;
use crowdb_test_harness::chunkdb::{self as cdb_harness, ChunkdbProcess, ChunkdbStartOptions};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::{self as ddb_harness, DiskdbProcess};
use crowdb_test_harness::diskio::{self as dio_harness, DiskioProcess, DiskioStartOpts};
use crowdb_test_harness::hardware::{make_disk_id, seed_hardware};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

static E2E_STACK_PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();

pub fn all_binaries_available() -> bool {
    let available = (std::env::var("CROWDB_KV_SERVER_BIN").is_ok()
        || crowdb_test_harness::cluster::crowdb_kv_server_bin().is_some())
        && ddb_harness::crowdb_diskdb_bin().is_some()
        && dio_harness::crowdb_diskio_bin().is_some()
        && cdb_harness::crowdb_chunkdb_bin().is_some();
    if !available {
        eprintln!("skipping real-process E2E: build kv-server, diskdb, diskio, and chunkdb first");
    }
    available
}

fn standard_disk_ids() -> Vec<ProtoDiskId> {
    (1..=12).map(|index| make_disk_id(0, index)).collect()
}

pub struct E2eStack {
    _permit: OwnedSemaphorePermit,
    _cluster: KvCluster,
    _diskdb: DiskdbProcess,
    diskio: DiskioProcess,
    _chunkdb: Option<ChunkdbProcess>,
    pub client: ChunkIoClient,
    _rpc_server: Arc<RpcServer>,
    _diskio_client: Arc<DiskioClient>,
    _diskio_connection: crowdb_rpc_ffi::Connection,
}

impl E2eStack {
    pub async fn start(small_write: SmallWritePolicy) -> Self {
        Self::start_with_disk_and_chunkdb_options(
            small_write,
            "mem",
            0.0,
            ChunkdbStartOptions {
                allow_unsafe_ec: true,
                allow_degraded_failure_domains: true,
                repair_allow_unsafe_placement: true,
                ..ChunkdbStartOptions::default()
            },
        )
        .await
    }

    async fn start_with_disk_and_chunkdb_options(
        small_write: SmallWritePolicy,
        dummy_disk: &str,
        fault_error_rate: f64,
        chunkdb_options: ChunkdbStartOptions,
    ) -> Self {
        let permit = E2E_STACK_PERMITS
            .get_or_init(|| Arc::new(Semaphore::new(1)))
            .clone()
            .acquire_owned()
            .await
            .expect("E2E stack semaphore is never closed");
        let cluster = KvCluster::start().await;
        let hardware = cluster.make_hardware_client();
        seed_hardware(&hardware, &standard_disk_ids()).await;

        let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
        diskdb.wait_for_ready().await;
        let diskio = DiskioProcess::start(&DiskioStartOpts {
            dummy_disk,
            kv_seeds: &cluster.mgmt_endpoints,
            disks: &[],
            fault_error_rate,
            fault_latency_ms: None,
            no_o_direct: false,
        });

        let rpc_server = Arc::new(RpcServer::new(None));
        rpc_server.listen("127.0.0.1", 0).expect("listen for RPC client");
        rpc_server.start();
        std::thread::sleep(Duration::from_millis(50));
        let diskio_connection = rpc_server
            .connect("127.0.0.1", diskio.port)
            .expect("connect to diskio");
        let diskio_client = Arc::new(DiskioClient::new());
        diskio_client.attach(&diskio_connection);
        diskio
            .wait_for_disks(&diskio_client, &rpc_server, &diskio_connection)
            .await;

        let chunkdb = ChunkdbProcess::start_with_options(&cluster.mgmt_endpoints, chunkdb_options);
        chunkdb.wait_for_ready().await;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let client = loop {
            if let Ok(client) = ChunkIoClient::connect(ChunkIoClientConfig {
                management_seeds: cluster.mgmt_endpoints.clone(),
                diskio_connections_per_endpoint: 2,
                diskio_rpc_workers: 1,
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
        tokio::time::sleep(Duration::from_secs(3)).await;

        Self {
            _permit: permit,
            _cluster: cluster,
            _diskdb: diskdb,
            diskio,
            _chunkdb: Some(chunkdb),
            client,
            _rpc_server: rpc_server,
            _diskio_client: diskio_client,
            _diskio_connection: diskio_connection,
        }
    }

    pub fn crash_diskio(&mut self) {
        self.diskio.child.kill().expect("kill DiskIO process");
        self.diskio.child.wait().expect("reap DiskIO process");
    }
}
