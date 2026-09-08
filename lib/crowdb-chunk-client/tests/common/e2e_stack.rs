// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Real-process chunk-client E2E fixture and disk read-back helpers.

use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_diskio_client::{DiskId, DiskIoRetCode, DiskioClient};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, Location, QueryChunkRequest};
use crowdb_protocol::common::DiskId as ProtoDiskId;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::RpcServer;
use crowdb_test_harness::chunkdb::{self as cdb_harness, ChunkdbProcess, ChunkdbStartOptions};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::{self as ddb_harness, DiskdbProcess};
use crowdb_test_harness::diskio::{self as dio_harness, DiskioProcess, DiskioStartOpts};
use crowdb_test_harness::hardware::{make_disk_id, seed_hardware};

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
    pub cluster: KvCluster,
    _diskdb: DiskdbProcess,
    _diskio: DiskioProcess,
    #[allow(dead_code)]
    chunkdb: Option<ChunkdbProcess>,
    #[allow(dead_code)]
    chunkdb_options: ChunkdbStartOptions,
    pub client: ChunkIoClient,
    rpc_server: Arc<RpcServer>,
    diskio_client: Arc<DiskioClient>,
    diskio_connection: crowdb_rpc_ffi::Connection,
}

impl E2eStack {
    pub async fn start(small_write: SmallWritePolicy) -> Self {
        Self::start_with_chunkdb_options(
            small_write,
            ChunkdbStartOptions {
                allow_unsafe_ec: true,
                ..ChunkdbStartOptions::default()
            },
        )
        .await
    }

    pub async fn start_with_chunkdb_options(
        small_write: SmallWritePolicy,
        chunkdb_options: ChunkdbStartOptions,
    ) -> Self {
        let cluster = KvCluster::start().await;
        let hardware = cluster.make_hardware_client();
        seed_hardware(&hardware, &standard_disk_ids()).await;

        let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
        diskdb.wait_for_ready().await;
        let diskio = DiskioProcess::start(&DiskioStartOpts {
            dummy_disk: "mem",
            kv_seeds: &cluster.mgmt_endpoints,
            disks: &[],
            fault_error_rate: 0.0,
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
            cluster,
            _diskdb: diskdb,
            _diskio: diskio,
            chunkdb: Some(chunkdb),
            chunkdb_options,
            client,
            rpc_server,
            diskio_client,
            diskio_connection,
        }
    }

    #[allow(dead_code)]
    pub async fn wait_for_conversion_active(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let metrics = self
                    .chunkdb
                    .as_ref()
                    .expect("chunkdb is running")
                    .conversion_metrics()
                    .await;
                if metrics["attempts_completed"].as_u64().unwrap_or(0) > 0
                    && metrics["active"].as_u64().unwrap_or(0) > 0
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("conversion never became active");
    }

    #[allow(dead_code)]
    pub async fn crash_and_restart_chunkdb(&mut self) {
        let mut chunkdb = self.chunkdb.take().expect("chunkdb is running");
        chunkdb.crash();
        drop(chunkdb);
        let replacement =
            ChunkdbProcess::start_with_options(&self.cluster.mgmt_endpoints, self.chunkdb_options);
        replacement.wait_for_ready().await;
        self.chunkdb = Some(replacement);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    pub async fn query_chunk(&self, location: &Location) -> Chunk {
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
            self.cluster.mgmt_endpoints.clone(),
        )));
        let service = ServiceRegistryClient::from_shared(kv);
        let chunkdb = ChunkdbClient::new(service, Arc::new(ChunkdbRpcTransport::new()));
        chunkdb.refresh_endpoints().await.unwrap();
        let response = chunkdb
            .query_chunk(QueryChunkRequest {
                chunk_id: location.chunk_id,
            })
            .await
            .unwrap();
        assert!(response.layout_validity_ms > 0);
        response.chunk.expect("location chunk")
    }

    pub async fn read_segment(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        length: u32,
    ) -> Vec<u8> {
        let disk_id = segment.disk_id.expect("segment disk ID");
        let response = self
            .diskio_client
            .read(
                &self.rpc_server,
                &self.diskio_connection,
                DiskId::new(disk_id.high, disk_id.low),
                segment.zone_index,
                segment.unit_offset * unit_bytes + offset,
                length,
                0,
            )
            .expect("send disk read");
        let (code, data) = DiskioClient::await_read_response(response).await.unwrap();
        assert_eq!(code, DiskIoRetCode::Success);
        data.expect("successful disk read data")
    }
}
