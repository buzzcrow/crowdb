// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, SegmentTarget};
use crowdb_paxos_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, Location, QueryChunkRequest};
use crowdb_protocol::diskdb::rpc::Segment;
use std::sync::Arc;

pub(super) struct TestSeeds {
    pub mgmt_endpoints: Vec<String>,
}

#[derive(Clone, Copy)]
pub(super) struct TestChunkdbOptions {
    pub conversion_enabled: bool,
    pub conversion_min_seal_age_secs: u64,
    pub conversion_scan_interval_secs: u64,
    pub conversion_max_bandwidth_mbps: u64,
    pub conversion_task_lease_secs: u64,
    pub repair_enabled: bool,
    pub repair_scan_interval_secs: u64,
    pub repair_max_concurrency: usize,
    pub repair_memory_bytes: usize,
}
impl Default for TestChunkdbOptions {
    fn default() -> Self {
        Self {
            conversion_enabled: false,
            conversion_min_seal_age_secs: 3600,
            conversion_scan_interval_secs: 30,
            conversion_max_bandwidth_mbps: 50,
            conversion_task_lease_secs: 30,
            repair_enabled: true,
            repair_scan_interval_secs: 1,
            repair_max_concurrency: 4,
            repair_memory_bytes: 64 * 1024 * 1024,
        }
    }
}
impl TestChunkdbOptions {
    fn json(self) -> serde_json::Value {
        serde_json::json!({
        "conversion_enabled": self.conversion_enabled, "conversion_min_seal_age_secs": self.conversion_min_seal_age_secs,
        "conversion_scan_interval_secs": self.conversion_scan_interval_secs, "conversion_max_bandwidth_mbps": self.conversion_max_bandwidth_mbps,
        "conversion_task_lease_secs": self.conversion_task_lease_secs, "repair_enabled": self.repair_enabled,
        "repair_scan_interval_secs": self.repair_scan_interval_secs, "repair_max_concurrency": self.repair_max_concurrency,
        "repair_memory_bytes": self.repair_memory_bytes })
    }
}

pub(super) struct TestStack {
    pub cluster: TestSeeds,
    options: TestChunkdbOptions,
    pub client: ChunkIoClient,
    diskio: DiskioClient,
}
impl TestStack {
    pub async fn start(small_write: SmallWritePolicy) -> Self {
        let ips = crate::common::management::addresses(11);
        let client = ChunkIoClient::connect(ChunkIoClientConfig {
            management_seeds: ips[..3]
                .iter()
                .map(|ip| crate::common::management::origin(ip))
                .collect(),
            diskio_connections_per_endpoint: 2,
            diskio_rpc_workers: 1,
            small_write,
        })
        .await
        .unwrap();
        let diskio = DiskioClient::connect_with_clients(
            ServiceRegistryClient::new(crate::common::hardware::kv_client()),
            HardwareClient::new(crate::common::hardware::kv_client()),
            DiskioClientConfig::default(),
        )
        .await
        .unwrap();
        Self {
            client,
            diskio,
            cluster: TestSeeds {
                mgmt_endpoints: ips[..3]
                    .iter()
                    .map(|ip| crate::common::management::origin(ip))
                    .collect(),
            },
            options: TestChunkdbOptions::default(),
        }
    }
    pub async fn query_chunk(&self, location: &Location) -> Chunk {
        let client = ChunkdbClient::new(
            ServiceRegistryClient::new(crate::common::hardware::kv_client()),
            Arc::new(ChunkdbRpcTransport::new()),
        );
        client.refresh_endpoints().await.unwrap();
        let response = client
            .query_chunk(QueryChunkRequest {
                chunk_id: location.chunk_id,
            })
            .await
            .unwrap();
        assert!(response.layout_validity_ms > 0);
        response.chunk.unwrap()
    }
    pub async fn read_segment(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        length: u32,
    ) -> Vec<u8> {
        let disk = segment.disk_id.unwrap();
        let target = SegmentTarget::new(
            DiskId::new(disk.high, disk.low),
            segment.zone_index,
            segment.unit_offset,
            segment.unit_count,
            u32::try_from(unit_bytes).unwrap(),
        )
        .unwrap();
        self.diskio
            .read(target, offset, length, self.diskio.normal_options())
            .await
            .unwrap()
            .to_vec()
    }
}

impl TestStack {
    pub async fn start_with_chunkdb_options(policy: SmallWritePolicy, options: TestChunkdbOptions) -> Self {
        crate::common::control::configure_chunkdb(options.json()).await;
        let mut stack = Self::start(policy).await;
        stack.options = options;
        stack
    }
    pub async fn start_with_diskio_fault_rate(policy: SmallWritePolicy, fault: f64) -> Self {
        Self::assert_storage("mem", fault);
        Self::start(policy).await
    }
    pub async fn start_null(policy: SmallWritePolicy) -> Self {
        Self::assert_storage("null", 0.0);
        Self::start(policy).await
    }
    fn assert_storage(backend: &str, fault: f64) {
        let path = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap())
            .join("storage-endpoints.json");
        let endpoints: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        for endpoint in endpoints.as_array().unwrap() {
            assert_eq!(endpoint["backend"], backend);
            assert_eq!(
                endpoint["fault_rate"].as_f64().unwrap().to_bits(),
                fault.to_bits()
            );
        }
    }
    pub async fn crash_diskio(&mut self) {
        for index in 5..11 {
            crate::common::control::node("kill", index).await;
        }
    }
    pub async fn crash_and_restart_chunkdb(&mut self) {
        self.crash_and_restart_chunkdb_with_options(self.options).await;
    }
    pub async fn crash_and_restart_chunkdb_with_options(&mut self, options: TestChunkdbOptions) {
        crate::common::control::configure_chunkdb(options.json()).await;
        self.options = options;
    }
    async fn metrics(&self, path: &str) -> serde_json::Value {
        let ips = crate::common::management::addresses(11);
        crate::common::management::client()
            .get(format!("http://{}:7301/{path}", ips[4]))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    pub async fn repair_metrics(&self) -> serde_json::Value {
        self.metrics("repair_metrics").await
    }
    pub async fn conversion_metrics(&self) -> serde_json::Value {
        self.metrics("conversion_metrics").await
    }
    pub async fn wait_for_conversion_active(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let metrics = self.metrics("conversion_metrics").await;
                if metrics["attempts_completed"].as_u64().unwrap_or(0) > 0
                    && metrics["active"].as_u64().unwrap_or(0) > 0
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("conversion never became active");
    }
}
