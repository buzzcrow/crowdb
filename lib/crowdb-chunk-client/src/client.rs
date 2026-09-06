// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Application-facing chunk IO client and prepared large writes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::EcScheme;
use crowdb_kv_client::{
    ClientConfig, CrowdbKvClient, HardwareClient, RangeBindingClient, ServiceRegistryClient,
};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, DeleteChunkRequest,
    DeleteChunkResponse, Location, QueryChunkRequest, QueryChunkResponse, SealChunkRequest,
    SealChunkResponse, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::diskdb::rpc::Segment;

use crate::{
    ChunkAllocator, ChunkClientConfig, ChunkClientMetrics, DiskWriter, LargeAsyncObjectWriter, Result,
    RoutedDiskWriter,
};

/// Discovery and transport configuration for [`ChunkIoClient`].
#[derive(Debug, Clone)]
pub struct ChunkIoClientConfig {
    /// KV management endpoints used to discover ChunkDB, DiskIO, and disks.
    pub management_seeds: Vec<String>,
}

/// Large-write EC and bounded-buffer policy.
#[derive(Debug, Clone)]
pub struct LargeWritePolicy {
    pub ec_scheme: EcScheme,
    pub client: Arc<ChunkClientConfig>,
}

/// Completed large-write accounting returned to applications.
#[derive(Debug, Clone)]
pub struct LargeWriteResult {
    pub locations: Vec<Location>,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub chunks: usize,
    pub strips: u64,
    pub elapsed: Duration,
    pub preparation_stalls: u64,
    pub preparation_stall_time: Duration,
}

/// A reusable client that owns discovery and transport wiring.
#[derive(Clone)]
pub struct ChunkIoClient {
    allocator: Arc<dyn crate::ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    topology: Option<Arc<ClientTopology>>,
    metrics: Option<Arc<ChunkClientMetrics>>,
}

struct ClientTopology {
    service: ServiceRegistryClient,
    hardware: HardwareClient,
    chunkdb: Arc<ChunkdbClient>,
    disk_writer: Arc<RoutedDiskWriter>,
}

impl ChunkIoClient {
    /// Discover services and build lock-free DiskIO routing.
    pub async fn connect(config: ChunkIoClientConfig) -> Result<Self> {
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(config.management_seeds)));
        let service = ServiceRegistryClient::from_shared(kv.clone());
        let hardware = HardwareClient::from_shared(kv.clone());
        let range_binding = discover_current_range_bindings(&service, kv.clone()).await?;
        let mut chunkdb = ChunkdbClient::new(service.clone(), Arc::new(ChunkdbRpcTransport::new()));
        if let Some(range_binding) = range_binding {
            chunkdb = chunkdb.with_range_binding(range_binding);
        }
        let chunkdb = Arc::new(chunkdb);
        chunkdb.refresh_endpoints().await?;
        let disk_writer = Arc::new(RoutedDiskWriter::connect(&service, &hardware).await?);
        Ok(Self {
            allocator: chunkdb.clone(),
            disk_writer: disk_writer.clone(),
            topology: Some(Arc::new(ClientTopology {
                service,
                hardware,
                chunkdb,
                disk_writer,
            })),
            metrics: None,
        })
    }

    /// Construct from low-level seams. Intended for focused tests and embedded fixtures.
    pub fn from_parts(allocator: Arc<dyn crate::ChunkAllocator>, disk_writer: Arc<dyn DiskWriter>) -> Self {
        Self {
            allocator,
            disk_writer,
            topology: None,
            metrics: None,
        }
    }

    /// Attach aggregate write-path metrics registered by the embedding process.
    #[must_use]
    pub fn with_metrics(mut self, metrics: &Arc<ChunkClientMetrics>) -> Self {
        self.allocator = Arc::new(MetricsChunkAllocator {
            inner: self.allocator,
            metrics: Arc::clone(metrics),
        });
        self.disk_writer = Arc::new(MetricsDiskWriter {
            inner: self.disk_writer,
            metrics: Arc::clone(metrics),
        });
        self.metrics = Some(Arc::clone(metrics));
        self
    }

    /// Refresh `ChunkDB` service endpoints and range ownership routes.
    pub async fn refresh_chunkdb_routes(&self) -> Result<()> {
        let topology = self.topology.as_ref().ok_or_else(|| {
            crate::IoError::Topology("discovery is unavailable for a parts-based client".into())
        })?;
        topology.chunkdb.refresh_routes().await.map_err(Into::into)
    }

    /// Rebuild and atomically publish `DiskIO` service and disk ownership routes.
    pub async fn refresh_diskio_routes(&self) -> Result<()> {
        let topology = self.topology.as_ref().ok_or_else(|| {
            crate::IoError::Topology("discovery is unavailable for a parts-based client".into())
        })?;
        topology
            .disk_writer
            .refresh(&topology.service, &topology.hardware)
            .await
    }

    /// Start bounded chunk preparation as soon as object metadata is known.
    pub fn prepare_large_write(
        &self,
        object_size: Option<u64>,
        policy: LargeWritePolicy,
    ) -> PreparedLargeWrite {
        let mut writer = LargeAsyncObjectWriter::new(
            self.allocator.clone(),
            self.disk_writer.clone(),
            policy.ec_scheme,
            policy.client.clone(),
        );
        writer.prepare(object_size);
        PreparedLargeWrite {
            writer,
            object_size,
            policy,
            prepared_at: Instant::now(),
            metrics: self.metrics.clone(),
        }
    }
}

struct MetricsChunkAllocator {
    inner: Arc<dyn ChunkAllocator>,
    metrics: Arc<ChunkClientMetrics>,
}

#[async_trait]
impl ChunkAllocator for MetricsChunkAllocator {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let mut operation = self.metrics.chunk_allocate.start();
        let result = self.inner.allocate_chunk(req).await;
        if result.is_ok() {
            operation.mark_success();
        }
        result
    }

    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let mut operation = self.metrics.chunk_append.start();
        let result = self.inner.append_chunk(req).await;
        if result.is_ok() {
            operation.mark_success();
        }
        result
    }

    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let mut operation = self.metrics.chunk_seal.start();
        let result = self.inner.seal_chunk(req).await;
        if result.is_ok() {
            operation.mark_success();
        }
        result
    }

    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let mut operation = self.metrics.chunk_delete.start();
        let result = self.inner.delete_chunk(req).await;
        if result.is_ok() {
            operation.mark_success();
        }
        result
    }

    async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        self.inner.update_chunk_strip(req).await
    }

    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        self.inner.query_chunk(req).await
    }
}

struct MetricsDiskWriter {
    inner: Arc<dyn DiskWriter>,
    metrics: Arc<ChunkClientMetrics>,
}

#[async_trait]
impl DiskWriter for MetricsDiskWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        let mut operation = self.metrics.diskio_write.start();
        let result = self.inner.write(seg, unit_bytes, data).await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }
}

async fn discover_current_range_bindings(
    service: &ServiceRegistryClient,
    kv: Arc<CrowdbKvClient>,
) -> Result<Option<RangeBindingClient>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let bindings = RangeBindingClient::from_shared(kv);
    loop {
        bindings
            .refresh()
            .await
            .map_err(|error| crate::IoError::Topology(format!("chunkdb range discovery failed: {error}")))?;
        let instances = service.read_all_chunkdb_instances().await.map_err(|error| {
            crate::IoError::Topology(format!("chunkdb instance discovery failed: {error}"))
        })?;
        let current = bindings.snapshot().iter().all(|binding| {
            instances.iter().any(|(instance_id, instance)| {
                *instance_id == binding.instance_id && instance.rpc_endpoint == binding.rpc_endpoint
            })
        });
        if bindings.is_empty() {
            return Ok(None);
        }
        if current {
            return Ok(Some(bindings));
        }
        if Instant::now() >= deadline {
            return Err(crate::IoError::Topology(
                "chunkdb range bindings did not converge with live instances".into(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One application-owned, single-use large-write session.
pub struct PreparedLargeWrite {
    writer: LargeAsyncObjectWriter,
    object_size: Option<u64>,
    policy: LargeWritePolicy,
    prepared_at: Instant,
    metrics: Option<Arc<ChunkClientMetrics>>,
}

impl PreparedLargeWrite {
    /// Stream the object and return durable locations plus accounting.
    pub async fn write_stream(
        mut self,
        source: impl tokio::io::AsyncRead + Unpin + Send,
    ) -> Result<LargeWriteResult> {
        let mut operation = self.metrics.as_ref().map(|metrics| metrics.object_write.start());
        let locations = self.writer.write_stream(source, self.object_size).await?;
        let logical_bytes: u64 = locations.iter().map(|location| location.length).sum();
        let block_bytes = self.policy.client.read_buffer_size as u64;
        let strip_data_bytes = block_bytes * self.policy.ec_scheme.data_num as u64;
        let full_strips = logical_bytes / strip_data_bytes;
        let tail_bytes = logical_bytes % strip_data_bytes;
        let strips = full_strips + u64::from(tail_bytes > 0);
        let tail_parity_bytes = tail_bytes.min(block_bytes);
        let parity_bytes =
            (full_strips * block_bytes + tail_parity_bytes) * self.policy.ec_scheme.code_num as u64;
        let physical_bytes = logical_bytes + parity_bytes;
        if let Some(metrics) = &self.metrics {
            metrics.logical_bytes.observe(logical_bytes);
            metrics.physical_bytes.observe(physical_bytes);
        }
        if let Some(operation) = &mut operation {
            operation.mark_success();
        }
        Ok(LargeWriteResult {
            chunks: locations.len(),
            locations,
            logical_bytes,
            physical_bytes,
            strips,
            elapsed: self.prepared_at.elapsed(),
            preparation_stalls: self.writer.preparation_stalls(),
            preparation_stall_time: self.writer.preparation_stall_time(),
        })
    }

    /// Time elapsed since the request prepared this write session.
    pub fn preparation_age(&self) -> Duration {
        self.prepared_at.elapsed()
    }
}
