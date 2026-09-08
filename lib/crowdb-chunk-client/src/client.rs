// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Application-facing chunk IO client and prepared large writes.

use std::collections::VecDeque;
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
    AdvanceChunkWriteRequest, AdvanceChunkWriteResponse, AllocateChunkRequest, AllocateChunkResponse,
    AppendChunkRequest, AppendChunkResponse, DeleteChunkRequest, DeleteChunkResponse, Location,
    QueryChunkRequest, QueryChunkResponse, SealChunkRequest, SealChunkResponse, UpdateChunkStripRequest,
    UpdateChunkStripResponse,
};
use crowdb_protocol::diskdb::rpc::Segment;

use crate::metrics::SmallWriteMetrics;
use crate::writer::small_pool::SmallWritePool;
use crate::{
    ChunkAllocator, ChunkClientConfig, ChunkClientMetrics, ChunkIoWriter, ChunkReadPolicy, ChunkReadStream,
    ChunkReader, DiskWriter, LargeAsyncObjectWriter, PartialReadResult, ReadResult, Result, RoutedDiskWriter,
    SmallObjectWriter, SmallWriteMetricsSnapshot, SmallWritePolicy,
};

/// Discovery and transport configuration for [`ChunkIoClient`].
#[derive(Debug, Clone)]
pub struct ChunkIoClientConfig {
    /// KV management endpoints used to discover ChunkDB, DiskIO, and disks.
    pub management_seeds: Vec<String>,
    /// Shared small-object aggregation and elasticity policy.
    pub small_write: SmallWritePolicy,
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
    pub source_reads: u64,
    pub source_read_time: Duration,
    pub assembly_copies: u64,
    pub assembly_copy_bytes: u64,
    pub assembly_copy_time: Duration,
    pub ec_encode_time: Duration,
    pub completion_wait_time: Duration,
}

/// A reusable client that owns discovery and transport wiring.
#[derive(Clone)]
pub struct ChunkIoClient {
    allocator: Arc<dyn crate::ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    topology: Option<Arc<ClientTopology>>,
    metrics: Option<Arc<ChunkClientMetrics>>,
    small_pool: Arc<SmallWritePool>,
    reader: ChunkReader,
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
        let small_pool = SmallWritePool::new(
            chunkdb.clone(),
            disk_writer.clone(),
            config.small_write,
            Arc::new(SmallWriteMetrics::default()),
        )?;
        let reader = ChunkReader::new(chunkdb.clone(), disk_writer.clone(), ChunkReadPolicy::default())
            .map_err(|error| crate::IoError::Internal(error.to_string()))?;
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
            small_pool,
            reader,
        })
    }

    /// Construct from low-level seams. Intended for focused tests and embedded fixtures.
    pub fn from_parts(allocator: Arc<dyn crate::ChunkAllocator>, disk_writer: Arc<dyn DiskWriter>) -> Self {
        Self::from_parts_with_small_policy(allocator, disk_writer, SmallWritePolicy::default())
            .unwrap_or_else(|_| unreachable!("default small-write policy is valid"))
    }

    /// Construct low-level seams with an explicit small-write policy.
    pub fn from_parts_with_small_policy(
        allocator: Arc<dyn crate::ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        small_write: SmallWritePolicy,
    ) -> Result<Self> {
        let small_pool = SmallWritePool::new(
            Arc::clone(&allocator),
            Arc::clone(&disk_writer),
            small_write,
            Arc::new(SmallWriteMetrics::default()),
        )?;
        let reader = ChunkReader::new(
            Arc::clone(&allocator),
            Arc::clone(&disk_writer),
            ChunkReadPolicy::default(),
        )
        .map_err(|error| crate::IoError::Internal(error.to_string()))?;
        Ok(Self {
            allocator,
            disk_writer,
            topology: None,
            metrics: None,
            small_pool,
            reader,
        })
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
        self.small_pool = SmallWritePool::new(
            Arc::clone(&self.allocator),
            Arc::clone(&self.disk_writer),
            (*self.small_pool.policy).clone(),
            Arc::clone(&metrics.small_write),
        )
        .unwrap_or_else(|_| unreachable!("existing small-write policy was already validated"));
        self.reader = ChunkReader::new(
            Arc::clone(&self.allocator),
            Arc::clone(&self.disk_writer),
            ChunkReadPolicy::default(),
        )
        .unwrap_or_else(|_| unreachable!("default read policy is valid"));
        self
    }

    /// Replace the object-read memory and layout-retry policy.
    pub fn with_read_policy(mut self, policy: ChunkReadPolicy) -> ReadResult<Self> {
        self.reader = ChunkReader::new(Arc::clone(&self.allocator), Arc::clone(&self.disk_writer), policy)?;
        Ok(self)
    }

    /// Reconstruct a complete object from writer-produced locations.
    pub async fn read_object(&self, locations: &[Location]) -> ReadResult<Bytes> {
        self.reader.read_object(locations).await
    }

    /// Reconstruct the logical half-open range `[start, end)`.
    pub async fn read_range(&self, locations: &[Location], start: u64, end: u64) -> ReadResult<Bytes> {
        self.reader.read_range(locations, start, end).await
    }

    /// Read a range while preserving exact successful and failed sub-ranges.
    pub async fn read_range_partial(
        &self,
        locations: &[Location],
        start: u64,
        end: u64,
    ) -> ReadResult<PartialReadResult> {
        self.reader.read_range_partial(locations, start, end).await
    }

    /// Build a pull-based, memory-windowed object stream.
    pub fn read_stream(&self, locations: &[Location]) -> ReadResult<ChunkReadStream> {
        self.reader.read_stream(locations)
    }

    /// Reserve one bounded object and return its single-use writer handle.
    pub async fn prepare_small_write(&self, object_size: usize) -> Result<SmallObjectWriter> {
        if object_size == 0 {
            return Ok(SmallObjectWriter::empty());
        }
        let (runtime, reservation) = self.small_pool.reserve(object_size).await?;
        Ok(SmallObjectWriter::new(runtime, object_size, reservation))
    }

    /// Stop admission, drain accepted objects, and finalize shared chunks.
    pub async fn shutdown_small_writes(&self) -> Result<()> {
        self.small_pool.shutdown().await
    }

    /// Snapshot lock-free shared small-write counters and gauges.
    pub fn small_write_metrics(&self) -> SmallWriteMetricsSnapshot {
        let mut snapshot = self.small_pool.metrics.snapshot();
        if snapshot.batches != 0 {
            snapshot.average_batch_fill_ppm = snapshot
                .batch_bytes
                .saturating_mul(1_000_000)
                .checked_div(
                    snapshot
                        .batches
                        .saturating_mul(self.small_pool.policy.max_batch_bytes as u64),
                )
                .unwrap_or(0);
        }
        snapshot
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

    /// Allocate the first chunk for several write sessions before admitting
    /// load. All allocations start together; the returned queue owns every
    /// prepared chunk and callers must write or abort each session.
    pub async fn prepare_large_writes(
        &self,
        count: usize,
        object_size: Option<u64>,
        policy: LargeWritePolicy,
    ) -> Result<VecDeque<PreparedLargeWrite>> {
        let mut writes: VecDeque<_> = (0..count)
            .map(|_| self.prepare_large_write(object_size, policy.clone()))
            .collect();
        let mut preparation_error = None;
        for write in &mut writes {
            if let Err(error) = write.wait_until_prepared().await {
                preparation_error = Some(error);
                break;
            }
        }
        if let Some(error) = preparation_error {
            while let Some(write) = writes.pop_front() {
                let _ = write.abort().await;
            }
            return Err(error);
        }
        Ok(writes)
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

    async fn advance_chunk_write(&self, req: AdvanceChunkWriteRequest) -> Result<AdvanceChunkWriteResponse> {
        self.inner.advance_chunk_write(req).await
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

    async fn fsync(&self, seg: &Segment) -> Result<()> {
        self.inner.fsync(seg).await
    }

    async fn read(&self, seg: &Segment, unit_bytes: u64, segment_offset: u64, length: u32) -> Result<Bytes> {
        self.inner.read(seg, unit_bytes, segment_offset, length).await
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
    /// Wait until this session owns its first allocated chunk.
    pub async fn wait_until_prepared(&mut self) -> Result<()> {
        self.writer.wait_until_prepared().await?;
        Ok(())
    }

    /// Stream the object and return durable locations plus accounting.
    pub async fn write_stream(
        mut self,
        source: impl tokio::io::AsyncRead + Unpin + Send,
    ) -> Result<LargeWriteResult> {
        let write_started = Instant::now();
        let mut operation = self.metrics.as_ref().map(|metrics| metrics.object_write.start());
        let locations = self.writer.write_stream(source, self.object_size).await?;
        let result = build_large_write_result(&self.writer, &self.policy, locations, write_started.elapsed());
        if let Some(metrics) = &self.metrics {
            metrics.logical_bytes.observe(result.logical_bytes);
            metrics.physical_bytes.observe(result.physical_bytes);
        }
        if let Some(operation) = &mut operation {
            operation.mark_success();
        }
        Ok(result)
    }

    /// Send owned blocks directly without the stream fetch/assembly copy.
    pub async fn write_buffers(
        mut self,
        buffers: impl IntoIterator<Item = Bytes>,
    ) -> Result<LargeWriteResult> {
        let write_started = Instant::now();
        let mut operation = self.metrics.as_ref().map(|metrics| metrics.object_write.start());
        for buffer in buffers {
            if let Err(error) = self.writer.on_data(buffer).await {
                let _ = self.writer.abort_pipeline().await;
                return Err(error);
            }
        }
        let locations = match self.writer.on_finish().await {
            Ok(locations) => locations,
            Err(error) => {
                let _ = self.writer.abort_pipeline().await;
                return Err(error);
            }
        };
        let result = build_large_write_result(&self.writer, &self.policy, locations, write_started.elapsed());
        if let Some(metrics) = &self.metrics {
            metrics.logical_bytes.observe(result.logical_bytes);
            metrics.physical_bytes.observe(result.physical_bytes);
        }
        if let Some(operation) = &mut operation {
            operation.mark_success();
        }
        Ok(result)
    }

    /// Time elapsed since the request prepared this write session.
    pub fn preparation_age(&self) -> Duration {
        self.prepared_at.elapsed()
    }

    /// Release a prepared session that will not be written.
    pub async fn abort(mut self) -> Result<()> {
        self.writer.abort_pipeline().await.map(|_| ())
    }
}

fn build_large_write_result(
    writer: &LargeAsyncObjectWriter,
    policy: &LargeWritePolicy,
    locations: Vec<Location>,
    elapsed: Duration,
) -> LargeWriteResult {
    let logical_bytes: u64 = locations.iter().map(|location| location.length).sum();
    let block_bytes = policy.client.read_buffer_size as u64;
    let strip_data_bytes = block_bytes * policy.ec_scheme.data_num as u64;
    let full_strips = logical_bytes / strip_data_bytes;
    let tail_bytes = logical_bytes % strip_data_bytes;
    let strips = full_strips + u64::from(tail_bytes > 0);
    let parity_bytes =
        (full_strips * block_bytes + tail_bytes.min(block_bytes)) * policy.ec_scheme.code_num as u64;
    LargeWriteResult {
        chunks: locations.len(),
        locations,
        logical_bytes,
        physical_bytes: logical_bytes + parity_bytes,
        strips,
        elapsed,
        preparation_stalls: writer.preparation_stalls(),
        preparation_stall_time: writer.preparation_stall_time(),
        source_reads: writer.source_reads,
        source_read_time: writer.source_read_time,
        assembly_copies: writer.assembly_copies,
        assembly_copy_bytes: writer.assembly_copy_bytes,
        assembly_copy_time: writer.assembly_copy_time,
        ec_encode_time: writer.ec_encode_time,
        completion_wait_time: writer.completion_wait_time,
    }
}
