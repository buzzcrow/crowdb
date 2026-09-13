// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Routed semantic `DiskIO` client.

use std::collections::HashMap;
#[cfg(feature = "test-util")]
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient};
use crowdb_rpc_ffi::{
    ConnectionPoolError, ConnectionPoolIndex, OwnedClientRoute, RpcError, RpcServer, SelectedConnection,
};

use crate::client::{DiskIoRetCode, WireClient, WireError, WireWriteTarget};
use crate::topology::{self, DiskRoute};
use crate::{DiskId, DiskioError, DiskioResult, DiskioStatus, SegmentTarget};

/// Independently bounded `DiskIO` traffic lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrafficLane {
    Normal,
    Priority,
}

/// When a successful write may be acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Buffered,
    Fsync,
}

/// Per-operation scheduling and total-deadline policy.
#[derive(Debug, Clone, Copy)]
pub struct OperationOptions {
    pub lane: TrafficLane,
    pub deadline: Instant,
}

impl OperationOptions {
    #[must_use]
    pub fn within(timeout: Duration) -> Self {
        Self {
            lane: TrafficLane::Normal,
            deadline: Instant::now() + timeout,
        }
    }

    #[must_use]
    pub fn priority(mut self) -> Self {
        self.lane = TrafficLane::Priority;
        self
    }
}

/// Discovery, pool, and retry bounds for a semantic client.
#[derive(Debug, Clone)]
pub struct DiskioClientConfig {
    pub management_seeds: Vec<String>,
    pub normal_connections_per_endpoint: usize,
    pub priority_connections_per_endpoint: usize,
    pub max_endpoints: usize,
    pub rpc_workers: u32,
    pub max_pending_calls: usize,
    pub send_queue_capacity: u32,
    pub retry_attempts: usize,
    pub reconnect_initial_backoff: Duration,
    pub reconnect_max_backoff: Duration,
    pub default_timeout: Duration,
}

/// Opaque retained routes for a native storage engine.
pub struct NativeDiskIoRoutes {
    routes: Vec<(DiskId, OwnedClientRoute)>,
}

/// One injected static route for real-wire integration tests.
#[cfg(feature = "test-util")]
#[derive(Debug, Clone)]
pub struct TestDiskRoute {
    pub disk_id: DiskId,
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub instance_id: u64,
    pub endpoint: String,
}

impl NativeDiskIoRoutes {
    /// Consume the opaque set at the native FFI boundary.
    #[must_use]
    pub fn into_owned_routes(self) -> Vec<(DiskId, OwnedClientRoute)> {
        self.routes
    }
}

impl Default for DiskioClientConfig {
    fn default() -> Self {
        Self {
            management_seeds: Vec::new(),
            normal_connections_per_endpoint: 2,
            priority_connections_per_endpoint: 1,
            max_endpoints: 1_024,
            rpc_workers: 2,
            max_pending_calls: 4_096,
            send_queue_capacity: 4_096,
            retry_attempts: 3,
            reconnect_initial_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(100),
            default_timeout: Duration::from_secs(5),
        }
    }
}

impl DiskioClientConfig {
    fn validate(&self, require_seeds: bool) -> DiskioResult<()> {
        if require_seeds && self.management_seeds.is_empty() {
            return Err(DiskioError::InvalidInput(
                "at least one management seed is required".into(),
            ));
        }
        if self.normal_connections_per_endpoint == 0
            || self.priority_connections_per_endpoint == 0
            || self.max_endpoints == 0
            || self.rpc_workers == 0
            || self.max_pending_calls == 0
            || self.send_queue_capacity == 0
            || self.retry_attempts == 0
            || self.reconnect_initial_backoff.is_zero()
            || self.reconnect_max_backoff < self.reconnect_initial_backoff
            || self.default_timeout.is_zero()
        {
            return Err(DiskioError::InvalidInput("invalid DiskIO client bounds".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct RouteGroup {
    normal_generation: u64,
    priority_generation: u64,
}

#[derive(Debug)]
struct RouteSnapshot {
    generation: u64,
    published_at: Instant,
    routes: HashMap<DiskId, DiskRoute>,
    groups: HashMap<Arc<str>, RouteGroup>,
    nodes: usize,
    endpoints: usize,
}

impl RouteSnapshot {
    fn empty() -> Self {
        Self {
            generation: 0,
            published_at: Instant::now(),
            routes: HashMap::new(),
            groups: HashMap::new(),
            nodes: 0,
            endpoints: 0,
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    inflight: AtomicU64,
    normal_inflight: AtomicU64,
    priority_inflight: AtomicU64,
    retries: AtomicU64,
    queue_rejections: AtomicU64,
    ambiguous_writes: AtomicU64,
    connect_attempts: AtomicU64,
    reconnect_attempts: AtomicU64,
    read_operations: AtomicU64,
    write_operations: AtomicU64,
    fsync_operations: AtomicU64,
    read_latency_us: AtomicU64,
    write_latency_us: AtomicU64,
    fsync_latency_us: AtomicU64,
}

#[derive(Clone, Copy)]
enum OperationKind {
    Read,
    Write,
    Fsync,
}

impl OperationKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Fsync => "fsync",
        }
    }
}

struct InflightGuard<'a> {
    counters: &'a Counters,
    kind: OperationKind,
    started: Instant,
    counted_inflight: bool,
    lane_inflight: Option<&'a AtomicU64>,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        if self.counted_inflight {
            self.counters.inflight.fetch_sub(1, Ordering::Release);
            self.lane_inflight
                .expect("admitted operation must retain its lane counter")
                .fetch_sub(1, Ordering::Release);
        }
        let latency = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let (operations, total) = match self.kind {
            OperationKind::Read => (&self.counters.read_operations, &self.counters.read_latency_us),
            OperationKind::Write => (&self.counters.write_operations, &self.counters.write_latency_us),
            OperationKind::Fsync => (&self.counters.fsync_operations, &self.counters.fsync_latency_us),
        };
        operations.fetch_add(1, Ordering::Relaxed);
        total.fetch_add(latency, Ordering::Relaxed);
    }
}

/// Complete routed semantic `DiskIO` client.
pub struct DiskioClient {
    config: DiskioClientConfig,
    service: Option<ServiceRegistryClient>,
    hardware: Option<HardwareClient>,
    wire: Arc<WireClient>,
    server: Arc<RpcServer>,
    normal: ConnectionPoolIndex,
    priority: ConnectionPoolIndex,
    routes: ArcSwap<RouteSnapshot>,
    next_route_generation: AtomicU64,
    counters: Counters,
}

impl std::fmt::Debug for DiskioClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiskioClient")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl DiskioClient {
    /// Connect a semantic client to an injected static route set.
    ///
    /// # Errors
    ///
    /// Returns an input, endpoint, connection, or duplicate-route error before
    /// publishing the static generation.
    #[cfg(feature = "test-util")]
    pub fn connect_for_tests(routes: Vec<TestDiskRoute>, config: DiskioClientConfig) -> DiskioResult<Self> {
        config.validate(false)?;
        let server = Arc::new(RpcServer::with_engines(None, 1, config.rpc_workers));
        server.set_send_queue_capacity(config.send_queue_capacity);
        server
            .listen("127.0.0.1", 0)
            .map_err(|error| DiskioError::TransportUnavailable(format!("start RPC client: {error}")))?;
        server.start();
        let client = Self {
            wire: Arc::new(WireClient::with_completion_capacity(config.max_pending_calls)),
            server,
            normal: ConnectionPoolIndex::new(
                config.normal_connections_per_endpoint,
                Some(config.max_endpoints),
            ),
            priority: ConnectionPoolIndex::new(
                config.priority_connections_per_endpoint,
                Some(config.max_endpoints),
            ),
            routes: ArcSwap::from_pointee(RouteSnapshot::empty()),
            next_route_generation: AtomicU64::new(1),
            counters: Counters::default(),
            service: None,
            hardware: None,
            config,
        };
        let mut disk_routes = HashMap::with_capacity(routes.len());
        let mut nodes = HashSet::new();
        let mut endpoints = HashSet::new();
        for route in routes {
            topology::parse_endpoint(&route.endpoint)?;
            let endpoint: Arc<str> = Arc::from(route.endpoint);
            let pool_key: Arc<str> = Arc::from(format!("{}@{}", route.instance_id, endpoint));
            let disk_route = DiskRoute {
                rack_id: route.rack_id,
                node_id: route.node_id,
                disk_group_id: route.disk_group_id,
                instance_id: route.instance_id,
                endpoint: Arc::clone(&endpoint),
                pool_key,
            };
            if disk_routes.insert(route.disk_id, disk_route).is_some() {
                return Err(DiskioError::TopologyInconsistent(format!(
                    "duplicate static route for disk {}:{}",
                    route.disk_id.high, route.disk_id.low
                )));
            }
            nodes.insert((route.rack_id, route.node_id));
            endpoints.insert(endpoint);
        }
        let observed = client.routes.load_full();
        client.publish_draft(
            &observed,
            topology::TopologyDraft {
                routes: disk_routes,
                nodes: nodes.len(),
                endpoints: endpoints.len(),
            },
        )?;
        Ok(client)
    }

    /// Connect using only group-0 management seeds.
    ///
    /// # Errors
    ///
    /// Returns an input, control-plane, topology, or connection error.
    pub async fn connect(config: DiskioClientConfig) -> DiskioResult<Self> {
        config.validate(true)?;
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
            config.management_seeds.clone(),
        )));
        kv.refresh_topology()
            .await
            .map_err(|error| DiskioError::TopologyUnavailable(error.to_string()))?;
        Self::connect_with_clients(
            ServiceRegistryClient::from_shared(Arc::clone(&kv)),
            HardwareClient::from_shared(kv),
            config,
        )
        .await
    }

    /// Connect with injected group-0 service and hardware clients.
    ///
    /// # Errors
    ///
    /// Returns an input, topology, RPC startup, or endpoint connection error.
    pub async fn connect_with_clients(
        service: ServiceRegistryClient,
        hardware: HardwareClient,
        config: DiskioClientConfig,
    ) -> DiskioResult<Self> {
        config.validate(false)?;
        let server = Arc::new(RpcServer::with_engines(None, 1, config.rpc_workers));
        server.set_send_queue_capacity(config.send_queue_capacity);
        server
            .listen("127.0.0.1", 0)
            .map_err(|error| DiskioError::TransportUnavailable(format!("start RPC client: {error}")))?;
        server.start();
        let client = Self {
            wire: Arc::new(WireClient::with_completion_capacity(config.max_pending_calls)),
            server,
            normal: ConnectionPoolIndex::new(
                config.normal_connections_per_endpoint,
                Some(config.max_endpoints),
            ),
            priority: ConnectionPoolIndex::new(
                config.priority_connections_per_endpoint,
                Some(config.max_endpoints),
            ),
            routes: ArcSwap::from_pointee(RouteSnapshot::empty()),
            next_route_generation: AtomicU64::new(1),
            counters: Counters::default(),
            service: Some(service),
            hardware: Some(hardware),
            config,
        };
        client.refresh().await?;
        Ok(client)
    }

    /// Rebuild and atomically publish one complete authoritative route generation.
    ///
    /// # Errors
    ///
    /// Returns a control-plane, topology, or connection error without
    /// publishing a partial generation.
    pub async fn refresh(&self) -> DiskioResult<u64> {
        let observed = self.routes.load_full();
        let service = self.service.as_ref().ok_or_else(|| {
            DiskioError::TopologyUnavailable("static test topology cannot be refreshed".into())
        })?;
        let hardware = self.hardware.as_ref().ok_or_else(|| {
            DiskioError::TopologyUnavailable("static test topology cannot be refreshed".into())
        })?;
        let draft = topology::discover(service, hardware).await?;
        self.publish_draft(&observed, draft)
    }

    fn publish_draft(
        &self,
        observed: &Arc<RouteSnapshot>,
        draft: topology::TopologyDraft,
    ) -> DiskioResult<u64> {
        let mut endpoints = HashMap::<Arc<str>, Arc<str>>::new();
        for route in draft.routes.values() {
            endpoints
                .entry(Arc::clone(&route.pool_key))
                .or_insert_with(|| Arc::clone(&route.endpoint));
        }
        let mut groups = HashMap::with_capacity(endpoints.len());
        for (pool_key, endpoint) in endpoints {
            let normal = self.acquire_from(&self.normal, &pool_key, &endpoint, false)?;
            let priority = self.acquire_from(&self.priority, &pool_key, &endpoint, false)?;
            groups.insert(
                pool_key,
                RouteGroup {
                    normal_generation: normal.generation(),
                    priority_generation: priority.generation(),
                },
            );
        }
        let generation = self.next_route_generation.fetch_add(1, Ordering::Relaxed);
        let replacement = Arc::new(RouteSnapshot {
            generation,
            published_at: Instant::now(),
            routes: draft.routes,
            groups,
            nodes: draft.nodes,
            endpoints: draft.endpoints,
        });
        let previous = self.routes.compare_and_swap(observed, Arc::clone(&replacement));
        if !Arc::ptr_eq(&previous, observed) {
            self.retire_candidate_groups(&replacement, &previous);
            return Ok(previous.generation);
        }
        self.retire_candidate_groups(observed, &replacement);
        Ok(generation)
    }

    /// Read an exact byte range relative to an allocated segment.
    ///
    /// # Errors
    ///
    /// Returns a typed input, topology, transport, protocol, or disk error.
    pub async fn read(
        &self,
        target: SegmentTarget,
        offset: u64,
        length: u32,
        options: OperationOptions,
    ) -> DiskioResult<Bytes> {
        let (zone_offset, _) = target.checked_range(offset, length as usize)?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        let _inflight = self.begin_operation(OperationKind::Read, options.lane)?;
        let mut backoff = self.config.reconnect_initial_backoff;
        for attempt in 0..self.config.retry_attempts {
            let (route, selected) = self.select(target.disk_id, options.lane)?;
            let future = match self.wire.read(
                &self.server,
                &selected,
                target.disk_id,
                target.zone_index,
                zone_offset,
                length,
                0,
            ) {
                Ok(future) => future,
                Err(error) => {
                    let error = self.classify_wire(error, OperationKind::Read);
                    if error.is_retryable_read() && attempt + 1 < self.config.retry_attempts {
                        self.prepare_retry(&route, options.lane, &selected, &mut backoff, options.deadline)
                            .await?;
                        continue;
                    }
                    return Err(error);
                }
            };
            let result = self.await_read(future, length, options.deadline).await;
            match result {
                Ok(data) => return Ok(data),
                Err(error) if error.is_retryable_read() && attempt + 1 < self.config.retry_attempts => {
                    self.prepare_retry(&route, options.lane, &selected, &mut backoff, options.deadline)
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(DiskioError::DeadlineExceeded)
    }

    /// Write caller-owned bytes to an exact segment-relative range.
    ///
    /// # Errors
    ///
    /// Returns a typed input, topology, backpressure, disk, durability, or
    /// ambiguous-outcome error.
    pub async fn write(
        &self,
        target: SegmentTarget,
        offset: u64,
        data: Bytes,
        durability: Durability,
        options: OperationOptions,
    ) -> DiskioResult<()> {
        if data.is_empty() {
            return Err(DiskioError::InvalidInput("write data must be nonempty".into()));
        }
        let (zone_offset, _) = target.checked_range(offset, data.len())?;
        let _inflight = self.begin_operation(OperationKind::Write, options.lane)?;
        let mut backoff = self.config.reconnect_initial_backoff;
        let mut last_error = None;
        let mut only_backpressure = true;
        for attempt in 0..self.config.retry_attempts {
            let (route, selected) = self.select(target.disk_id, options.lane)?;
            let future = match self.wire.write_segment_bytes(
                &self.server,
                &selected,
                WireWriteTarget {
                    disk_id: target.disk_id,
                    zone_index: target.zone_index,
                    zone_offset,
                    ordering_zone_offset: target.segment_base,
                },
                data.clone(),
            ) {
                Ok(future) => future,
                Err(error) => {
                    let error = self.classify_wire(error, OperationKind::Write);
                    only_backpressure &= matches!(error, DiskioError::Backpressure(_));
                    last_error = Some(error);
                    if attempt + 1 < self.config.retry_attempts {
                        if self
                            .prepare_retry(&route, options.lane, &selected, &mut backoff, options.deadline)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    break;
                }
            };
            match self
                .await_write(future, options.deadline, OperationKind::Write)
                .await
            {
                Ok(()) => {
                    if durability == Durability::Fsync {
                        self.fsync_inner(target.disk_id, options, true).await?;
                    }
                    return Ok(());
                }
                Err(DiskioError::PartialWrite) => return Err(DiskioError::PartialWrite),
                Err(error @ (DiskioError::DiskFailure(_) | DiskioError::InvalidInput(_))) => {
                    return Err(error);
                }
                Err(error) => {
                    only_backpressure = false;
                    last_error = Some(error);
                    if attempt + 1 < self.config.retry_attempts
                        && self
                            .prepare_retry(&route, options.lane, &selected, &mut backoff, options.deadline)
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
            }
        }
        self.counters.ambiguous_writes.fetch_add(1, Ordering::Relaxed);
        if only_backpressure {
            let Some(error @ DiskioError::Backpressure(_)) = last_error.as_ref() else {
                return Err(DiskioError::AmbiguousWrite("completion was not observed".into()));
            };
            return Err(error.clone());
        }
        Err(DiskioError::AmbiguousWrite(last_error.map_or_else(
            || "completion was not observed".into(),
            |error| error.to_string(),
        )))
    }

    /// Flush all prior writes to one disk on the selected lane.
    ///
    /// # Errors
    ///
    /// Returns a typed topology, transport, durability, protocol, or disk
    /// error.
    pub async fn fsync(&self, disk_id: DiskId, options: OperationOptions) -> DiskioResult<()> {
        self.fsync_inner(disk_id, options, false).await
    }

    /// Return an operation policy using the configured default deadline.
    #[must_use]
    pub fn normal_options(&self) -> OperationOptions {
        OperationOptions::within(self.config.default_timeout)
    }

    /// Snapshot route and connection state without locking the request path.
    #[must_use]
    pub fn status(&self) -> DiskioStatus {
        let routes = self.routes.load();
        DiskioStatus {
            route_generation: routes.generation,
            route_age_ms: u64::try_from(routes.published_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            disks: routes.routes.len(),
            nodes: routes.nodes,
            endpoints: routes.endpoints,
            normal_connections: self.normal.connection_count(),
            normal_healthy_connections: self.normal.healthy_count(),
            priority_connections: self.priority.connection_count(),
            priority_healthy_connections: self.priority.healthy_count(),
            inflight: self.counters.inflight.load(Ordering::Relaxed),
            queued: 0,
            retries: self.counters.retries.load(Ordering::Relaxed),
            queue_rejections: self.counters.queue_rejections.load(Ordering::Relaxed),
            ambiguous_writes: self.counters.ambiguous_writes.load(Ordering::Relaxed),
            connect_attempts: self.counters.connect_attempts.load(Ordering::Relaxed),
            reconnect_attempts: self.counters.reconnect_attempts.load(Ordering::Relaxed),
            read_operations: self.counters.read_operations.load(Ordering::Relaxed),
            write_operations: self.counters.write_operations.load(Ordering::Relaxed),
            fsync_operations: self.counters.fsync_operations.load(Ordering::Relaxed),
            read_average_us: average(&self.counters.read_latency_us, &self.counters.read_operations),
            write_average_us: average(&self.counters.write_latency_us, &self.counters.write_operations),
            fsync_average_us: average(&self.counters.fsync_latency_us, &self.counters.fsync_operations),
        }
    }

    /// Retain one priority-lane route per disk for the native tree page store.
    ///
    /// # Errors
    ///
    /// Returns [`DiskioError::TransportUnavailable`] if a published endpoint
    /// has no healthy priority connection.
    pub fn native_routes(&self) -> DiskioResult<NativeDiskIoRoutes> {
        let snapshot = self.routes.load();
        let mut routes = Vec::with_capacity(snapshot.routes.len());
        for (disk_id, route) in &snapshot.routes {
            let selected = self.priority.get_first(&route.pool_key).ok_or_else(|| {
                DiskioError::TransportUnavailable(format!(
                    "DiskIO endpoint {} has no healthy priority connection",
                    route.endpoint
                ))
            })?;
            routes.push((
                *disk_id,
                OwnedClientRoute::new(
                    Arc::clone(&self.wire),
                    Arc::clone(&self.server),
                    selected.into_connection(),
                ),
            ));
        }
        Ok(NativeDiskIoRoutes { routes })
    }

    fn begin_operation(&self, kind: OperationKind, lane: TrafficLane) -> DiskioResult<InflightGuard<'_>> {
        let pending_limit = u64::try_from(self.config.max_pending_calls).unwrap_or(u64::MAX);
        let lane_inflight = match lane {
            TrafficLane::Normal => &self.counters.normal_inflight,
            TrafficLane::Priority => &self.counters.priority_inflight,
        };
        let admitted = lane_inflight.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < pending_limit).then_some(current + 1)
        });
        if admitted.is_err() {
            self.counters.queue_rejections.fetch_add(1, Ordering::Relaxed);
            return Err(DiskioError::Backpressure(format!(
                "semantic pending-call limit {} reached",
                self.config.max_pending_calls
            )));
        }
        self.counters.inflight.fetch_add(1, Ordering::Relaxed);
        Ok(InflightGuard {
            counters: &self.counters,
            kind,
            started: Instant::now(),
            counted_inflight: true,
            lane_inflight: Some(lane_inflight),
        })
    }

    fn begin_nested_operation(&self, kind: OperationKind) -> InflightGuard<'_> {
        InflightGuard {
            counters: &self.counters,
            kind,
            started: Instant::now(),
            counted_inflight: false,
            lane_inflight: None,
        }
    }

    fn select(&self, disk_id: DiskId, lane: TrafficLane) -> DiskioResult<(DiskRoute, SelectedConnection)> {
        let route = self.routes.load().routes.get(&disk_id).cloned().ok_or_else(|| {
            DiskioError::TopologyUnavailable(format!(
                "disk {}:{} has no published route",
                disk_id.high, disk_id.low
            ))
        })?;
        let pool = self.pool(lane);
        let selected = pool.get(&route.pool_key).map_or_else(
            || self.acquire_from(pool, &route.pool_key, &route.endpoint, true),
            Ok,
        )?;
        Ok((route, selected))
    }

    fn acquire_from(
        &self,
        pool: &ConnectionPoolIndex,
        pool_key: &str,
        endpoint: &str,
        reconnect: bool,
    ) -> DiskioResult<SelectedConnection> {
        let (host, port) = topology::parse_endpoint(endpoint)?;
        let counter = if reconnect {
            &self.counters.reconnect_attempts
        } else {
            &self.counters.connect_attempts
        };
        pool.get_or_try_install(pool_key, || {
            counter.fetch_add(1, Ordering::Relaxed);
            let connection = self.server.connect(host, port)?;
            self.wire.attach(&connection);
            Ok::<_, RpcError>(connection)
        })
        .map_err(classify_pool_error)
    }

    fn pool(&self, lane: TrafficLane) -> &ConnectionPoolIndex {
        match lane {
            TrafficLane::Normal => &self.normal,
            TrafficLane::Priority => &self.priority,
        }
    }

    async fn prepare_retry(
        &self,
        route: &DiskRoute,
        lane: TrafficLane,
        selected: &SelectedConnection,
        backoff: &mut Duration,
        deadline: Instant,
    ) -> DiskioResult<()> {
        self.counters.retries.fetch_add(1, Ordering::Relaxed);
        let pool = self.pool(lane);
        let route_is_current = self.routes.load().groups.contains_key(&route.pool_key);
        if route_is_current && !selected.is_open() {
            let (host, port) = topology::parse_endpoint(&route.endpoint)?;
            pool.replace_if_degraded(&route.pool_key, selected.generation(), || {
                self.counters.reconnect_attempts.fetch_add(1, Ordering::Relaxed);
                let connection = self.server.connect(host, port)?;
                self.wire.attach(&connection);
                Ok::<_, RpcError>(connection)
            })
            .map_err(classify_pool_error)?;
        }
        if Instant::now() >= deadline {
            return Err(DiskioError::DeadlineExceeded);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep((*backoff).min(remaining)).await;
        *backoff = (*backoff * 2).min(self.config.reconnect_max_backoff);
        Ok(())
    }

    async fn fsync_inner(
        &self,
        disk_id: DiskId,
        options: OperationOptions,
        durability_context: bool,
    ) -> DiskioResult<()> {
        let _inflight = if durability_context {
            self.begin_nested_operation(OperationKind::Fsync)
        } else {
            self.begin_operation(OperationKind::Fsync, options.lane)?
        };
        let mut backoff = self.config.reconnect_initial_backoff;
        let mut last_error = None;
        for attempt in 0..self.config.retry_attempts {
            let (route, selected) = self.select(disk_id, options.lane)?;
            let future = match self.wire.fsync(&self.server, &selected, disk_id) {
                Ok(future) => future,
                Err(error) => {
                    last_error = Some(self.classify_wire(error, OperationKind::Fsync));
                    if attempt + 1 < self.config.retry_attempts {
                        if self
                            .prepare_retry(&route, options.lane, &selected, &mut backoff, options.deadline)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    break;
                }
            };
            match self
                .await_write(future, options.deadline, OperationKind::Fsync)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error @ DiskioError::DiskFailure(_)) => {
                    return if durability_context {
                        Err(DiskioError::DurabilityFailure(error.to_string()))
                    } else {
                        Err(error)
                    };
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < self.config.retry_attempts
                        && self
                            .prepare_retry(&route, options.lane, &selected, &mut backoff, options.deadline)
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
            }
        }
        let message = last_error.map_or_else(
            || "fsync completion was not observed".into(),
            |error| error.to_string(),
        );
        if durability_context {
            Err(DiskioError::DurabilityFailure(message))
        } else {
            Err(DiskioError::TransportUnavailable(message))
        }
    }

    async fn await_write(
        &self,
        future: crowdb_rpc_ffi::CallFuture,
        deadline: Instant,
        operation: OperationKind,
    ) -> DiskioResult<()> {
        if Instant::now() >= deadline {
            return Err(DiskioError::DeadlineExceeded);
        }
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            WireClient::await_write_response(future),
        )
        .await
        .map_err(|_| DiskioError::DeadlineExceeded)?;
        result
            .map(|_| ())
            .map_err(|error| self.classify_wire(error, operation))
    }

    async fn await_read(
        &self,
        future: crowdb_rpc_ffi::CallFuture,
        expected: u32,
        deadline: Instant,
    ) -> DiskioResult<Bytes> {
        if Instant::now() >= deadline {
            return Err(DiskioError::DeadlineExceeded);
        }
        let (code, data) = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            WireClient::await_read_response(future),
        )
        .await
        .map_err(|_| DiskioError::DeadlineExceeded)?
        .map_err(|error| self.classify_wire(error, OperationKind::Read))?;
        if code != DiskIoRetCode::Success {
            return Err(DiskioError::Protocol(format!("unexpected read result {code:?}")));
        }
        let data = data.ok_or_else(|| DiskioError::Protocol("successful read omitted data".into()))?;
        if data.len() != expected as usize {
            return Err(DiskioError::Protocol(format!(
                "read returned {} bytes, expected {expected}",
                data.len()
            )));
        }
        Ok(Bytes::from(data))
    }

    fn classify_wire(&self, error: WireError, operation: OperationKind) -> DiskioError {
        match error {
            WireError::Rpc(RpcError::SendQueueFull) => {
                self.counters.queue_rejections.fetch_add(1, Ordering::Relaxed);
                DiskioError::Backpressure("RPC send queue is full".into())
            }
            WireError::Rpc(error) if error.is_retryable() => {
                DiskioError::TransportUnavailable(error.to_string())
            }
            WireError::Rpc(error) => DiskioError::Protocol(error.to_string()),
            WireError::Protocol(message) => DiskioError::Protocol(message),
            WireError::IoError(DiskIoRetCode::PartialWrite) => DiskioError::PartialWrite,
            WireError::IoError(DiskIoRetCode::InvalidAlignment) => {
                DiskioError::InvalidInput("DiskIO rejected address alignment".into())
            }
            WireError::IoError(DiskIoRetCode::ConnectionError) => {
                DiskioError::TransportUnavailable("DiskIO connection error".into())
            }
            WireError::IoError(code) => {
                DiskioError::DiskFailure(format!("{} returned {code:?}", operation.label()))
            }
        }
    }

    fn retire_candidate_groups(&self, retired: &RouteSnapshot, current: &RouteSnapshot) {
        for (key, group) in &retired.groups {
            if !current.groups.contains_key(key) {
                self.normal.invalidate(key, group.normal_generation);
                self.priority.invalidate(key, group.priority_generation);
            }
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn classify_pool_error(error: ConnectionPoolError<RpcError>) -> DiskioError {
    match error {
        ConnectionPoolError::Connect(error) => DiskioError::TransportUnavailable(error.to_string()),
        ConnectionPoolError::Capacity { max_endpoints } => DiskioError::TopologyInconsistent(format!(
            "DiskIO endpoint count exceeds configured maximum {max_endpoints}"
        )),
    }
}

fn average(total: &AtomicU64, count: &AtomicU64) -> u64 {
    let count = count.load(Ordering::Relaxed);
    total
        .load(Ordering::Relaxed)
        .checked_div(count)
        .unwrap_or_default()
}
