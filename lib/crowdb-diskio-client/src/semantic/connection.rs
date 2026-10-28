// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Runtime connection setup for complete or disk-scoped discovery.

use super::{
    Counters, DiskioClient, DiskioClientConfig, DiskioError, DiskioResult, RouteSnapshot, WireClient,
};
use arc_swap::ArcSwap;
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient, ServiceRegistryClient};
use crowdb_rpc_ffi::{ConnectionPoolIndex, RdmaWireLayout, RpcServer};
use std::sync::{atomic::AtomicU64, Arc};

impl DiskioClient {
    pub(super) fn create_rpc_server(config: &DiskioClientConfig) -> DiskioResult<Arc<RpcServer>> {
        let server = Arc::new(
            RpcServer::with_rdma_layout(None, 1, config.rpc_workers, RdmaWireLayout::ControlData, 1 << 20)
                .map_err(|error| DiskioError::TransportUnavailable(format!("create RPC client: {error}")))?,
        );
        if let Some(rdma) = &config.rdma {
            server.configure_rdma(rdma).map_err(|error| {
                DiskioError::TransportUnavailable(format!("configure DiskIO RPC client: {error}"))
            })?;
        }
        Ok(server)
    }

    #[cfg(feature = "test-util")]
    /// # Errors
    /// Uses production scoped discovery against injected real Group-0 clients.
    pub async fn connect_disk_with_clients_for_tests(
        service: ServiceRegistryClient,
        hardware: HardwareClient,
        config: DiskioClientConfig,
        disk: crowdb_protocol::key::DiskKey,
    ) -> DiskioResult<Self> {
        Self::connect_scoped_clients(service, hardware, config, Some(disk)).await
    }
    /// Connect using only group-0 management seeds.
    ///
    /// # Errors
    ///
    /// Returns an input, control-plane, topology, or connection error.
    pub async fn connect(config: DiskioClientConfig) -> DiskioResult<Self> {
        Self::connect_scope(config, None).await
    }

    /// Connect a bounded administrative probe to one authoritative disk.
    ///
    /// # Errors
    /// Rejects missing placement, missing or conflicting owners and transport errors.
    pub async fn connect_disk(
        config: DiskioClientConfig,
        disk: crowdb_protocol::key::DiskKey,
    ) -> DiskioResult<Self> {
        Self::connect_scope(config, Some(disk)).await
    }

    async fn connect_scope(
        config: DiskioClientConfig,
        scope: Option<crowdb_protocol::key::DiskKey>,
    ) -> DiskioResult<Self> {
        config.validate(true)?;
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
            config.management_seeds.clone(),
        )));
        kv.refresh_topology()
            .await
            .map_err(|error| DiskioError::TopologyUnavailable(error.to_string()))?;
        Self::connect_scoped_clients(
            ServiceRegistryClient::from_shared(Arc::clone(&kv)),
            HardwareClient::from_shared(kv),
            config,
            scope,
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
        Self::connect_scoped_clients(service, hardware, config, None).await
    }

    async fn connect_scoped_clients(
        service: ServiceRegistryClient,
        hardware: HardwareClient,
        config: DiskioClientConfig,
        scope: Option<crowdb_protocol::key::DiskKey>,
    ) -> DiskioResult<Self> {
        config.validate(false)?;
        let server = Self::create_rpc_server(&config)?;
        server.set_send_queue_capacity(config.send_queue_capacity);
        server
            .listen("127.0.0.1", 0)
            .map_err(|error| DiskioError::TransportUnavailable(format!("start RPC client: {error}")))?;
        server.start();
        let client = Self {
            closed: std::sync::atomic::AtomicBool::new(false),
            closing: tokio::sync::Notify::new(),
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
            last_topology_refresh_ms: AtomicU64::new(0),
            counters: Counters::default(),
            service: Some(service),
            hardware: Some(hardware),
            scope,
            config,
        };
        client.refresh().await?;
        Ok(client)
    }
}
