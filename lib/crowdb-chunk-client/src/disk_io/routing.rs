// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free disk-ID routing for production DiskIO writes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskIoRetCode, DiskioClient, SegmentWriteTarget};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::{Connection, RpcServer};

use crate::{DiskWriter, IoError, Result};

#[derive(Clone)]
struct Route {
    endpoint: Arc<str>,
    connections: Arc<[Connection]>,
    priority_connection: Connection,
    next_connection: Arc<AtomicUsize>,
}

impl Route {
    fn connection(&self) -> Connection {
        let index = self.next_connection.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        self.connections[index].clone()
    }
}

/// Disk writer backed by an atomically published disk-owner snapshot.
pub struct RoutedDiskWriter {
    client: Arc<DiskioClient>,
    server: Arc<RpcServer>,
    routes: ArcSwap<HashMap<DiskId, Route>>,
    connections_per_endpoint: usize,
}

impl RoutedDiskWriter {
    /// Discover live DiskIO owners and connect to their endpoints.
    pub async fn connect(service: &ServiceRegistryClient, hardware: &HardwareClient) -> Result<Self> {
        Self::connect_with_connections(service, hardware, 1).await
    }

    /// Discover owners and keep a fixed lock-free connection pool per endpoint.
    pub async fn connect_with_connections(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
        connections_per_endpoint: usize,
    ) -> Result<Self> {
        Self::connect_with_connections_and_workers(service, hardware, connections_per_endpoint, 1).await
    }

    /// Discover owners with fixed connection pools and RPC I/O workers.
    pub async fn connect_with_connections_and_workers(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
        connections_per_endpoint: usize,
        rpc_workers: u32,
    ) -> Result<Self> {
        if connections_per_endpoint == 0 || rpc_workers == 0 {
            return Err(IoError::Topology(
                "DiskIO connections and RPC workers must be non-zero".into(),
            ));
        }
        let server = Arc::new(RpcServer::with_engines(None, 1, rpc_workers));
        server
            .listen("127.0.0.1", 0)
            .map_err(|error| IoError::Topology(format!("start RPC client: {error}")))?;
        server.start();
        let client = Arc::new(DiskioClient::new());
        let routes = Self::discover(service, hardware, &server, &client, connections_per_endpoint).await?;
        Ok(Self {
            client,
            server,
            routes: ArcSwap::from_pointee(routes),
            connections_per_endpoint,
        })
    }

    /// Refresh topology off the write path, then atomically publish it.
    pub async fn refresh(&self, service: &ServiceRegistryClient, hardware: &HardwareClient) -> Result<()> {
        let routes = Self::discover(
            service,
            hardware,
            &self.server,
            &self.client,
            self.connections_per_endpoint,
        )
        .await?;
        self.routes.store(Arc::new(routes));
        Ok(())
    }

    async fn discover(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
        server: &RpcServer,
        client: &DiskioClient,
        connections_per_endpoint: usize,
    ) -> Result<HashMap<DiskId, Route>> {
        let instances = service
            .read_all_diskio_instances()
            .await
            .map_err(|error| IoError::Topology(format!("read DiskIO instances: {error}")))?;
        let mut dg_owners = HashMap::<u64, String>::new();
        for (_, instance) in instances {
            let extra = instance.extra.and_then(|extra| extra.diskdb).ok_or_else(|| {
                IoError::Topology(format!(
                    "DiskIO {} has no ownership metadata",
                    instance.rpc_endpoint
                ))
            })?;
            for dg_id in extra.owned_dg_ids {
                if let Some(previous) = dg_owners.insert(dg_id, instance.rpc_endpoint.clone()) {
                    return Err(IoError::Topology(format!(
                        "disk group {dg_id} has duplicate owners {previous} and {}",
                        instance.rpc_endpoint
                    )));
                }
            }
        }

        let disks = hardware
            .list_all_disks()
            .await
            .map_err(|error| IoError::Topology(format!("read disks: {error}")))?;
        let mut endpoint_routes = HashMap::<String, Route>::new();
        let mut routes = HashMap::with_capacity(disks.len());
        for disk in disks {
            let endpoint = dg_owners.get(&disk.disk_group_id).ok_or_else(|| {
                IoError::Topology(format!(
                    "disk group {} has no live DiskIO owner",
                    disk.disk_group_id
                ))
            })?;
            if !endpoint_routes.contains_key(endpoint) {
                let (host, port) = parse_endpoint(endpoint)?;
                let mut connections = Vec::with_capacity(connections_per_endpoint);
                for _ in 0..connections_per_endpoint {
                    let connection = server
                        .connect(host, port)
                        .map_err(|error| IoError::Topology(format!("connect DiskIO {endpoint}: {error}")))?;
                    client.attach(&connection);
                    connections.push(connection);
                }
                let priority_connection = server.connect(host, port).map_err(|error| {
                    IoError::Topology(format!("connect priority DiskIO {endpoint}: {error}"))
                })?;
                client.attach(&priority_connection);
                endpoint_routes.insert(
                    endpoint.clone(),
                    Route {
                        endpoint: Arc::from(endpoint.as_str()),
                        connections: connections.into(),
                        priority_connection,
                        next_connection: Arc::new(AtomicUsize::new(0)),
                    },
                );
            }
            let id = DiskId::new(disk.disk_id.high, disk.disk_id.low);
            routes.insert(id, endpoint_routes[endpoint].clone());
        }
        Ok(routes)
    }

    fn route(&self, disk_id: DiskId) -> Result<Route> {
        self.routes
            .load()
            .get(&disk_id)
            .cloned()
            .ok_or_else(|| IoError::Topology(format!("disk {}:{} has no route", disk_id.high, disk_id.low)))
    }
}

fn parse_endpoint(endpoint: &str) -> Result<(&str, i32)> {
    let endpoint = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| IoError::Topology(format!("invalid DiskIO endpoint {endpoint}")))?;
    let port = port
        .parse::<i32>()
        .map_err(|error| IoError::Topology(format!("invalid DiskIO endpoint {endpoint}: {error}")))?;
    Ok((host, port))
}

#[async_trait]
impl DiskWriter for RoutedDiskWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        let id = seg
            .disk_id
            .map(|id| DiskId::new(id.high, id.low))
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let route = self.route(id)?;
        let connection = route.connection();
        let future = self
            .client
            .write_segment_bytes(
                &self.server,
                &connection,
                SegmentWriteTarget {
                    disk_id: id,
                    zone_index: seg.zone_index,
                    zone_offset: seg.unit_offset * unit_bytes,
                    allocation_ts: seg.allocation_ts,
                    allocation_zone_offset: seg.unit_offset * unit_bytes,
                },
                data,
            )
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_write_response(future)
            .await
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }

    async fn write_priority_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        super::disk_writer::validate_segment_byte_write(seg, unit_bytes, byte_offset, data.len())?;
        let id = seg
            .disk_id
            .map(|id| DiskId::new(id.high, id.low))
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let route = self.route(id)?;
        let zone_offset = seg
            .unit_offset
            .checked_mul(unit_bytes)
            .and_then(|offset| offset.checked_add(byte_offset))
            .ok_or_else(|| IoError::WriteFailed("disk write offset overflow".into()))?;
        let future = self
            .client
            .write_segment_bytes(
                &self.server,
                &route.priority_connection,
                SegmentWriteTarget {
                    disk_id: id,
                    zone_index: seg.zone_index,
                    zone_offset,
                    allocation_ts: seg.allocation_ts,
                    allocation_zone_offset: seg.unit_offset * unit_bytes,
                },
                data,
            )
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_write_response(future)
            .await
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }

    async fn write_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        super::disk_writer::validate_segment_byte_write(seg, unit_bytes, byte_offset, data.len())?;
        let id = seg
            .disk_id
            .map(|id| DiskId::new(id.high, id.low))
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let route = self.route(id)?;
        let connection = route.connection();
        let zone_offset = seg
            .unit_offset
            .checked_mul(unit_bytes)
            .and_then(|offset| offset.checked_add(byte_offset))
            .ok_or_else(|| IoError::WriteFailed("disk write offset overflow".into()))?;
        let future = self
            .client
            .write_segment_bytes(
                &self.server,
                &connection,
                SegmentWriteTarget {
                    disk_id: id,
                    zone_index: seg.zone_index,
                    zone_offset,
                    allocation_ts: seg.allocation_ts,
                    allocation_zone_offset: seg.unit_offset * unit_bytes,
                },
                data,
            )
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_write_response(future)
            .await
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }

    async fn fsync(&self, seg: &Segment) -> Result<()> {
        let id = seg
            .disk_id
            .map(|id| DiskId::new(id.high, id.low))
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let route = self.route(id)?;
        let connection = route.connection();
        let future = self
            .client
            .fsync(&self.server, &connection, id)
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_fsync_response(future)
            .await
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }

    async fn fsync_priority(&self, seg: &Segment) -> Result<()> {
        let id = seg
            .disk_id
            .map(|id| DiskId::new(id.high, id.low))
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let route = self.route(id)?;
        let future = self
            .client
            .fsync(&self.server, &route.priority_connection, id)
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_fsync_response(future)
            .await
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }

    async fn read(&self, seg: &Segment, unit_bytes: u64, segment_offset: u64, length: u32) -> Result<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let id = seg
            .disk_id
            .map(|id| DiskId::new(id.high, id.low))
            .ok_or_else(|| IoError::ReadFailed("segment missing disk_id".into()))?;
        let segment_bytes = u64::from(seg.unit_count)
            .checked_mul(unit_bytes)
            .ok_or_else(|| IoError::ReadFailed("segment byte capacity overflow".into()))?;
        let end = segment_offset
            .checked_add(u64::from(length))
            .ok_or_else(|| IoError::ReadFailed("segment-relative read end overflow".into()))?;
        if unit_bytes == 0 || end > segment_bytes {
            return Err(IoError::ReadFailed("disk read is outside its segment".into()));
        }
        let route = self.route(id)?;
        let connection = route.connection();
        let zone_offset = seg
            .unit_offset
            .checked_mul(unit_bytes)
            .and_then(|offset| offset.checked_add(segment_offset))
            .ok_or_else(|| IoError::ReadFailed("disk read offset overflow".into()))?;
        let future = self
            .client
            .read(
                &self.server,
                &connection,
                id,
                seg.zone_index,
                zone_offset,
                length,
                0,
            )
            .map_err(|error| IoError::TransientRead(format!("{}: {error}", route.endpoint)))?;
        let (code, data) = DiskioClient::await_read_response(future)
            .await
            .map_err(|error| IoError::TransientRead(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            let message = format!("{} returned {code:?}", route.endpoint);
            return Err(match code {
                DiskIoRetCode::DiskNotExist
                | DiskIoRetCode::ZoneNotExist
                | DiskIoRetCode::IoError
                | DiskIoRetCode::StaleAllocation => IoError::ReadFailed(message),
                DiskIoRetCode::Success => unreachable!(),
                DiskIoRetCode::PartialWrite
                | DiskIoRetCode::InvalidAlignment
                | DiskIoRetCode::ConnectionError => IoError::TransientRead(message),
            });
        }
        let data =
            data.ok_or_else(|| IoError::TransientRead(format!("{} omitted read data", route.endpoint)))?;
        if data.len() != length as usize {
            return Err(IoError::TransientRead(format!(
                "{} returned {} bytes, expected {length}",
                route.endpoint,
                data.len()
            )));
        }
        Ok(Bytes::from(data))
    }
}
