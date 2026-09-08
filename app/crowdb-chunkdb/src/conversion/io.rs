// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free DiskIO routing for background conversion.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskIoRetCode, DiskioClient};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::{Connection, RpcServer};

#[derive(Debug, thiserror::Error)]
pub enum ConversionIoError {
    #[error("conversion DiskIO topology error: {0}")]
    Topology(String),
    #[error("conversion DiskIO operation failed: {0}")]
    Io(String),
}

#[derive(Clone)]
struct Route {
    endpoint: Arc<str>,
    connection: Connection,
}

pub struct ConversionDiskIo {
    client: Arc<DiskioClient>,
    server: Arc<RpcServer>,
    routes: ArcSwap<HashMap<DiskId, Route>>,
}

impl ConversionDiskIo {
    pub async fn connect(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
    ) -> Result<Self, ConversionIoError> {
        let server = Arc::new(RpcServer::new(None));
        server
            .listen("127.0.0.1", 0)
            .map_err(|error| ConversionIoError::Topology(format!("start RPC client: {error}")))?;
        server.start();
        let client = Arc::new(DiskioClient::new());
        let routes = discover(service, hardware, &server, &client).await?;
        Ok(Self {
            client,
            server,
            routes: ArcSwap::from_pointee(routes),
        })
    }

    pub async fn refresh(
        &self,
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
    ) -> Result<(), ConversionIoError> {
        let routes = discover(service, hardware, &self.server, &self.client).await?;
        self.routes.store(Arc::new(routes));
        Ok(())
    }

    pub async fn read_segment(&self, segment: &Segment, unit_bytes: u64) -> Result<Bytes, ConversionIoError> {
        let (id, route) = self.route(segment)?;
        let size = u64::from(segment.unit_count)
            .checked_mul(unit_bytes)
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or_else(|| ConversionIoError::Io("segment read size overflows u32".into()))?;
        let future = self
            .client
            .read(
                &self.server,
                &route.connection,
                id,
                segment.zone_index,
                segment.unit_offset.saturating_mul(unit_bytes),
                size,
                0,
            )
            .map_err(|error| ConversionIoError::Io(format!("{}: {error}", route.endpoint)))?;
        let (code, data) = DiskioClient::await_read_response(future)
            .await
            .map_err(|error| ConversionIoError::Io(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(ConversionIoError::Io(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        data.map(Bytes::from)
            .ok_or_else(|| ConversionIoError::Io("successful read returned no payload".into()))
    }

    pub async fn write_segment(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        data: Bytes,
    ) -> Result<(), ConversionIoError> {
        let (id, route) = self.route(segment)?;
        let future = self
            .client
            .write_bytes(
                &self.server,
                &route.connection,
                id,
                segment.zone_index,
                segment.unit_offset.saturating_mul(unit_bytes),
                data,
            )
            .map_err(|error| ConversionIoError::Io(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_write_response(future)
            .await
            .map_err(|error| ConversionIoError::Io(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(ConversionIoError::Io(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }

    pub async fn fsync_segment(&self, segment: &Segment) -> Result<(), ConversionIoError> {
        let (id, route) = self.route(segment)?;
        let future = self
            .client
            .fsync(&self.server, &route.connection, id)
            .map_err(|error| ConversionIoError::Io(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_fsync_response(future)
            .await
            .map_err(|error| ConversionIoError::Io(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(ConversionIoError::Io(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }

    fn route(&self, segment: &Segment) -> Result<(DiskId, Route), ConversionIoError> {
        let id = segment
            .disk_id
            .map(|id| DiskId::new(id.high, id.low))
            .ok_or_else(|| ConversionIoError::Topology("segment has no disk id".into()))?;
        let route = self.routes.load().get(&id).cloned().ok_or_else(|| {
            ConversionIoError::Topology(format!("disk {}:{} has no route", id.high, id.low))
        })?;
        Ok((id, route))
    }
}

async fn discover(
    service: &ServiceRegistryClient,
    hardware: &HardwareClient,
    server: &RpcServer,
    client: &DiskioClient,
) -> Result<HashMap<DiskId, Route>, ConversionIoError> {
    let instances = service
        .read_all_diskio_instances()
        .await
        .map_err(|error| ConversionIoError::Topology(error.to_string()))?;
    let mut owners = HashMap::<u64, String>::new();
    for (_, instance) in instances {
        let extra = instance.extra.and_then(|extra| extra.diskdb).ok_or_else(|| {
            ConversionIoError::Topology(format!(
                "DiskIO {} has no ownership metadata",
                instance.rpc_endpoint
            ))
        })?;
        for disk_group_id in extra.owned_dg_ids {
            if owners
                .insert(disk_group_id, instance.rpc_endpoint.clone())
                .is_some()
            {
                return Err(ConversionIoError::Topology(format!(
                    "disk group {disk_group_id} has duplicate DiskIO owners"
                )));
            }
        }
    }
    let disks = hardware
        .list_all_disks()
        .await
        .map_err(|error| ConversionIoError::Topology(error.to_string()))?;
    let mut connections = HashMap::<String, Connection>::new();
    let mut routes = HashMap::with_capacity(disks.len());
    for disk in disks {
        let endpoint = owners.get(&disk.disk_group_id).ok_or_else(|| {
            ConversionIoError::Topology(format!(
                "disk group {} has no live DiskIO owner",
                disk.disk_group_id
            ))
        })?;
        if !connections.contains_key(endpoint) {
            let (host, port) = parse_endpoint(endpoint)?;
            let connection = server
                .connect(host, port)
                .map_err(|error| ConversionIoError::Topology(format!("connect {endpoint}: {error}")))?;
            client.attach(&connection);
            connections.insert(endpoint.clone(), connection);
        }
        routes.insert(
            DiskId::new(disk.disk_id.high, disk.disk_id.low),
            Route {
                endpoint: Arc::from(endpoint.as_str()),
                connection: connections[endpoint].clone(),
            },
        );
    }
    Ok(routes)
}

fn parse_endpoint(endpoint: &str) -> Result<(&str, i32), ConversionIoError> {
    let endpoint = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| ConversionIoError::Topology(format!("invalid DiskIO endpoint {endpoint}")))?;
    let port = port
        .parse::<i32>()
        .map_err(|error| ConversionIoError::Topology(format!("invalid DiskIO endpoint: {error}")))?;
    Ok((host, port))
}
