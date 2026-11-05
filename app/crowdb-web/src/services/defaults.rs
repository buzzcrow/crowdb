// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{operation::Operation, Failure};
use crate::{
    error::{err_400, err_409},
    state::AppState,
};
use axum::{extract::State, Json};
use crowdb_console_shared::{config::ServiceType, ConsoleConfig};
use crowdb_protocol::ServicePort;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

fn port(origin: &str) -> Option<u16> {
    let origin = if origin.contains("://") {
        origin.to_owned()
    } else {
        format!("http://{origin}")
    };
    reqwest::Url::parse(&origin).ok()?.port_or_known_default()
}

fn occupied(config: &ConsoleConfig) -> HashSet<u16> {
    let mut ports = HashSet::new();
    for server in &config.servers {
        ports.extend(port(&server.url));
        ports.extend(server.rpc_url.as_deref().and_then(port));
        ports.extend(server.rest_port);
        ports.extend(server.access_origins.values().filter_map(|origin| port(origin)));
        ports.extend(server.rpc_port);
    }
    for launch in config.local_launches.values() {
        for name in [
            "CROWDB_S3_PUBLIC_URI",
            "CROWDB_ICEBERG_PUBLIC_URI",
            "CROWDB_DATASET_PUBLIC_URI",
        ] {
            ports.extend(launch.env.get(name).and_then(|value| port(value)));
        }
        ports.extend(launch.readiness_url.as_deref().and_then(port));
    }
    ports
}

fn available(port: u16) -> bool {
    std::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port)).is_ok()
}

fn next(used: &mut HashSet<u16>, service: ServicePort) -> Result<u16, Failure> {
    for instance in 0..service.range_size() {
        let candidate = service.port(instance);
        if !used.contains(&candidate) && available(candidate) {
            used.insert(candidate);
            return Ok(candidate);
        }
    }
    Err(err_409(
        "No free port remains in the service's default range; specify a free listener explicitly",
    ))
}

pub(super) async fn get(State(state): State<AppState>) -> Result<Json<Value>, Failure> {
    let config = state.config.read().unwrap().clone();
    let mut used = occupied(&config);
    used.extend(super::plans::reserved_ports(&state)?);
    let mut result = BTreeMap::new();
    for (kind, listeners) in [
        (
            "paxos-kv",
            vec![
                ("http_port", ServicePort::PaxosKvServerMgmt),
                ("rpc_port", ServicePort::PaxosKvServerRpc),
            ],
        ),
        (
            "diskdb",
            vec![
                ("http_port", ServicePort::DiskdbHttp),
                ("rpc_port", ServicePort::DiskdbRpc),
            ],
        ),
        (
            "chunkdb",
            vec![
                ("http_port", ServicePort::ChunkdbHttp),
                ("rpc_port", ServicePort::ChunkdbRpc),
            ],
        ),
        ("diskio", vec![("rpc_port", ServicePort::DiskioRpc)]),
        (
            "chunk-kv",
            vec![
                ("http_port", ServicePort::ChunkKvHttp),
                ("rpc_port", ServicePort::ChunkKvRpc),
            ],
        ),
        (
            "access-server",
            vec![
                ("s3_port", ServicePort::AccessServerHttp),
                ("http_port", ServicePort::AccessServerIcebergHttp),
                ("health_port", ServicePort::AccessServerHealthHttp),
                ("dataset_port", ServicePort::AccessServerDatasetHttp),
            ],
        ),
    ] {
        let instance = config
            .servers
            .iter()
            .filter_map(|server| server.id.strip_prefix(&format!("{kind}-"))?.parse::<u64>().ok())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .filter(|id| i64::try_from(*id).is_ok())
            .ok_or_else(|| err_409("Service instance ID space exhausted"))?;
        let mut value = json!({"instance_id":instance.to_string()});
        for (field, service) in listeners {
            value[field] = json!(next(&mut used, service)?);
        }
        if kind == "chunkdb" {
            if let Some(server) = config
                .servers
                .iter()
                .find(|server| server.service_type == ServiceType::Chunkdb)
            {
                value["dynamic_ownership"] = json!(config
                    .local_launches
                    .get(&server.id)
                    .and_then(|launch| launch.env.get("CROWDB_CHUNKDB_OWNERSHIP_POLICY"))
                    .is_some_and(|policy| policy == "dynamic"));
            }
        }
        result.insert(kind, value);
    }
    Ok(Json(json!(result)))
}

pub(crate) fn claim_ports(state: &AppState, ports: &[u16]) -> Result<Operation, Failure> {
    if ports.contains(&0) || ports.iter().collect::<HashSet<_>>().len() != ports.len() {
        return Err(err_400("Listeners must use distinct, nonzero ports"));
    }
    let operation = Operation::claim(
        state,
        ports.iter().map(|port| format!("listener/{port}")).collect(),
    )?;
    let used = occupied(&state.config.read().unwrap());
    if ports.iter().any(|port| used.contains(port) || !available(*port)) {
        return Err(err_409(
            "A listener port is already in use; reopen the dialog for fresh defaults",
        ));
    }
    Ok(operation)
}
