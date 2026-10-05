// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{operation::Operation, Failure};
use crate::{
    error::{err_400, err_409},
    state::AppState,
};
use axum::{extract::State, Json};
use crowdb_console_shared::{config::ServiceType, ConsoleConfig};
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
        ports.extend(server.rpc_port);
        if server.service_type == ServiceType::Diskdb {
            if let Some(rpc) = server.rpc_url.as_deref().and_then(port) {
                ports.extend([rpc.saturating_sub(2), rpc.saturating_sub(1)]);
            }
        }
    }
    for launch in config.local_launches.values() {
        for name in ["CROWDB_S3_PUBLIC_URI", "CROWDB_ICEBERG_PUBLIC_URI"] {
            ports.extend(launch.env.get(name).and_then(|value| port(value)));
        }
        ports.extend(launch.readiness_url.as_deref().and_then(port));
    }
    ports
}

fn available(port: u16) -> bool {
    std::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port)).is_ok()
}

fn next(used: &mut HashSet<u16>, start: u16, width: u16) -> Result<u16, Failure> {
    for candidate in start..=32767 - width {
        if (candidate..candidate + width).all(|port| !used.contains(&port) && available(port)) {
            used.extend(candidate..candidate + width);
            return Ok(candidate);
        }
    }
    Err(err_409(
        "No free listener range is available; release a port before deploying",
    ))
}

pub(super) async fn get(State(state): State<AppState>) -> Result<Json<Value>, Failure> {
    let config = state.config.read().unwrap().clone();
    let mut used = occupied(&config);
    used.extend(super::plans::reserved_ports(&state)?);
    let mut result = BTreeMap::new();
    for (kind, base) in [
        ("paxos-kv", 19910),
        ("diskdb", 29920),
        ("chunkdb", 12010),
        ("diskio", 13010),
        ("chunk-kv", 15010),
        ("access-server", 9091),
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
        let first = next(&mut used, base, if kind == "diskdb" { 3 } else { 1 })?;
        let mut value = json!({"instance_id":instance.to_string()});
        match kind {
            "diskdb" | "diskio" => value["rpc_port"] = json!(first),
            "access-server" => {
                value["s3_port"] = json!(first);
                value["http_port"] = json!(next(&mut used, base + 1, 1)?);
                value["health_port"] = json!(next(&mut used, base + 2, 1)?);
            }
            _ => {
                value["http_port"] = json!(first);
                value["rpc_port"] = json!(next(&mut used, base + 100, 1)?);
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
