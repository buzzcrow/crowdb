// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Rebind node-owned internal listeners without changing public client URIs.

use crate::error::{Error, Result};
use std::net::{IpAddr, SocketAddr};

/// # Errors
/// Rejects malformed retained service configuration and invalid node addresses.
pub fn rebind_service(configuration: &str, old: IpAddr, current: IpAddr, seeds: &[String]) -> Result<String> {
    let mut config: toml::Value = toml::from_str(configuration).map_err(|e| Error::Config(e.to_string()))?;
    for path in [
        "server.listen_addr",
        "server.http_listen_addr",
        "server.rpc_listen_addr",
        "server.bind_address",
        "http_listen_addr",
        "rpc_listen_addr",
        "http_advertise_addr",
        "rpc_advertise_addr",
        "dataset.listen",
        "iceberg.listen",
        "s3.listen",
        "health.listen",
    ] {
        let mut keys = path.split('.');
        let Some(first) = keys.next() else {
            continue;
        };
        let value = if let Some(second) = keys.next() {
            config.get_mut(first).and_then(|table| table.get_mut(second))
        } else {
            config.get_mut(first)
        };
        if let Some(value) = value {
            if value.as_str().and_then(|text| text.parse::<IpAddr>().ok()) == Some(old) {
                *value = toml::Value::String(current.to_string());
                continue;
            }
            if let Some(address) = value.as_str().and_then(|text| text.parse::<SocketAddr>().ok()) {
                if address.ip() == old {
                    *value = toml::Value::String(SocketAddr::new(current, address.port()).to_string());
                }
            }
        }
    }
    if !seeds.is_empty() {
        for path in [
            "server.kv_server_mgmt_seeds",
            "group0.kv_seeds",
            "group0_mgmt_seeds",
            "common.management_seeds",
        ] {
            let mut keys = path.split('.');
            let first = keys.next().unwrap_or_default();
            let value = if let Some(second) = keys.next() {
                config.get_mut(first).and_then(|table| table.get_mut(second))
            } else {
                config.get_mut(first)
            };
            if let Some(value) = value.filter(|value| value.is_array()) {
                *value = toml::Value::Array(seeds.iter().cloned().map(toml::Value::String).collect());
            }
        }
    }
    toml::to_string(&config).map_err(|e| Error::Config(e.to_string()))
}
