// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{service, Result};
use crate::{DeploymentProfile, ProbeKind, ServiceProfile};
use crowdb_protocol::mgmt::node::NodeServiceIntent;

pub(super) fn for_intent(profile: &DeploymentProfile, intent: &NodeServiceIntent) -> Result<ServiceProfile> {
    let mut service = service(profile, &intent.kind)?;
    let config: toml::Value = toml::from_str(&intent.configuration)?;
    let address = match intent.kind.as_str() {
        "diskio" => format!(
            "{}:{}",
            field(&config, &["server", "bind_address"])?
                .as_str()
                .ok_or("missing bind address")?,
            field(&config, &["server", "listen_port"])?
                .as_integer()
                .ok_or("missing port")?
        ),
        "diskdb" | "chunkdb" => field(&config, &["server", "http_listen_addr"])?
            .as_str()
            .ok_or("missing health address")?
            .to_owned(),
        "chunk-kv" => field(&config, &["http_listen_addr"])?
            .as_str()
            .ok_or("missing health address")?
            .to_owned(),
        "access" => field(&config, &["health", "listen"])?
            .as_str()
            .ok_or("missing health address")?
            .to_owned(),
        _ => return Err("unsupported service probe".into()),
    };
    service.probe.target = if intent.kind == "diskio" {
        address
    } else {
        format!(
            "http://{address}{}",
            match intent.kind.as_str() {
                "access" => "/_crowdb/health/ready",
                "chunkdb" => "/ready",
                _ => "/health",
            }
        )
    };
    service.probe.kind = if intent.kind == "diskio" {
        ProbeKind::RpcPing
    } else {
        ProbeKind::Http
    };
    service.additional_probes.clear();
    Ok(service)
}

fn field<'a>(mut value: &'a toml::Value, path: &[&str]) -> Result<&'a toml::Value> {
    for key in path {
        value = value.get(*key).ok_or("missing service probe setting")?;
    }
    Ok(value)
}
