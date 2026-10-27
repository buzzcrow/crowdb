// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Access uses its dedicated readiness listener, including remote deployments.

use crowdb_console_shared::{cluster::NodeHealth, config::ServerEntry, ConsoleConfig};
use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};

pub(super) async fn observe(
    config: &ConsoleConfig,
    service: &ServerEntry,
    remote: bool,
    intents: &[NodeServiceIntent],
) -> NodeHealth {
    let url = if remote {
        let Some(intent) = intents.iter().find(|intent| {
            intent.kind == "access"
                && intent.service_id == service.id
                && Some(intent.node_id) == service.node_id
        }) else {
            return NodeHealth::Unknown;
        };
        if matches!(intent.action, NodeServiceAction::Stop | NodeServiceAction::Delete) {
            return NodeHealth::Down;
        }
        config
            .node(intent.node_id)
            .and_then(|node| remote_url(&intent.configuration, &node.host))
    } else {
        config
            .local_launches
            .get(&service.id)
            .and_then(crowdb_console_shared::config::LocalLaunchSpec::access_health_url)
    };
    probe(url.as_deref()).await
}

fn remote_url(configuration: &str, host: &str) -> Option<String> {
    let config: toml::Value = toml::from_str(configuration).ok()?;
    let mut address: std::net::SocketAddr = config.get("health")?.get("listen")?.as_str()?.parse().ok()?;
    let owner: std::net::IpAddr = host.parse().ok()?;
    if address.port() == 0 || (!address.ip().is_unspecified() && address.ip() != owner) {
        return None;
    }
    for protocol in ["s3", "iceberg", "dataset"] {
        if config
            .get(protocol)
            .and_then(|value| value.get("listen"))
            .and_then(toml::Value::as_str)
            .and_then(|value| value.parse::<std::net::SocketAddr>().ok())
            .is_some_and(|listener| listener.port() == address.port())
        {
            return None;
        }
    }
    address.set_ip(owner);
    Some(format!("http://{address}/_crowdb/health/ready"))
}

async fn probe(url: Option<&str>) -> NodeHealth {
    let Some(url) = url else {
        return NodeHealth::Unknown;
    };
    let Ok(client) = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(1))
        .build()
    else {
        return NodeHealth::Unknown;
    };
    if client
        .get(url)
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
    {
        NodeHealth::Up
    } else {
        NodeHealth::Down
    }
}

/// Exercise remote URL validation and the same readiness probe as the server inventory.
#[cfg(feature = "test-util")]
pub async fn remote_access_health_for_tests(configuration: &str, host: &str) -> NodeHealth {
    probe(remote_url(configuration, host).as_deref()).await
}
