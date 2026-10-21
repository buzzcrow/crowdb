// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated, non-mutating observations on each required protocol listener.

use crate::state::AppState;
use crowdb_protocol::mgmt::{
    cluster::{DependencyKind, DependencyObservation, DependencyState, ReadinessReason},
    node::{
        observation::NodeRuntimeObservation, setup::SetupServiceKind, NodeServiceAction, NodeServiceIntent,
    },
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DATASET_PROBE: &str = "crowdb_readiness_probe";

pub(super) async fn observe(
    state: &AppState,
    node: &NodeRuntimeObservation,
    intents: Option<&[NodeServiceIntent]>,
) -> Vec<DependencyObservation> {
    let Some(binding) = &node.binding else {
        return Vec::new();
    };
    if !node
        .policy
        .setup
        .services
        .iter()
        .any(|service| service.kind == SetupServiceKind::AccessServer && service.enabled)
    {
        return Vec::new();
    }
    let id = format!("access-{}", binding.node_id);
    let current = intents.and_then(|intents| {
        intents.iter().find(|intent| {
            intent.service_id == id
                && intent.node_id == binding.node_id
                && intent.cluster_id == binding.bootstrap.cluster_id
        })
    });
    let runtime_ready = current.is_some_and(|current| {
        node.services.iter().any(|service| {
            service.service_id == current.service_id
                && service.operation_id.as_ref() == Some(&current.operation_id)
                && service.state == DependencyState::Ready
        })
    });
    futures::future::join_all(
        ["s3", "iceberg", "dataset"]
            .into_iter()
            .map(|protocol| async move {
                let (status, code) = if let Some(current) = current {
                    if matches!(
                        current.action,
                        NodeServiceAction::Stop | NodeServiceAction::Delete
                    ) {
                        (DependencyState::Stopped, "operator-stopped-service")
                    } else if runtime_ready {
                        probe(state, protocol, current).await
                    } else {
                        (DependencyState::Initializing, "access-service-not-ready")
                    }
                } else {
                    (DependencyState::Unknown, "access-intent-unavailable")
                };
                let dependency = format!("node/{}/protocol/{protocol}", binding.node_id);
                DependencyObservation {
                    id: dependency.clone(),
                    kind: DependencyKind::Protocol,
                    required: true,
                    state: status,
                    observed_at_unix_ms: now_ms(),
                    max_age_ms: 3000,
                    revision: None,
                    operation_id: current.map(|intent| intent.operation_id.clone()),
                    current_operation_id: current.map(|intent| intent.operation_id.clone()),
                    reason: (status != DependencyState::Ready).then(|| ReadinessReason {
                        dependency,
                        code: code.into(),
                        detail: "Required protocol has no current readiness proof".into(),
                    }),
                }
            }),
    )
    .await
}

async fn probe(
    state: &AppState,
    protocol: &str,
    intent: &NodeServiceIntent,
) -> (DependencyState, &'static str) {
    match request(state, protocol, intent).await {
        Some(true) => (DependencyState::Ready, "ready"),
        Some(false) => (DependencyState::Unknown, "protocol-not-ready"),
        None => (DependencyState::Unknown, "protocol-observation-unavailable"),
    }
}

async fn request(state: &AppState, protocol: &str, intent: &NodeServiceIntent) -> Option<bool> {
    let config: toml::Value = toml::from_str(&intent.configuration).ok()?;
    let address = config.get(protocol)?.get("listen")?.as_str()?;
    let origin = internal_origin(state, intent.node_id, address)?;
    let path = match protocol {
        "s3" => "/_crowdb/health/ready".into(),
        "iceberg" => "/v1/config".into(),
        "dataset" => format!("/api/dataset/v1/namespaces/dataset-ns/datasets/{DATASET_PROBE}"),
        _ => return None,
    };
    let token = if protocol == "s3" {
        None
    } else {
        let credentials =
            crowdb_monitor::ServerCredentials::load_existing(state.runtime_root.parent()?).ok()?;
        Some(if protocol == "iceberg" {
            credentials.iceberg_read_token().to_owned()
        } else {
            credentials.dataset_token()
        })
    };
    request_probe(protocol, &format!("{origin}{path}"), token.as_deref()).await
}

pub(super) async fn request_probe(protocol: &str, url: &str, token: Option<&str>) -> Option<bool> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(1))
        .build()
        .ok()?;
    let mut request = client.get(url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let mut response = request.send().await.ok()?;
    let status = response.status().as_u16();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if bytes.len() + chunk.len() > 65536 {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    let body: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    Some(match protocol {
        "s3" => status == 200 && body["ready"] == true,
        "iceberg" => status == 200 && body["defaults"].is_object() && body["overrides"].is_object(),
        "dataset" => {
            (status == 404 && body["code"] == "not_found")
                || (status == 200 && body["name"] == DATASET_PROBE && body["id"].is_string())
        }
        _ => false,
    })
}

fn internal_origin(state: &AppState, node: u64, address: &str) -> Option<String> {
    let url = reqwest::Url::parse(&format!("http://{address}")).ok()?;
    let host = url.host_str()?;
    let config = state.config.read().ok()?;
    let own_host = &config.node(node)?.host;
    if !["127.0.0.1", "localhost", "0.0.0.0", "[::]", "[::1]"].contains(&host) && host != own_host {
        return None;
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return None;
    }
    let host = match host {
        "0.0.0.0" => "127.0.0.1",
        "[::]" => "[::1]",
        host => host,
    };
    Some(format!("http://{host}:{}", url.port()?))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| time.as_millis().try_into().ok())
        .unwrap_or(0)
}
