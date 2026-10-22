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

mod response;
use response::list_buckets_body;

const DATASET_PROBE: &str = "crowdb_readiness_probe";
type ProbeResult = Result<String, (&'static str, String)>;

pub(super) async fn observe(
    state: &AppState,
    node: &NodeRuntimeObservation,
    intents: Option<&[NodeServiceIntent]>,
    own_host: Option<&str>,
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
                let result = if let Some(current) = current {
                    if matches!(
                        current.action,
                        NodeServiceAction::Stop | NodeServiceAction::Delete
                    ) {
                        Err((
                            "operator-stopped-service",
                            "Access service is explicitly stopped or deleted".into(),
                        ))
                    } else if runtime_ready {
                        request(state, protocol, current, own_host).await
                    } else {
                        Err((
                            "access-service-not-ready",
                            format!(
                                "Access service {} has no ready observation for current operation {}",
                                current.service_id, current.operation_id
                            ),
                        ))
                    }
                } else {
                    Err((
                        "access-intent-unavailable",
                        "Current committed Access service intent is unavailable".into(),
                    ))
                };
                let status = match &result {
                    Ok(_) => DependencyState::Ready,
                    Err(("operator-stopped-service", _)) => DependencyState::Stopped,
                    Err(("access-service-not-ready", _)) => DependencyState::Initializing,
                    Err(_) => DependencyState::Unknown,
                };
                let detail = match &result {
                    Ok(detail) | Err((_, detail)) => detail.clone(),
                };
                let dependency = format!("node/{}/protocol/{protocol}", binding.node_id);
                DependencyObservation {
                    id: dependency.clone(),
                    kind: DependencyKind::Protocol,
                    required: true,
                    state: status,
                    detail: detail.clone(),
                    observed_at_unix_ms: now_ms(),
                    max_age_ms: 3000,
                    revision: None,
                    operation_id: current.map(|intent| intent.operation_id.clone()),
                    current_operation_id: current.map(|intent| intent.operation_id.clone()),
                    reason: result.err().map(|(code, _)| ReadinessReason {
                        dependency,
                        code: code.into(),
                        detail,
                    }),
                }
            }),
    )
    .await
}

async fn request(
    state: &AppState,
    protocol: &str,
    intent: &NodeServiceIntent,
    own_host: Option<&str>,
) -> ProbeResult {
    let invalid = || {
        (
            "protocol-configuration-invalid",
            format!("{protocol} has no valid local listener in the committed Access configuration"),
        )
    };
    let config: toml::Value = toml::from_str(&intent.configuration).map_err(|_| invalid())?;
    let address = config
        .get(protocol)
        .and_then(|value| value.get("listen"))
        .and_then(toml::Value::as_str)
        .ok_or_else(invalid)?;
    let origin = internal_origin(own_host, address).ok_or_else(invalid)?;
    if protocol == "s3" {
        return signed_s3_probe(state, &format!("{origin}/")).await;
    }
    let path = match protocol {
        "iceberg" => "/v1/config".into(),
        "dataset" => format!("/api/dataset/v1/namespaces/dataset-ns/datasets/{DATASET_PROBE}"),
        _ => return Err(invalid()),
    };
    let credentials =
        crowdb_monitor::ServerCredentials::load_existing(state.runtime_root.as_ref()).map_err(|_| {
            (
                "protocol-credentials-unavailable",
                format!("Retained {protocol} credentials are unavailable"),
            )
        })?;
    let token = if protocol == "iceberg" {
        credentials.iceberg_read_token().to_owned()
    } else {
        credentials.dataset_token()
    };
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {token}").parse().map_err(|_| {
            (
                "protocol-credentials-invalid",
                "Retained bearer credential is invalid".into(),
            )
        })?,
    );
    execute_probe(protocol, &format!("{origin}{path}"), headers).await
}

pub(super) async fn signed_s3_probe(state: &AppState, url: &str) -> ProbeResult {
    let parsed = reqwest::Url::parse(url).map_err(|_| {
        (
            "protocol-configuration-invalid",
            "Invalid local S3 listener".into(),
        )
    })?;
    let headers = crate::access::sign_local_s3_read(state, &parsed).map_err(|_| {
        (
            "protocol-credentials-unavailable",
            "Cannot sign S3 ListBuckets with retained local credentials".into(),
        )
    })?;
    execute_probe("s3", url, headers).await
}

#[cfg(feature = "test-util")]
pub(super) async fn request_probe(protocol: &str, url: &str, token: Option<&str>) -> Option<bool> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = token {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token}").parse().ok()?,
        );
    }
    match execute_probe(protocol, url, headers).await {
        Ok(_) => Some(true),
        Err(("protocol-http-status", _)) => Some(false),
        Err(_) => None,
    }
}

async fn execute_probe(protocol: &str, url: &str, headers: reqwest::header::HeaderMap) -> ProbeResult {
    let transport = || {
        (
            "protocol-transport-unavailable",
            format!("{protocol} read at {url} failed or exceeded its one-second deadline"),
        )
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(1))
        .build()
        .map_err(|_| transport())?;
    let mut response = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(|_| transport())?;
    let status = response.status().as_u16();
    if status != 200 && !(protocol == "dataset" && status == 404) {
        return Err((
            "protocol-http-status",
            format!(
                "{protocol} read at {url} returned HTTP {status}; expected {}",
                if protocol == "dataset" {
                    "200 or authenticated not-found 404"
                } else {
                    "200"
                }
            ),
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| transport())? {
        if bytes.len() + chunk.len() > 65536 {
            return Err((
                "protocol-response-too-large",
                format!("{protocol} response at {url} exceeds 65536 bytes"),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let invalid = || {
        (
            "protocol-response-invalid",
            format!("{protocol} read at {url} returned HTTP {status} with an unexpected protocol response"),
        )
    };
    if protocol == "s3" {
        return if list_buckets_body(&bytes) {
            Ok(format!(
                "S3 signed ListBuckets at {url}: HTTP 200, valid ListAllMyBucketsResult"
            ))
        } else {
            Err(invalid())
        };
    }
    let body: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    let valid = match protocol {
        "iceberg" => status == 200 && body["defaults"].is_object() && body["overrides"].is_object(),
        "dataset" => {
            (status == 404 && body["code"] == "not_found")
                || (status == 200 && body["name"] == DATASET_PROBE && body["id"].is_string())
        }
        _ => false,
    };
    if valid {
        Ok(format!(
            "{protocol} authenticated read at {url}: HTTP {status}, valid protocol response"
        ))
    } else {
        Err(invalid())
    }
}

pub(super) fn internal_origin(own_host: Option<&str>, address: &str) -> Option<String> {
    let url = reqwest::Url::parse(&format!("http://{address}")).ok()?;
    let host = url.host_str()?;
    let own_host = own_host?;
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
