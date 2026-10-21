// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only target resolution, separate from runtime resource ownership.

use std::time::Duration;

use crowdb_protocol::mgmt::cluster::ClusterTargetIdentity;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSelection {
    pub runtime: String,
    pub endpoint: String,
    pub context: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOwnership {
    pub selection: RuntimeSelection,
    pub owner: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTargetBinding {
    pub version: u32,
    pub name: String,
    pub cluster_id: String,
    pub management_endpoints: Vec<String>,
    pub runtime: Option<RuntimeOwnership>,
}

#[derive(Debug, thiserror::Error)]
pub enum TargetError {
    #[error("select a cluster binding or at least one management endpoint")]
    Missing,
    #[error("invalid cluster binding or management endpoint")]
    Invalid,
    #[error("management endpoint is unavailable or has no confirmed cluster identity")]
    Unavailable,
    #[error("management endpoints and selected binding disagree on cluster identity")]
    IdentityConflict,
    #[error("selected runtime endpoint/context differs from the cluster binding")]
    RuntimeConflict,
    #[error(
        "native RPC endpoint is unreachable; publishing only HTTP ports does not expose native transport"
    )]
    RpcUnreachable,
}

#[derive(Clone, Debug)]
pub struct ResolvedClusterTarget {
    pub identity: ClusterTargetIdentity,
    pub management_endpoints: Vec<String>,
    pub runtime: Option<RuntimeOwnership>,
}

/// Observe every selected endpoint before returning a workload target.
///
/// # Errors
/// Rejects missing/malformed inputs, unreachable or mixed identities, stale
/// bindings and changed runtime selection. Never retries through another target.
pub async fn resolve(
    binding: Option<&ClusterTargetBinding>,
    explicit: &[String],
    runtime: Option<&RuntimeSelection>,
) -> Result<ResolvedClusterTarget, TargetError> {
    validate_binding(binding, runtime)?;
    let selected = if explicit.is_empty() {
        binding.map_or(&[][..], |binding| binding.management_endpoints.as_slice())
    } else {
        explicit
    };
    let management_endpoints = management_origins(selected)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|_| TargetError::Unavailable)?;
    // A single bounded observation pass; the slowest endpoint bounds the pass.
    let responses =
        futures::future::join_all(management_endpoints.iter().map(|origin| observe(&client, origin))).await;
    let mut identity: Option<ClusterTargetIdentity> = None;
    for response in responses {
        let current = response?;
        if identity
            .as_ref()
            .is_some_and(|seen| seen.cluster_id != current.cluster_id)
            || binding.is_some_and(|binding| binding.cluster_id != current.cluster_id)
        {
            return Err(TargetError::IdentityConflict);
        }
        if let Some(seen) = &mut identity {
            seen.group0_management_endpoints
                .extend(current.group0_management_endpoints);
            seen.group0_rpc_endpoints.extend(current.group0_rpc_endpoints);
        } else {
            identity = Some(current);
        }
    }
    let mut identity = identity.ok_or(TargetError::Missing)?;
    identity.group0_management_endpoints.sort();
    identity.group0_management_endpoints.dedup();
    identity.group0_rpc_endpoints.sort();
    identity.group0_rpc_endpoints.dedup();
    Ok(ResolvedClusterTarget {
        identity,
        management_endpoints,
        runtime: binding.and_then(|binding| binding.runtime.clone()),
    })
}

impl ResolvedClusterTarget {
    /// Confirm native transport resolves to the same committed application UUID.
    ///
    /// # Errors
    /// Rejects unreachable authority, missing publication and a different native
    /// cluster, even if its TCP listener accepts connections.
    pub async fn verify_native_authority(
        &self,
        kv: &crowdb_kv_client::CrowdbKvClient,
    ) -> Result<(), TargetError> {
        use crowdb_kv_client::{GetOutcome, ReadMode};
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            kv.get(0, 0, super::CLUSTER_KEY, ReadMode::Linearizable, None),
        )
        .await
        .map_err(|_| TargetError::Unavailable)?
        .map_err(|_| TargetError::Unavailable)?;
        let GetOutcome::Found { value, .. } = result else {
            return Err(TargetError::Unavailable);
        };
        if value.len() > 1024 * 1024 {
            return Err(TargetError::Invalid);
        }
        let prepared: super::PreparedBootstrap =
            serde_json::from_slice(&value).map_err(|_| TargetError::Invalid)?;
        prepared.validate().map_err(|_| TargetError::Invalid)?;
        if prepared.identity.cluster_id != self.identity.cluster_id {
            return Err(TargetError::IdentityConflict);
        }
        Ok(())
    }

    /// Verify native transport reachability separately from HTTP-only attach.
    ///
    /// # Errors
    /// Rejects an unreachable advertised RPC endpoint before workload writes.
    pub async fn require_native_transport(&self) -> Result<(), TargetError> {
        let results = futures::future::join_all(self.identity.group0_rpc_endpoints.iter().map(|endpoint| {
            tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(endpoint))
        }))
        .await;
        if results.is_empty() || results.into_iter().any(|result| !matches!(result, Ok(Ok(_)))) {
            return Err(TargetError::RpcUnreachable);
        }
        Ok(())
    }
}

fn validate_binding(
    binding: Option<&ClusterTargetBinding>,
    runtime: Option<&RuntimeSelection>,
) -> Result<(), TargetError> {
    let Some(binding) = binding else { return Ok(()) };
    if binding.version != 1
        || !uuid(&binding.cluster_id)
        || binding.name.is_empty()
        || binding.name.len() > 48
        || !binding.name.as_bytes()[0].is_ascii_alphanumeric()
        || !binding
            .name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err(TargetError::Invalid);
    }
    management_origins(&binding.management_endpoints)?;
    if let Some(ownership) = &binding.runtime {
        if !uuid(&ownership.owner)
            || ownership.selection.endpoint.is_empty()
            || !matches!(ownership.selection.runtime.as_str(), "docker" | "containerd")
        {
            return Err(TargetError::Invalid);
        }
        if runtime != Some(&ownership.selection) {
            return Err(TargetError::RuntimeConflict);
        }
    }
    Ok(())
}

fn uuid(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|id| !id.is_nil() && id.to_string() == value)
}

/// Validate and canonicalize explicit console management origins.
///
/// # Errors
/// Rejects empty, excessive, malformed or credential-bearing endpoints.
pub fn management_origins(values: &[String]) -> Result<Vec<String>, TargetError> {
    if values.is_empty() {
        return Err(TargetError::Missing);
    }
    if values.len() > 16 {
        return Err(TargetError::Invalid);
    }
    let mut result = Vec::new();
    for value in values {
        let url = reqwest::Url::parse(value).map_err(|_| TargetError::Invalid)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port() == Some(0)
        {
            return Err(TargetError::Invalid);
        }
        result.push(url.as_str().trim_end_matches('/').to_owned());
    }
    result.sort();
    result.dedup();
    Ok(result)
}

async fn observe(client: &reqwest::Client, origin: &str) -> Result<ClusterTargetIdentity, TargetError> {
    let mut response = client
        .get(format!("{origin}/api/cluster/target"))
        .send()
        .await
        .map_err(|_| TargetError::Unavailable)?
        .error_for_status()
        .map_err(|_| TargetError::Unavailable)?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| TargetError::Unavailable)? {
        if body.len() + chunk.len() > 32768 {
            return Err(TargetError::Invalid);
        }
        body.extend_from_slice(&chunk);
    }
    let mut identity: ClusterTargetIdentity =
        serde_json::from_slice(&body).map_err(|_| TargetError::Invalid)?;
    if identity.version != 1
        || !uuid(&identity.cluster_id)
        || identity.group0_rpc_endpoints.is_empty()
        || identity.group0_rpc_endpoints.len() > 16
    {
        return Err(TargetError::Invalid);
    }
    identity.group0_management_endpoints =
        management_origins(&identity.group0_management_endpoints).map_err(|_| TargetError::Invalid)?;
    for endpoint in &identity.group0_rpc_endpoints {
        // Use the URL parser for DNS and bracketed IPv6, while rejecting paths,
        // credentials and omitted/zero ports on the native transport address.
        let url = reqwest::Url::parse(&format!("tcp://{endpoint}")).map_err(|_| TargetError::Invalid)?;
        if url.host_str().is_none()
            || url.port().map_or(true, |port| port == 0)
            || !url.path().is_empty()
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(TargetError::Invalid);
        }
    }
    Ok(identity)
}
