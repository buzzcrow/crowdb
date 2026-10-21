// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Verify each member's local evidence against committed identity and intent.

use crate::deployment::{registry::NodeRecord, PreparedBootstrap};
use crowdb_protocol::mgmt::{
    cluster::{DependencyKind, DependencyObservation, DependencyState, ReadinessReason},
    node::{
        deployment::NodeDeploymentPolicy, observation::NodeRuntimeObservation, setup::SetupServiceKind,
        NodeServiceAction, NodeServiceIntent,
    },
};

/// Project a bounded node response; missing or conflicting members cannot be
/// hidden by another member's healthy service of the same kind.
#[must_use]
pub fn member_evidence(
    bootstrap: &PreparedBootstrap,
    expected: &NodeRecord,
    policy: &NodeDeploymentPolicy,
    intents: Option<&[NodeServiceIntent]>,
    observed: Option<&NodeRuntimeObservation>,
    now: u64,
) -> Vec<DependencyObservation> {
    let id = format!("node/{}", expected.node_id);
    let Some(node) = observed else {
        return vec![fact(
            id,
            DependencyKind::Member,
            DependencyState::Unknown,
            now,
            "member-unreachable",
        )];
    };
    if duplicate_evidence(node)
        || node.version != 1
        || node.discovery_id != expected.discovery_id
        || node.policy.validate().is_err()
        || node.policy.formation() != policy.formation()
        || node.binding.as_ref().is_some_and(|binding| {
            binding.node_id != expected.node_id || binding.bootstrap != bootstrap.identity
        })
    {
        return vec![fact(
            id,
            DependencyKind::Identity,
            DependencyState::Failed,
            now,
            "member-identity-conflict",
        )];
    }
    if node.binding.is_none() {
        return vec![fact(
            id,
            DependencyKind::Member,
            DependencyState::Initializing,
            node.observed_at_unix_ms,
            "member-binding-pending",
        )];
    }
    let mut facts = vec![fact(
        id,
        DependencyKind::Member,
        DependencyState::Ready,
        node.observed_at_unix_ms,
        "member-observed",
    )];
    for selected in node
        .policy
        .setup
        .services
        .iter()
        .filter(|service| service.enabled)
    {
        let kind = service_name(selected.kind);
        let current: Vec<_> = intents
            .into_iter()
            .flatten()
            .filter(|intent| {
                intent.node_id == expected.node_id
                    && intent.cluster_id == bootstrap.identity.cluster_id
                    && intent.kind == kind
            })
            .collect();
        if current.is_empty() {
            facts.push(service_fact(node, kind, None));
        } else {
            for intent in current {
                facts.push(service_fact(node, kind, Some(intent)));
            }
        }
        if selected.kind == SetupServiceKind::AccessServer {
            for protocol in ["s3", "iceberg", "dataset"] {
                facts.push(protocol_fact(
                    node,
                    protocol,
                    intents,
                    &bootstrap.identity.cluster_id,
                ));
            }
        }
    }
    facts
}

fn service_fact(
    node: &NodeRuntimeObservation,
    kind: &str,
    current: Option<&NodeServiceIntent>,
) -> DependencyObservation {
    let node_id = node.binding.as_ref().map_or(0, |binding| binding.node_id);
    let service_id = current.map_or_else(|| format!("{kind}-{node_id}"), |intent| intent.service_id.clone());
    let observed = node.services.iter().find(|service| {
        service.kind == kind
            && (service.service_id == service_id
                || (kind == "kv" && current.is_none() && service.service_id == "kv"))
    });
    let mut result = fact(
        format!("node/{node_id}/service/{service_id}"),
        DependencyKind::Service,
        DependencyState::Unknown,
        node.observed_at_unix_ms,
        "service-observation-missing",
    );
    result.current_operation_id = current.map(|intent| intent.operation_id.clone());
    if let Some(observed) = observed {
        result.operation_id.clone_from(&observed.operation_id);
        result.observed_at_unix_ms = observed.observed_at_unix_ms;
        result.max_age_ms = observed.max_age_ms.min(3000);
        result.state = observed.state;
    }
    if current.is_none() && kind != "kv" {
        result.state = DependencyState::Unknown;
    }
    if current
        .is_some_and(|intent| matches!(intent.action, NodeServiceAction::Stop | NodeServiceAction::Delete))
    {
        result.state = DependencyState::Stopped;
        result.operation_id = result.current_operation_id.clone();
        result.observed_at_unix_ms = node.observed_at_unix_ms;
    }
    result.reason =
        (result.state != DependencyState::Ready).then(|| reason(&result.id, "required-service-not-ready"));
    result
}

fn protocol_fact(
    node: &NodeRuntimeObservation,
    protocol: &str,
    intents: Option<&[NodeServiceIntent]>,
    cluster: &str,
) -> DependencyObservation {
    let node_id = node.binding.as_ref().map_or(0, |binding| binding.node_id);
    let id = format!("node/{node_id}/protocol/{protocol}");
    let current = intents.into_iter().flatten().find(|intent| {
        intent.node_id == node_id
            && intent.cluster_id == cluster
            && intent.service_id == format!("access-{node_id}")
            && intent.kind == "access"
    });
    let mut result = fact(
        id.clone(),
        DependencyKind::Protocol,
        DependencyState::Unknown,
        node.observed_at_unix_ms,
        "protocol-observation-missing",
    );
    result.current_operation_id = current.map(|intent| intent.operation_id.clone());
    if let Some(observed) = node
        .protocols
        .iter()
        .find(|observed| observed.id == id && observed.kind == DependencyKind::Protocol)
    {
        result.operation_id.clone_from(&observed.operation_id);
        result.observed_at_unix_ms = observed.observed_at_unix_ms;
        result.max_age_ms = observed.max_age_ms.min(3000);
        result.state = observed.state;
    }
    if current.is_none() {
        result.state = DependencyState::Unknown;
    } else if current
        .is_some_and(|intent| matches!(intent.action, NodeServiceAction::Stop | NodeServiceAction::Delete))
    {
        result.state = DependencyState::Stopped;
        result.operation_id = result.current_operation_id.clone();
        result.observed_at_unix_ms = node.observed_at_unix_ms;
    }
    result.reason =
        (result.state != DependencyState::Ready).then(|| reason(&id, "required-protocol-not-ready"));
    result
}

fn service_name(kind: SetupServiceKind) -> &'static str {
    match kind {
        SetupServiceKind::PaxosKv => "kv",
        SetupServiceKind::Diskdb => "diskdb",
        SetupServiceKind::Diskio => "diskio",
        SetupServiceKind::Chunkdb => "chunkdb",
        SetupServiceKind::ChunkKv => "chunk-kv",
        SetupServiceKind::AccessServer => "access",
    }
}

fn fact(
    id: String,
    kind: DependencyKind,
    state: DependencyState,
    now: u64,
    code: &str,
) -> DependencyObservation {
    let reason = (state != DependencyState::Ready).then(|| reason(&id, code));
    DependencyObservation {
        id,
        kind,
        required: true,
        state,
        observed_at_unix_ms: now,
        max_age_ms: 3000,
        revision: None,
        operation_id: None,
        current_operation_id: None,
        reason,
    }
}

fn reason(id: &str, code: &str) -> ReadinessReason {
    ReadinessReason {
        dependency: id.into(),
        code: code.into(),
        detail: "Required member evidence is unavailable or does not match current authority".into(),
    }
}

fn duplicate_evidence(node: &NodeRuntimeObservation) -> bool {
    let mut services = std::collections::BTreeSet::new();
    let mut protocols = std::collections::BTreeSet::new();
    node.services.len() > 64
        || node.protocols.len() > 3
        || node
            .services
            .iter()
            .any(|service| !services.insert(&service.service_id))
        || node
            .protocols
            .iter()
            .any(|protocol| !protocols.insert(&protocol.id))
}
