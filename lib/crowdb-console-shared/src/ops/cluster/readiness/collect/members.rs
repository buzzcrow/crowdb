// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Each confirmed member supplies bounded current application evidence.

use super::{fact, now_ms};
use crate::deployment::{registry, PreparedBootstrap};
use crowdb_protocol::mgmt::{
    cluster::{ClusterMemberObservation, ClusterObservation, DependencyKind, DependencyState},
    node::{observation::NodeRuntimeObservation, NodeServiceIntent},
};
use futures::StreamExt;
use std::collections::BTreeMap;
use tokio::time::Instant;

pub(super) async fn read(
    bootstrap: &PreparedBootstrap,
    local: &NodeRuntimeObservation,
    observation: &mut ClusterObservation,
    registry: registry::NodeRegistry,
    intents: Option<Vec<NodeServiceIntent>>,
    deadline: Instant,
) -> BTreeMap<u64, NodeRuntimeObservation> {
    let mut verified = BTreeMap::new();
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build();
    let Ok(client) = client else {
        return verified;
    };
    let expected: Vec<_> = registry
        .nodes
        .into_iter()
        .filter(|node| node.confirmed && !node.cancelled)
        .collect();
    for member in bootstrap.intent.members() {
        if !expected.iter().any(|node| node.node_id == *member) {
            observation.dependencies.push(fact(
                &format!("node/{member}"),
                DependencyKind::Member,
                DependencyState::Unknown,
                "confirmed-member-missing",
            ));
        }
    }
    let replies: Vec<_> = futures::stream::iter(expected.into_iter().map(|node| {
        let client = &client;
        async move {
            let observed = if node.discovery_id == local.discovery_id {
                Some(local.clone())
            } else {
                tokio::time::timeout_at(deadline, member(client, &node))
                    .await
                    .ok()
                    .flatten()
            };
            (node, observed)
        }
    }))
    .buffer_unordered(8)
    .collect()
    .await;
    for (node, observed) in replies {
        let host = node.host.trim_matches(['[', ']']);
        observation.members.push(ClusterMemberObservation {
            discovery_id: node.discovery_id.clone(),
            node_id: Some(node.node_id),
            management_endpoints: vec![format!(
                "http://{}:{}",
                if host.contains(':') {
                    format!("[{host}]")
                } else {
                    host.into()
                },
                node.web_port,
            )],
        });
        let now = now_ms();
        let facts = super::super::member_evidence(
            bootstrap,
            &node,
            &local.policy,
            intents.as_deref(),
            observed.as_ref(),
            now,
        );
        if facts
            .first()
            .is_some_and(|fact| fact.kind == DependencyKind::Member && fact.state == DependencyState::Ready)
            && observed.as_ref().is_some_and(|value| {
                value.observed_at_unix_ms <= now && now - value.observed_at_unix_ms <= 3000
            })
        {
            if let Some(value) = observed {
                verified.insert(node.node_id, value);
            }
        }
        observation.dependencies.extend(facts);
    }
    observation.members.sort_by_key(|member| member.node_id);
    verified
}

async fn member(client: &reqwest::Client, node: &registry::NodeRecord) -> Option<NodeRuntimeObservation> {
    let host = node.host.trim_matches(['[', ']']);
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.into()
    };
    let mut reply = client
        .get(format!(
            "http://{host}:{}/api/cluster/node-observation",
            node.web_port
        ))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = reply.chunk().await.ok()? {
        if bytes.len() + chunk.len() > 256 * 1024 {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}
