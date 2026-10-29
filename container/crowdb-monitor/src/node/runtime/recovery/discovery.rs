// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Discovery supplies hints; only pinned SSH proofs enter the local route view.

use super::super::{durable, AcceptedNode, Result};
use crowdb_console_shared::{
    deployment::{recovery::MemberTrust, PreparedBootstrap},
    ConsoleConfig,
};
use crowdb_protocol::mgmt::node::{
    recovery::{MemberEndpoint, MemberEndpoints},
    CandidateSnapshot,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(in crate::node::runtime) struct Worker(tokio::task::JoinHandle<()>);
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(in crate::node::runtime) fn start(root: std::path::PathBuf, port: u16) -> Worker {
    Worker(tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(3));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if let Err(error) = refresh(&root, port).await {
                tracing::debug!(%error, "member endpoint recovery is pending");
            }
        }
    }))
}

async fn refresh(root: &Path, port: u16) -> Result<()> {
    let Some(accepted) = durable::read::<AcceptedNode>(&root.join("accepted-node.json"))? else {
        return Ok(());
    };
    if super::super::admission::is_cancelled(root, &accepted)
        || root
            .join(format!(
                "retired-bootstrap-{}.json",
                accepted.bootstrap.operation_id
            ))
            .exists()
    {
        return Err("member recovery is retired".into());
    }
    if let Err(error) = super::confirmed::refresh(root, &accepted).await {
        tracing::debug!(%error, "confirmed recovery contacts await current authority");
    }
    super::check(root, &accepted)?;
    let operation = PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))?;
    let (trust, config) = super::confirmed::contacts(root, &operation)?;
    let local = super::identity(root)?.endpoint;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(1))
        .build()?;
    let response = client
        .get(format!("http://127.0.0.1:{port}/candidates"))
        .send()
        .await?
        .error_for_status()?;
    if response.content_length().is_some_and(|size| size > 1024 * 1024) {
        return Err("candidate reply too large".into());
    }
    let snapshot: CandidateSnapshot = response.json().await?;
    let nodes = destinations(root, &config, &trust, snapshot)?;
    let replies = futures::future::join_all(nodes.iter().map(|node| trust.endpoint(node))).await;
    let endpoints = std::iter::once(local.clone()).chain(
        replies
            .into_iter()
            .filter_map(std::result::Result::ok)
            .map(|identity| identity.endpoint),
    );
    let (members, conflicts) = unique(endpoints);
    let snapshot = MemberEndpoints {
        version: 1,
        bootstrap: accepted.bootstrap,
        observer_boot_id: local.boot_id,
        observed_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .try_into()?,
        members,
        conflicts,
    };
    durable::write(&root.join("member-endpoints.json"), &snapshot)?;
    match tokio::time::timeout(
        Duration::from_secs(3),
        super::publication::reconcile(root, &operation, &snapshot, &config),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "recovered endpoint publication awaits authority"),
        Err(_) => tracing::debug!("recovered endpoint publication yielded at its deadline"),
    }
    Ok(())
}

fn destinations(
    root: &Path,
    config: &ConsoleConfig,
    trust: &MemberTrust,
    snapshot: CandidateSnapshot,
) -> Result<Vec<crowdb_console_shared::config::NodeEntry>> {
    let mut probes = Vec::new();
    let mut destinations = BTreeSet::new();
    for candidate in snapshot.nodes.into_iter().take(128) {
        let Some(member) = trust
            .members
            .iter()
            .find(|member| member.discovery_id == candidate.advertisement.discovery_id)
        else {
            continue;
        };
        if candidate.advertisement.cluster_id.as_deref() != Some(trust.bootstrap.cluster_id.as_str()) {
            continue;
        }
        for endpoint in candidate.advertisement.monitor_endpoints.iter().take(16) {
            let Ok(url) = reqwest::Url::parse(endpoint) else {
                continue;
            };
            let Some(host) = url
                .host_str()
                .and_then(|host| host.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok())
            else {
                continue;
            };
            if destinations.len() >= 64 || !destinations.insert((member.node_id, host)) {
                continue;
            }
            let mut node = config
                .node(member.node_id)
                .cloned()
                .ok_or("member topology is absent")?;
            node.host = host.to_string();
            node.ssh_password = None;
            node.ssh_key = Some(root.join("ssh/id_ed25519").to_string_lossy().into_owned());
            probes.push(node);
        }
    }
    Ok(probes)
}

fn unique(endpoints: impl Iterator<Item = MemberEndpoint>) -> (Vec<MemberEndpoint>, Vec<String>) {
    let mut members = BTreeMap::<u64, MemberEndpoint>::new();
    let mut conflicts = BTreeSet::new();
    for endpoint in endpoints {
        if let Some(prior) = members.get(&endpoint.node_id) {
            // A node keeps its discovery identity across process restarts, but
            // publishes a fresh boot id. The local observation is emitted
            // before peer observations, so retain that authenticated identity
            // instead of treating the restart as a conflicting member and
            // pinning the route to the old address.
            if prior.discovery_id != endpoint.discovery_id {
                conflicts.insert(prior.discovery_id.clone());
                conflicts.insert(endpoint.discovery_id.clone());
            }
        } else {
            members.insert(endpoint.node_id, endpoint);
        }
    }
    members.retain(|_, endpoint| !conflicts.contains(&endpoint.discovery_id));
    (members.into_values().collect(), conflicts.into_iter().collect())
}

#[cfg(feature = "test-util")]
#[must_use]
pub fn unique_member_endpoints_for_tests(
    endpoints: Vec<MemberEndpoint>,
) -> (Vec<MemberEndpoint>, Vec<String>) {
    unique(endpoints.into_iter())
}
