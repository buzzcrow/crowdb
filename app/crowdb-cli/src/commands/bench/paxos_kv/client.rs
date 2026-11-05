// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::args::{PaxosKvArgs, ReadEndpoint};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient, ReadEndpointPolicy};
use crowdb_protocol::mgmt::TopologyResponse;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub(super) async fn fetch(endpoint: &str, path: &str) -> Result<Value, String> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())?
        .get(format!("{endpoint}{path}"))
        .send()
        .await
        .map_err(|e| format!("{endpoint}{path}: {e}"))?
        .error_for_status()
        .map_err(|e| e.to_string())?;
    serde_json::from_slice(&response.bytes().await.map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}

pub(super) async fn connect(args: &PaxosKvArgs) -> Result<(PaxosKvClient, Vec<Value>), String> {
    if args.endpoints.len() != 3 || args.endpoints.iter().collect::<BTreeSet<_>>().len() != 3 {
        return Err("provide exactly three distinct --endpoint management origins".into());
    }
    let mut topologies = Vec::new();
    let mut replicas = BTreeMap::new();
    let mut memberships = Vec::new();
    let mut leaders = BTreeSet::new();
    for endpoint in &args.endpoints {
        let raw = fetch(endpoint, "/topology").await?;
        let topology: TopologyResponse = serde_json::from_value(raw.clone()).map_err(|e| e.to_string())?;
        let store = topology
            .stores
            .iter()
            .find(|s| s.store_id == args.store)
            .ok_or("store absent on a node")?;
        let group = store
            .groups
            .iter()
            .find(|g| g.group_id == args.group)
            .ok_or("group absent on a node")?;
        let address = store.listen_addr.clone().ok_or("RPC listener absent")?;
        let mut socket: std::net::SocketAddr =
            address.parse().map_err(|e| format!("invalid RPC address: {e}"))?;
        if socket.ip().is_unspecified() {
            let origin = reqwest::Url::parse(endpoint).map_err(|e| e.to_string())?;
            let ip = origin
                .host_str()
                .ok_or("missing host")?
                .parse()
                .map_err(|_| "use a numeric endpoint IP")?;
            socket.set_ip(ip);
        }
        let address = socket.to_string();
        tokio::time::timeout(Duration::from_secs(3), tokio::net::TcpStream::connect(socket))
            .await
            .map_err(|_| format!("RPC endpoint unreachable: {socket}"))?
            .map_err(|e| e.to_string())?;
        if replicas.insert(group.local_replica_id, address.clone()).is_some() {
            return Err("duplicate replica identity".into());
        }
        let mut membership = BTreeMap::from([(group.local_replica_id, address)]);
        for remote in &group.remotes {
            if !remote.voting || membership.insert(remote.id, remote.endpoint.clone()).is_some() {
                return Err("expected three distinct voting replicas".into());
            }
        }
        memberships.push(membership);
        leaders.insert(group.leader_id);
        topologies.push(raw);
    }
    if memberships.iter().any(|members| members != &replicas) || leaders.len() != 1 {
        return Err("nodes disagree on replica membership or leader; wait for deployment readiness".into());
    }
    let leader = leaders
        .first()
        .and_then(|id| replicas.get(id))
        .ok_or("no elected leader")?;
    let mut config = ClientConfig::new(args.endpoints.clone());
    config.pool_size_per_endpoint = args.connections;
    config.rpc_workers = args.rpc_workers;
    config.event_write = args.event_write;
    config.send_queue_capacity = args.send_queue_capacity;
    config.read_endpoint_policy = match args.read_endpoint_policy {
        ReadEndpoint::Leader => ReadEndpointPolicy::Leader,
        ReadEndpoint::AnyReplica => ReadEndpointPolicy::AnyReplica,
    };
    let client = PaxosKvClient::new(config);
    client.refresh_topology().await.map_err(|e| e.to_string())?;
    client.seed_leader(args.store, args.group, leader.clone());
    Ok((client, topologies))
}
