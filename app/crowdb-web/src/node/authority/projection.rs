// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Project confirmed node addresses without replacing registered service identities.

use crowdb_console_shared::{
    config::{ServerEntry, ServiceType},
    deployment::registry::NodeRecord,
};

pub(crate) fn project(servers: &mut Vec<ServerEntry>, bootstrap: Vec<ServerEntry>, nodes: &[NodeRecord]) {
    for server in bootstrap {
        if server.service_type == ServiceType::PaxosKv
            && !servers.iter().any(|existing| {
                existing.service_type == ServiceType::PaxosKv && existing.node_id == server.node_id
            })
        {
            servers.push(server);
        }
    }
    for node in nodes.iter().filter(|node| node.confirmed && !node.cancelled) {
        let host = if node.host.contains(':') {
            format!("[{}]", node.host)
        } else {
            node.host.clone()
        };
        if let Some(server) = servers.iter_mut().find(|server| {
            server.node_id == Some(node.node_id) && server.service_type == ServiceType::PaxosKv
        }) {
            let management_port = reqwest::Url::parse(&server.url)
                .ok()
                .and_then(|url| url.port_or_known_default())
                .unwrap_or(crowdb_protocol::PAXOS_KV_SERVER_MGMT_BASE);
            let rpc_port = server
                .rpc_url
                .as_deref()
                .and_then(|url| reqwest::Url::parse(&format!("http://{url}")).ok())
                .and_then(|url| url.port_or_known_default())
                .unwrap_or(crowdb_protocol::PAXOS_KV_SERVER_RPC_BASE);
            server.url = format!("http://{host}:{management_port}");
            server.rpc_url = Some(format!("{host}:{rpc_port}"));
        } else {
            let mut server = crowdb_console_shared::config::ServerEntry::new(
                format!("kv-{}", node.node_id),
                format!("http://{host}:{}", crowdb_protocol::PAXOS_KV_SERVER_MGMT_BASE),
            );
            server.node_id = Some(node.node_id);
            server.rpc_url = Some(format!("{host}:{}", crowdb_protocol::PAXOS_KV_SERVER_RPC_BASE));
            servers.push(server);
        }
    }
}

#[cfg(feature = "test-util")]
pub fn project_confirmed_servers_for_tests(
    servers: &mut Vec<ServerEntry>,
    bootstrap: Vec<ServerEntry>,
    nodes: &[NodeRecord],
) {
    project(servers, bootstrap, nodes);
}
