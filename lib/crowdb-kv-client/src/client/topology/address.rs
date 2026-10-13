// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::mgmt::TopologyResponse;

// A wildcard listener describes a bind socket, not a remotely routable identity.
// Only the reporting server's local listener inherits the management origin;
// explicit peer addresses remain unchanged.
pub(super) fn resolve_local_endpoints(body: &mut TopologyResponse, seed: &str) {
    let Ok(origin) = reqwest::Url::parse(seed) else {
        return;
    };
    let Some(host) = origin.host_str() else {
        return;
    };
    let host = host.trim_matches(['[', ']']);
    for store in &mut body.stores {
        let Some(endpoint) = &store.listen_addr else {
            continue;
        };
        let raw = endpoint.strip_prefix("http://").unwrap_or(endpoint);
        let Ok(address) = raw.parse::<std::net::SocketAddr>() else {
            continue;
        };
        if address.ip().is_unspecified() {
            let resolved = if host.contains(':') {
                format!("[{host}]:{}", address.port())
            } else {
                format!("{host}:{}", address.port())
            };
            store.listen_addr = Some(if endpoint.starts_with("http://") {
                format!("http://{resolved}")
            } else {
                resolved
            });
        }
    }
}
