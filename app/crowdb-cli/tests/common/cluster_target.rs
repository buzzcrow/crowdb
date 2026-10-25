// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{routing::get, Json, Router};
use crowdb_protocol::mgmt::cluster::ClusterTargetIdentity;

pub struct TestTargetServer {
    pub origin: String,
    worker: tokio::task::JoinHandle<()>,
}

impl TestTargetServer {
    pub async fn start(cluster_id: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let identity = ClusterTargetIdentity {
            version: 1,
            cluster_id: cluster_id.into(),
            group0_management_endpoints: vec![origin.clone()],
            group0_rpc_endpoints: vec![closed.local_addr().unwrap().to_string()],
        };
        drop(closed);
        let router = Router::new().route(
            "/api/cluster/target",
            get(move || {
                let identity = identity.clone();
                async move { Json(identity) }
            }),
        );
        let worker = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { origin, worker }
    }
}

impl Drop for TestTargetServer {
    fn drop(&mut self) {
        self.worker.abort();
    }
}
