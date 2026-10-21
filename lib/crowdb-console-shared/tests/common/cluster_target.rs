// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{routing::get, Json, Router};
use crowdb_protocol::mgmt::cluster::ClusterTargetIdentity;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub struct TestTargetServer {
    pub origin: String,
    pub requests: Arc<AtomicUsize>,
    worker: tokio::task::JoinHandle<()>,
}

impl TestTargetServer {
    pub async fn start(identity: ClusterTargetIdentity) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&requests);
        let router = Router::new().route(
            "/api/cluster/target",
            get(move || {
                let counter = Arc::clone(&counter);
                let identity = identity.clone();
                async move {
                    counter.fetch_add(1, Ordering::Relaxed);
                    Json(identity)
                }
            }),
        );
        let worker = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            origin,
            requests,
            worker,
        }
    }
}

impl Drop for TestTargetServer {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

pub fn identity(id: u128) -> ClusterTargetIdentity {
    ClusterTargetIdentity {
        version: 1,
        cluster_id: uuid::Uuid::from_u128(id).to_string(),
        group0_management_endpoints: vec!["http://127.0.0.1:10000".into()],
        group0_rpc_endpoints: vec!["127.0.0.1:10100".into()],
    }
}
