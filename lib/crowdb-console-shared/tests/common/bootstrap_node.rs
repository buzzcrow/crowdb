// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use crowdb_console_shared::config::{ConsoleConfig, ServerEntry};
use crowdb_console_shared::ops::OpContext;
use crowdb_protocol::mgmt::{GroupStatus, StoreStatus, SystemInitRequest, SystemInitResponse};
use serde_json::json;

pub struct TestNode {
    url: String,
    pub deleted: Arc<AtomicBool>,
    server: tokio::task::JoinHandle<()>,
}

impl TestNode {
    pub async fn start(existing: Option<u64>, reject: bool) -> Self {
        Self::with_wiring(existing, reject, false, false).await
    }

    pub async fn with_wiring(
        existing: Option<u64>,
        reject: bool,
        omit_endpoint: bool,
        reject_wiring: bool,
    ) -> Self {
        let deleted = Arc::new(AtomicBool::new(false));
        let observed = deleted.clone();
        let app = Router::new()
            .route("/health", get(move || async move {
                (if reject { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::OK }, Json(json!({"status":"ok"})))
            }))
            .route("/system/init", post(move |Json(request): Json<SystemInitRequest>| async move {
                (if existing.is_some() { StatusCode::CONFLICT } else { StatusCode::CREATED },
                    Json(SystemInitResponse { store_id: 0, group_id: 0, replica_id: request.replica_id, listen_addr: Some("127.0.0.1:9".into()) }))
            }))
            .route("/topology", get(move || async move {
                Json(json!({"stores": [StoreStatus { store_id: 0, listen_addr: (!omit_endpoint).then(|| "127.0.0.1:9".into()),
                    groups: vec![GroupStatus { group_id: 0, local_replica_id: existing.unwrap_or(1), ..Default::default() }],
                    ..Default::default() }]}))
            }))
            .route("/stores/0/groups/0/remotes", post(move || async move {
                if reject_wiring { StatusCode::INTERNAL_SERVER_ERROR } else { StatusCode::OK }
            }))
            .route("/stores/0/groups/0", delete(move || {
                observed.store(true, Ordering::SeqCst);
                async { StatusCode::NO_CONTENT }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, deleted, server }
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.server.abort();
    }
}

pub fn context(first: &TestNode, second: &TestNode) -> OpContext {
    let servers: Vec<ServerEntry> = [&first.url, &second.url]
        .into_iter()
        .enumerate()
        .map(|(i, url)| {
            serde_json::from_value(json!({"id": (i + 1).to_string(), "node_id": i + 1, "url": url})).unwrap()
        })
        .collect();
    let config = ConsoleConfig {
        servers,
        ..Default::default()
    };
    OpContext::new("127.0.0.1:9".into(), Vec::new(), config)
}
