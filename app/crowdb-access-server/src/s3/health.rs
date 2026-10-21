// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{dependencies, full_body, HandlerFuture, ResponseBody, S3Dispatcher, S3HttpHandler};
use crowdb_access_s3::metrics::{DependencyHealth, S3HealthSnapshot};
use hyper::{body::Incoming, Method, Request, Response, StatusCode};
use serde_json::json;
use std::sync::Arc;

/// Dedicated health surface sharing the data listener's dependency state.
pub struct AccessHealthHandler(pub Arc<S3Dispatcher>);

impl S3HttpHandler for AccessHealthHandler {
    fn handle(&self, request: Request<Incoming>) -> HandlerFuture {
        let dispatcher = Arc::clone(&self.0);
        Box::pin(async move {
            let response = match request.uri().path() {
                "/_crowdb/health/ready" if matches!(*request.method(), Method::GET | Method::HEAD) => Some(
                    dispatcher
                        .readiness_response(request.method() == Method::HEAD)
                        .await,
                ),
                "/_crowdb/health/live" => dispatcher.operational_response(&request),
                _ => None,
            };
            Ok(response.unwrap_or_else(|| {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(full_body(hyper::body::Bytes::new()))
                    .expect("static health response")
            }))
        })
    }
}

impl S3Dispatcher {
    async fn readiness_response(&self, head_only: bool) -> Response<ResponseBody> {
        let observed = tokio::time::timeout(
            std::time::Duration::from_millis(800),
            Arc::clone(&self.operations).observe_dependencies(),
        )
        .await
        .unwrap_or_else(|_| {
            Some(super::StorageReadiness {
                metadata: Err("Dependency observation exceeded its 800 ms deadline".into()),
                chunks: Err("Dependency observation exceeded its 800 ms deadline".into()),
            })
        });
        let mut snapshot = self
            .health
            .snapshot(&self.metrics, self.body_allocator.as_deref());
        let mut details = serde_json::Map::new();
        if let Some(observed) = observed {
            snapshot.readiness.metadata = dependencies::health(&observed.metadata);
            snapshot.readiness.chunks = dependencies::health(&observed.chunks);
            for (name, result) in [("metadata", observed.metadata), ("chunks", observed.chunks)] {
                details.insert(
                    name.into(),
                    json!(match result {
                        Ok(detail) | Err(detail) => detail,
                    }),
                );
            }
        }
        if let Some(ready) = self.authenticator.is_ready() {
            snapshot.readiness.authentication = if ready {
                DependencyHealth::Ready
            } else {
                DependencyHealth::Unavailable
            };
        }
        readiness_response(snapshot, &details, head_only)
    }
}

fn readiness_response(
    snapshot: S3HealthSnapshot,
    details: &serde_json::Map<String, serde_json::Value>,
    head_only: bool,
) -> Response<ResponseBody> {
    let readiness = snapshot.readiness;
    let ready = snapshot.live && readiness.is_ready();
    let mut reasons = Vec::new();
    if !snapshot.live {
        reasons.push("live");
    }
    for (name, state) in [
        ("listener", readiness.listener),
        ("metadata", readiness.metadata),
        ("chunks", readiness.chunks),
        ("native_pool", readiness.native_pool),
        ("cleanup", readiness.cleanup),
        ("authentication", readiness.authentication),
    ] {
        if !state.can_admit() {
            reasons.push(name);
        }
    }
    let body = json!({
        "ready": ready,
        "live": snapshot.live,
        "listener": readiness.listener.as_str(),
        "metadata": readiness.metadata.as_str(),
        "chunks": readiness.chunks.as_str(),
        "native_pool": readiness.native_pool.as_str(),
        "cleanup": readiness.cleanup.as_str(),
        "authentication": readiness.authentication.as_str(),
        "native_retained_bytes": snapshot.native_retained_bytes,
        "native_budget_bytes": snapshot.native_budget_bytes,
        "cleanup_backlog": snapshot.cleanup_backlog,
        "cleanup_backlog_limit": snapshot.cleanup_backlog_limit,
        "reasons": reasons,
        "details": details,
    });
    Response::builder()
        .status(if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        })
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(full_body(if head_only {
            hyper::body::Bytes::new()
        } else {
            body.to_string().into()
        }))
        .expect("static health response")
}
