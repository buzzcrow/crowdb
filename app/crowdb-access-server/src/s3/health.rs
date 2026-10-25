// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Health queries only read the backend sampler's latest immutable evidence.

mod observation;
pub(in crate::s3) use observation::HealthObservations;
pub use observation::HealthObserver;

use super::{full_body, HandlerFuture, ResponseBody, S3Dispatcher, S3HttpHandler};
use crowdb_access_s3::metrics::S3Readiness;
use hyper::{body::Incoming, Method, Request, Response, StatusCode};
use observation::{HealthObservation, MAX_AGE};
use serde_json::json;
use std::sync::Arc;

/// Dedicated health surface sharing the data listener's backend observations.
pub struct AccessHealthHandler(pub Arc<S3Dispatcher>);

impl S3HttpHandler for AccessHealthHandler {
    fn handle(&self, request: Request<Incoming>) -> HandlerFuture {
        let response = match request.uri().path() {
            "/_crowdb/health/ready" if matches!(*request.method(), Method::GET | Method::HEAD) => {
                let observed = self.0.observations.latest.load_full();
                Some(readiness_response(
                    observed.as_deref(),
                    self.0.health.is_live(),
                    request.method() == Method::HEAD,
                ))
            }
            "/_crowdb/health/live" => self.0.operational_response(&request),
            _ => None,
        }
        .unwrap_or_else(|| {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(full_body(hyper::body::Bytes::new()))
                .expect("static health response")
        });
        Box::pin(async move { Ok(response) })
    }
}

fn readiness_response(
    observed: Option<&HealthObservation>,
    live: bool,
    head_only: bool,
) -> Response<ResponseBody> {
    let age = observed.map(|value| value.started.elapsed());
    let current = age.is_some_and(|age| age <= MAX_AGE);
    let state = if current {
        "current"
    } else if observed.is_some() {
        "expired"
    } else {
        "pending"
    };
    let snapshot = observed.map(|value| value.health);
    let readiness = snapshot
        .filter(|_| current)
        .map_or_else(S3Readiness::unknown, |health| health.readiness);
    let ready = live && current && readiness.is_ready();
    let reasons = failure_reasons(readiness, live, current, observed.is_some());
    let body = json!({
        "ready": ready, "live": live,
        "listener": readiness.listener.as_str(), "metadata": readiness.metadata.as_str(),
        "chunks": readiness.chunks.as_str(), "native_pool": readiness.native_pool.as_str(),
        "cleanup": readiness.cleanup.as_str(), "authentication": readiness.authentication.as_str(),
        "native_retained_bytes": snapshot.map(|value| value.native_retained_bytes),
        "native_budget_bytes": snapshot.map(|value| value.native_budget_bytes),
        "cleanup_backlog": snapshot.map(|value| value.cleanup_backlog),
        "cleanup_backlog_limit": snapshot.map(|value| value.cleanup_backlog_limit),
        "reasons": reasons, "details": observed.map(|value| &value.details),
        "observation": {
            "state": state,
            "checked_at_unix_ms": observed.map(|value| value.checked_at_unix_ms),
            "age_ms": age.map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX)),
            "max_age_ms": u64::try_from(MAX_AGE.as_millis()).unwrap_or(u64::MAX),
            "generation": observed.map_or(0, |value| value.generation),
        },
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

fn failure_reasons(readiness: S3Readiness, live: bool, current: bool, observed: bool) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if !live {
        reasons.push("live");
    }
    if !current {
        reasons.push(if observed {
            "observation-expired"
        } else {
            "observation-pending"
        });
    }
    for (name, dependency) in [
        ("listener", readiness.listener),
        ("metadata", readiness.metadata),
        ("chunks", readiness.chunks),
        ("native_pool", readiness.native_pool),
        ("cleanup", readiness.cleanup),
        ("authentication", readiness.authentication),
    ] {
        if !dependency.can_admit() {
            reasons.push(name);
        }
    }
    reasons
}
