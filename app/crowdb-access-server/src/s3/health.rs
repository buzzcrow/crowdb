// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{full_body, HandlerFuture, S3Dispatcher, S3HttpHandler};
use hyper::{body::Incoming, Request, Response, StatusCode};
use std::sync::Arc;

/// Dedicated health surface sharing the data listener's dependency state.
pub struct AccessHealthHandler(pub Arc<S3Dispatcher>);

impl S3HttpHandler for AccessHealthHandler {
    fn handle(&self, request: Request<Incoming>) -> HandlerFuture {
        let response = if matches!(
            request.uri().path(),
            "/_crowdb/health/live" | "/_crowdb/health/ready"
        ) {
            self.0.operational_response(&request)
        } else {
            None
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
