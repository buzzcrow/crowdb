// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crowdb_access_s3::auth::{AuthError, RawAuthRequest, RequestAuthenticator};
use crowdb_access_s3::error::{S3Error, S3ErrorCode};
use crowdb_access_s3::metrics::{OutcomeClass, S3Metrics};
use crowdb_access_s3::route::{classify_request, RouteError};
use hyper::body::{Http1BodyAllocator, Incoming};
use hyper::{Method, Request};

use super::{error_response, HandlerFuture, S3HttpHandler, S3Operations};

pub struct S3Dispatcher {
    authenticator: Arc<dyn RequestAuthenticator>,
    operations: Arc<dyn S3Operations>,
    metrics: Arc<S3Metrics>,
    host_id: String,
    trusted_network: bool,
    body_allocator: Option<Arc<dyn Http1BodyAllocator>>,
    next_request_id: AtomicU64,
}

impl S3Dispatcher {
    #[must_use]
    pub fn new(
        authenticator: Arc<dyn RequestAuthenticator>,
        operations: Arc<dyn S3Operations>,
        metrics: Arc<S3Metrics>,
        host_id: String,
        trusted_network: bool,
    ) -> Self {
        Self {
            authenticator,
            operations,
            metrics,
            host_id,
            trusted_network,
            body_allocator: None,
            next_request_id: AtomicU64::new(1),
        }
    }

    #[must_use]
    pub fn with_body_allocator(mut self, allocator: Arc<dyn Http1BodyAllocator>) -> Self {
        self.body_allocator = Some(allocator);
        self
    }

    fn request_id(&self) -> String {
        format!("{:016x}", self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }
}

impl S3HttpHandler for S3Dispatcher {
    fn handle(&self, request: Request<Incoming>) -> HandlerFuture {
        let authenticator = Arc::clone(&self.authenticator);
        let operations = Arc::clone(&self.operations);
        let metrics = Arc::clone(&self.metrics);
        let host_id = self.host_id.clone();
        let request_id = self.request_id();
        let trusted_network = self.trusted_network;
        let body_allocator = self.body_allocator.clone();
        Box::pin(async move {
            let mut request = request;
            let resource = request.uri().path().to_owned();
            let head_only = request.method() == Method::HEAD;
            if let Err(error) = authenticator
                .authenticate(RawAuthRequest::from_parts(
                    request.method(),
                    request.uri(),
                    request.headers(),
                ))
                .await
            {
                let code = match error {
                    AuthError::Rejected => S3ErrorCode::AccessDenied,
                    AuthError::Unavailable => S3ErrorCode::ServiceUnavailable,
                };
                return Ok(error_response(
                    &S3Error::new(code, resource, request_id, host_id),
                    head_only,
                ));
            }
            if trusted_network {
                metrics.record_trusted_auth_bypass();
            }
            let route = match classify_request(request.method(), request.uri(), request.headers()) {
                Ok(route) => route,
                Err(RouteError::NotImplemented) => {
                    return Ok(error_response(
                        &S3Error::not_implemented(resource, request_id, host_id),
                        head_only,
                    ));
                }
                Err(RouteError::Invalid) => {
                    return Ok(error_response(
                        &S3Error::new(S3ErrorCode::InvalidRequest, resource, request_id, host_id),
                        head_only,
                    ));
                }
            };
            let operation = route.operation;
            if operation == crowdb_access_s3::route::S3Operation::PutObject {
                if let Some(allocator) = body_allocator {
                    request.body_mut().set_http1_body_allocator(allocator);
                }
            }
            let started = Instant::now();
            let _in_flight = metrics.begin_request();
            let request_bytes = request
                .headers()
                .get(hyper::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let mut response = operations
                .execute(route, request, request_id.clone(), host_id.clone())
                .await;
            if let Ok(value) = hyper::header::HeaderValue::from_str(&request_id) {
                response.headers_mut().insert("x-amz-request-id", value);
            }
            if let Ok(value) = hyper::header::HeaderValue::from_str(&host_id) {
                response.headers_mut().insert("x-amz-id-2", value);
            }
            let outcome = match response.status().as_u16() {
                200..=299 => OutcomeClass::Success,
                400..=499 => OutcomeClass::ClientError,
                503 => OutcomeClass::Unavailable,
                _ => OutcomeClass::Internal,
            };
            let response_bytes = hyper::body::Body::size_hint(response.body()).exact().unwrap_or(0);
            metrics.finish_request(
                operation,
                outcome,
                request_bytes,
                response_bytes,
                u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            Ok(response)
        })
    }
}
