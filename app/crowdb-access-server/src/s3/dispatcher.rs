// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crowdb_access_s3::auth::{AuthError, RawAuthRequest, RequestAuthenticator};
use crowdb_access_s3::error::{S3Error, S3ErrorCode};
use crowdb_access_s3::metrics::{DependencyHealth, OutcomeClass, RequestMeasurement, S3Health, S3Metrics};
use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_access_s3::route::{classify_request, RouteError};
use crowdb_chunk_client::ChunkIoClient;
use hyper::body::{Http1BodyReceiveProvider, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use tracing::Instrument;

use super::{
    error_response, full_body, measured_body, DeferredBodyReceiveProvider, HandlerFuture, ResponseBody,
    S3HttpHandler, S3Operations,
};

pub struct S3Dispatcher {
    authenticator: Arc<dyn RequestAuthenticator>,
    operations: Arc<dyn S3Operations>,
    metrics: Arc<S3Metrics>,
    host_id: String,
    trusted_network: bool,
    body_receive_provider_factory: Option<Arc<dyn Fn() -> DeferredBodyReceiveProvider + Send + Sync>>,
    body_allocator: Option<Arc<NativeBodyAllocator>>,
    chunk_metrics: Option<Arc<ChunkIoClient>>,
    health: Arc<S3Health>,
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
            body_receive_provider_factory: None,
            body_allocator: None,
            chunk_metrics: None,
            health: Arc::new(S3Health::ready(u64::MAX)),
            next_request_id: AtomicU64::new(1),
        }
    }

    #[must_use]
    pub fn with_body_receive_provider_factory<F>(mut self, factory: F) -> Self
    where
        F: Fn() -> Arc<dyn Http1BodyReceiveProvider> + Send + Sync + 'static,
    {
        self.body_receive_provider_factory =
            Some(Arc::new(move || DeferredBodyReceiveProvider::generic(factory())));
        self
    }

    #[must_use]
    pub fn with_native_body_allocator(mut self, allocator: Arc<NativeBodyAllocator>) -> Self {
        self.body_allocator = Some(Arc::clone(&allocator));
        self.body_receive_provider_factory = Some(Arc::new(move || {
            DeferredBodyReceiveProvider::native(Arc::new(allocator.object_receiver()))
        }));
        self
    }

    #[must_use]
    pub fn with_health(mut self, health: Arc<S3Health>) -> Self {
        self.health = health;
        self
    }

    #[must_use]
    pub fn with_chunk_metrics(mut self, chunks: Arc<ChunkIoClient>) -> Self {
        self.chunk_metrics = Some(chunks);
        self
    }

    fn request_id(&self) -> String {
        format!("{:016x}", self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    fn operational_response(&self, request: &Request<Incoming>) -> Option<Response<ResponseBody>> {
        if request.method() != Method::GET && request.method() != Method::HEAD {
            return None;
        }
        let snapshot = self
            .health
            .snapshot(&self.metrics, self.body_allocator.as_deref());
        let (status, content_type, body) = match request.uri().path() {
            "/_crowdb/health/live" => {
                let status = if snapshot.live {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                (
                    status,
                    "application/json",
                    format!(r#"{{"live":{}}}"#, snapshot.live),
                )
            }
            "/_crowdb/health/ready" => {
                let ready = snapshot.live && snapshot.readiness.is_ready();
                let status = if ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                let readiness = snapshot.readiness;
                (
                    status,
                    "application/json",
                    format!(
                        concat!(
                            r#"{{"ready":{},"listener":"{}","metadata":"{}","chunks":"{}",#,
                            r#""native_pool":"{}","cleanup":"{}","authentication":"{}",#,
                            r#""native_retained_bytes":{},"native_budget_bytes":{},#,
                            r#""cleanup_backlog":{},"cleanup_backlog_limit":{}}}"#
                        ),
                        ready,
                        readiness.listener.as_str(),
                        readiness.metadata.as_str(),
                        readiness.chunks.as_str(),
                        readiness.native_pool.as_str(),
                        readiness.cleanup.as_str(),
                        readiness.authentication.as_str(),
                        snapshot.native_retained_bytes,
                        snapshot.native_budget_bytes,
                        snapshot.cleanup_backlog,
                        snapshot.cleanup_backlog_limit,
                    ),
                )
            }
            "/_crowdb/metrics" => (
                StatusCode::OK,
                "text/plain; version=0.0.4",
                self.metrics
                    .render_prometheus(self.body_allocator.as_deref(), self.chunk_metrics.as_deref()),
            ),
            _ => return None,
        };
        let body = if request.method() == Method::HEAD {
            Vec::new()
        } else {
            body.into_bytes()
        };
        Some(
            Response::builder()
                .status(status)
                .header(hyper::header::CONTENT_TYPE, content_type)
                .body(full_body(body.into()))
                .expect("operational response is valid"),
        )
    }
}

impl S3HttpHandler for S3Dispatcher {
    fn handle(&self, request: Request<Incoming>) -> HandlerFuture {
        if let Some(response) = self.operational_response(&request) {
            return Box::pin(async move { Ok(response) });
        }
        let authenticator = Arc::clone(&self.authenticator);
        let operations = Arc::clone(&self.operations);
        let metrics = Arc::clone(&self.metrics);
        let host_id = self.host_id.clone();
        let request_id = self.request_id();
        let trusted_network = self.trusted_network;
        let body_receive_provider_factory = self.body_receive_provider_factory.clone();
        let health = Arc::clone(&self.health);
        Box::pin(async move {
            let started = Instant::now();
            let _in_flight = metrics.begin_request();
            let mut request = request;
            let resource = request.uri().path().to_owned();
            let head_only = request.method() == Method::HEAD;
            let authentication_started = Instant::now();
            if let Err(error) = authenticator
                .authenticate(RawAuthRequest::from_parts(
                    request.method(),
                    request.uri(),
                    request.headers(),
                ))
                .await
            {
                if matches!(error, AuthError::Unavailable) {
                    health.set_authentication(DependencyHealth::Unavailable);
                }
                let (code, outcome) = auth_error_outcome(error);
                metrics.finish_predispatch(outcome, elapsed_ns(started));
                return Ok(error_response(
                    &S3Error::new(code, resource, request_id, host_id),
                    head_only,
                ));
            }
            health.set_authentication(DependencyHealth::Ready);
            let authentication_latency_ns = elapsed_ns(authentication_started);
            if trusted_network {
                metrics.record_trusted_auth_bypass();
            }
            let route = match classify_request(request.method(), request.uri(), request.headers()) {
                Ok(route) => route,
                Err(RouteError::NotImplemented) => {
                    metrics.finish_predispatch(OutcomeClass::ClientError, elapsed_ns(started));
                    return Ok(error_response(
                        &S3Error::not_implemented(resource, request_id, host_id),
                        head_only,
                    ));
                }
                Err(RouteError::Invalid) => {
                    metrics.finish_predispatch(OutcomeClass::ClientError, elapsed_ns(started));
                    return Ok(error_response(
                        &S3Error::new(S3ErrorCode::InvalidRequest, resource, request_id, host_id),
                        head_only,
                    ));
                }
            };
            let operation = route.operation;
            defer_body_provider(operation, body_receive_provider_factory, &mut request);
            let request_bytes = request
                .headers()
                .get(hyper::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let operation_started = Instant::now();
            let operation_span = request_span(&request_id, operation);
            let mut response = operations
                .execute(route, request, request_id.clone(), host_id.clone())
                .instrument(operation_span)
                .await;
            let operation_latency_ns = elapsed_ns(operation_started);
            if let Ok(value) = hyper::header::HeaderValue::from_str(&request_id) {
                response.headers_mut().insert("x-amz-request-id", value);
            }
            if let Ok(value) = hyper::header::HeaderValue::from_str(&host_id) {
                response.headers_mut().insert("x-amz-id-2", value);
            }
            let outcome = classify_outcome(response.status());
            let response_bytes = response_bytes(&response, head_only);
            metrics.finish_request(
                operation,
                outcome,
                RequestMeasurement {
                    request_bytes,
                    response_bytes,
                    latency_ns: elapsed_ns(started),
                    authentication_latency_ns,
                    operation_latency_ns,
                },
            );
            let (parts, body) = response.into_parts();
            Ok(hyper::Response::from_parts(
                parts,
                measured_body(body, Arc::clone(&metrics), operation, outcome, started),
            ))
        })
    }
}

fn auth_error_outcome(error: AuthError) -> (S3ErrorCode, OutcomeClass) {
    match error {
        AuthError::Rejected => (S3ErrorCode::AccessDenied, OutcomeClass::ClientError),
        AuthError::Unavailable => (S3ErrorCode::ServiceUnavailable, OutcomeClass::Unavailable),
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn classify_outcome(status: StatusCode) -> OutcomeClass {
    match status.as_u16() {
        200..=299 => OutcomeClass::Success,
        408 | 504 => OutcomeClass::Timeout,
        429 => OutcomeClass::Throttled,
        400..=499 => OutcomeClass::ClientError,
        503 => OutcomeClass::Unavailable,
        _ => OutcomeClass::Internal,
    }
}

fn defer_body_provider(
    operation: crowdb_access_s3::route::S3Operation,
    factory: Option<Arc<dyn Fn() -> DeferredBodyReceiveProvider + Send + Sync>>,
    request: &mut Request<Incoming>,
) {
    if operation == crowdb_access_s3::route::S3Operation::PutObject {
        if let Some(factory) = factory {
            request.extensions_mut().insert(factory());
        }
    }
}

fn response_bytes(response: &hyper::Response<super::ResponseBody>, head_only: bool) -> u64 {
    if head_only {
        return 0;
    }
    response
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .or_else(|| hyper::body::Body::size_hint(response.body()).exact())
        .unwrap_or(0)
}

fn request_span(request_id: &str, operation: crowdb_access_s3::route::S3Operation) -> tracing::Span {
    tracing::debug_span!("s3_request", %request_id, ?operation)
}
