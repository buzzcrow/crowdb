// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use crowdb_access_s3::error::S3Error;
use crowdb_access_s3::metrics::{OutcomeClass, S3Metrics};
use crowdb_access_s3::native_buffer::NativeBodyReceiver;
use crowdb_access_s3::route::S3Operation;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Bytes, Frame, Http1BodyReceiveProvider, Incoming, SizeHint};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

mod dispatcher;
mod operations;

pub use dispatcher::S3Dispatcher;
pub use operations::{ProductionS3Operations, S3Operations, S3OperationsFuture, S3ServiceConfig};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseBody = http_body_util::combinators::BoxBody<Bytes, BoxError>;
pub type HandlerFuture =
    Pin<Box<dyn Future<Output = Result<Response<ResponseBody>, Infallible>> + Send + 'static>>;

pub trait S3HttpHandler: Send + Sync + 'static {
    fn handle(&self, request: Request<Incoming>) -> HandlerFuture;
}

#[derive(Clone)]
pub(crate) struct DeferredBodyReceiveProvider {
    provider: Arc<dyn Http1BodyReceiveProvider>,
    native: Option<Arc<NativeBodyReceiver>>,
}

impl DeferredBodyReceiveProvider {
    fn generic(provider: Arc<dyn Http1BodyReceiveProvider>) -> Self {
        Self {
            provider,
            native: None,
        }
    }

    fn native(receiver: Arc<NativeBodyReceiver>) -> Self {
        Self {
            provider: receiver.clone(),
            native: Some(receiver),
        }
    }
}

/// Installs the admitted request's provider immediately before body polling.
pub fn install_body_receive_provider(request: &mut Request<Incoming>) -> Option<Arc<NativeBodyReceiver>> {
    if let Some(deferred) = request.extensions_mut().remove::<DeferredBodyReceiveProvider>() {
        request
            .body_mut()
            .set_http1_body_receive_provider(deferred.provider);
        return deferred.native;
    }
    None
}

/// Runs one independent HTTP/1 S3 listener until shutdown.
///
/// # Errors
///
/// Returns listener accept errors. Per-connection protocol failures are logged
/// and do not terminate admission for unrelated connections.
pub async fn serve(
    listener: TcpListener,
    handler: Arc<dyn S3HttpHandler>,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    loop {
        let accepted = tokio::select! {
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => accepted,
        };
        let (stream, peer) = accepted?;
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            let service = service_fn(move |request| handler.handle(request));
            if let Err(error) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(%peer, %error, "S3 HTTP connection ended with an error");
            }
        });
    }
}

pub(crate) fn error_response(error: &S3Error, head_only: bool) -> Response<ResponseBody> {
    let body = if head_only {
        Bytes::new()
    } else {
        Bytes::from(error.to_xml())
    };
    let mut response = Response::builder()
        .status(error.status_code())
        .header(hyper::header::CONTENT_TYPE, error.content_type())
        .body(full_body(body))
        .expect("static response is valid");
    if let Ok(value) = hyper::header::HeaderValue::from_str(error.request_id()) {
        response.headers_mut().insert("x-amz-request-id", value);
    }
    if let Ok(value) = hyper::header::HeaderValue::from_str(error.host_id()) {
        response.headers_mut().insert("x-amz-id-2", value);
    }
    if let Some(seconds) = error.retry_after_seconds() {
        response
            .headers_mut()
            .insert(hyper::header::RETRY_AFTER, seconds.into());
    }
    response
}

pub(crate) fn full_body(value: Bytes) -> ResponseBody {
    Full::new(value).map_err(|never| match never {}).boxed()
}

pub(crate) fn measured_body(
    body: ResponseBody,
    metrics: Arc<S3Metrics>,
    operation: S3Operation,
    outcome: OutcomeClass,
    started: Instant,
) -> ResponseBody {
    if body.size_hint().exact() == Some(0) {
        metrics.record_time_to_first_byte(operation, outcome, elapsed_ns(started));
        return body;
    }
    MeasuredBody {
        inner: body,
        metrics,
        operation,
        outcome,
        started,
        recorded: false,
    }
    .boxed()
}

struct MeasuredBody {
    inner: ResponseBody,
    metrics: Arc<S3Metrics>,
    operation: S3Operation,
    outcome: OutcomeClass,
    started: Instant,
    recorded: bool,
}

impl Body for MeasuredBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(context);
        if result.is_ready() && !self.recorded {
            self.recorded = true;
            self.metrics
                .record_time_to_first_byte(self.operation, self.outcome, elapsed_ns(self.started));
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
