// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crowdb_access_s3::error::S3Error;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Http1BodyReceiveProvider, Incoming};
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
pub(crate) struct DeferredBodyReceiveProvider(pub Arc<dyn Http1BodyReceiveProvider>);

/// Installs the admitted request's provider immediately before body polling.
pub fn install_body_receive_provider(request: &mut Request<Incoming>) {
    if let Some(deferred) = request.extensions_mut().remove::<DeferredBodyReceiveProvider>() {
        request.body_mut().set_http1_body_receive_provider(deferred.0);
    }
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
