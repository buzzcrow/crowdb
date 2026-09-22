use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use super::body::IcebergBody;
use super::namespace_read::NamespaceHttp;
use crowdb_access_iceberg::catalog::{CatalogError, CatalogLifecycle, CatalogRepository, RootState};
use crowdb_access_iceberg::wire::{BearerAuthenticator, CatalogConfig, IcebergErrorResponse};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

pub struct IcebergHttpService {
    repository: Arc<CatalogRepository>,
    authentication: BearerAuthenticator,
    request_timeout: Duration,
    namespaces: Option<NamespaceHttp>,
}

impl IcebergHttpService {
    #[must_use]
    pub fn new(
        repository: Arc<CatalogRepository>,
        authentication: BearerAuthenticator,
        request_timeout: Duration,
    ) -> Self {
        Self {
            repository,
            authentication,
            request_timeout,
            namespaces: None,
        }
    }

    /// # Errors
    /// Rejects invalid namespace token signing configuration.
    pub fn with_namespaces<Store: crowdb_access_iceberg::namespace::NamespaceStore + 'static>(
        mut self,
        store: Arc<Store>,
    ) -> Result<Self, crowdb_access_iceberg::error::ValidationError> {
        self.namespaces = Some(NamespaceHttp::new(
            store,
            &self.authentication.namespace_token_key(),
        )?);
        Ok(self)
    }

    async fn handle(&self, request: Request<Incoming>) -> Result<Response<IcebergBody>, Infallible> {
        let head = request.method() == hyper::Method::HEAD;
        let result = tokio::time::timeout(self.request_timeout, self.dispatch(request)).await;
        let mut response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => response(error.error.code, serde_json::to_vec(&error).unwrap_or_default()),
            Err(_) => {
                tracing::warn!("Iceberg request deadline exhausted; durable recovery remains active");
                unavailable()
            }
        };
        if head {
            *response.body_mut() = IcebergBody::new(Vec::new());
        }
        Ok(response)
    }

    async fn dispatch(
        &self,
        request: Request<Incoming>,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        let authorization = request
            .headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let Some(principal) = self.authentication.authenticate(authorization) else {
            return Err(IcebergErrorResponse::new(
                401,
                "NotAuthorizedException",
                "Valid bearer authentication is required",
            ));
        };
        if request.uri().to_string().len() > 32 * 1024 {
            return Err(bad_request());
        }
        if request.uri().path() != "/v1/config" && self.namespaces.is_none() {
            return Err(IcebergErrorResponse::new(
                406,
                "UnsupportedOperationException",
                "This endpoint is not implemented",
            ));
        }
        let (root, authority) = self
            .repository
            .status()
            .await
            .map_err(|_| service_unavailable())?;
        if root.state != RootState::Ready
            || authority.lifecycle != CatalogLifecycle::Ready
            || self.request_timeout.as_millis() > u128::from(authority.admission_bounds.request_ms)
            || authority.capabilities.bits() != 0
        {
            return Err(service_unavailable());
        }
        if request.method() == hyper::Method::GET && request.uri().path() == "/v1/config" {
            let warehouse = warehouse(request.uri().query())?;
            let mut config = CatalogConfig::foundation(warehouse.as_deref())?;
            if self.namespaces.is_some() {
                config.endpoints = [
                    "GET /v1/{prefix}/namespaces",
                    "GET /v1/{prefix}/namespaces/{namespace}",
                    "HEAD /v1/{prefix}/namespaces/{namespace}",
                    "POST /v1/{prefix}/namespaces",
                    "POST /v1/{prefix}/namespaces/{namespace}/properties",
                    "DELETE /v1/{prefix}/namespaces/{namespace}",
                ]
                .map(str::to_owned)
                .to_vec();
                config.idempotency_key_lifetime = Some("PT24H".into());
            }
            return Ok(response(
                200,
                serde_json::to_vec(&config).map_err(|_| service_unavailable())?,
            ));
        }
        match &self.namespaces {
            Some(namespaces) => namespaces.dispatch(root.context, principal, request).await,
            None => Err(IcebergErrorResponse::new(
                406,
                "UnsupportedOperationException",
                "This endpoint is not implemented",
            )),
        }
    }
}

/// # Errors
/// Returns listener failures after stopping admission and draining connections.
pub async fn serve(
    listener: TcpListener,
    service: Arc<IcebergHttpService>,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    let recovery = reconcile(&service.repository);
    tokio::pin!(recovery);
    let mut connections = JoinSet::new();
    let mut failure = None;
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            () = &mut recovery => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept(), if connections.len() < 128 => {
                let (stream, peer) = match accepted { Ok(value) => value, Err(error) => { failure = Some(error); break; } };
                let service = Arc::clone(&service);
                connections.spawn(async move {
                    let timeout = service.request_timeout;
                    let handler = service_fn(move |request| { let service = Arc::clone(&service); async move { service.handle(request).await } });
                    let connection = http1::Builder::new().keep_alive(false).max_buf_size(64 * 1024)
                        .serve_connection(TokioIo::new(stream), handler);
                    if let Ok(Err(error)) = tokio::time::timeout(timeout, connection).await {
                        tracing::debug!(%peer, %error, "Iceberg HTTP connection failed");
                    }
                });
            }
        }
    }
    drop(listener);
    while connections.join_next().await.is_some() {}
    failure.map_or(Ok(()), Err)
}

async fn reconcile(repository: &CatalogRepository) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let Ok(elapsed) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
            tracing::error!("Iceberg recovery paused: system clock precedes Unix epoch");
            continue;
        };
        let Ok(now_ms) = u64::try_from(elapsed.as_millis()) else {
            tracing::error!("Iceberg recovery paused: system clock exceeds supported range");
            continue;
        };
        match repository.recover(now_ms).await {
            Ok(()) | Err(CatalogError::Busy) => {}
            Err(error) => tracing::error!(%error, "Iceberg recovery failed; retrying on next interval"),
        }
    }
}

fn warehouse(query: Option<&str>) -> Result<Option<String>, IcebergErrorResponse> {
    let mut warehouse = None;
    for pair in query
        .unwrap_or_default()
        .split('&')
        .filter(|value| !value.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let name = decode_query(name)?;
        if name == "warehouse" {
            if warehouse.is_some() {
                return Err(bad_request());
            }
            warehouse = Some(decode_query(value)?);
        }
    }
    Ok(warehouse)
}

pub(super) fn decode_query(value: &str) -> Result<String, IcebergErrorResponse> {
    for (index, byte) in value.bytes().enumerate() {
        if byte == b'%'
            && !value
                .as_bytes()
                .get(index + 1..index + 3)
                .is_some_and(|bytes| bytes.iter().all(u8::is_ascii_hexdigit))
        {
            return Err(bad_request());
        }
    }
    let value = value.replace('+', " ");
    percent_encoding::percent_decode_str(&value)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| bad_request())
}

pub(super) fn response(status: u16, bytes: Vec<u8>) -> Response<IcebergBody> {
    let mut response = Response::new(IcebergBody::new(bytes));
    *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    if status == 401 {
        response.headers_mut().insert(
            hyper::header::WWW_AUTHENTICATE,
            hyper::header::HeaderValue::from_static("Bearer"),
        );
    }
    response
}

pub(super) fn bad_request() -> IcebergErrorResponse {
    IcebergErrorResponse::new(400, "BadRequestException", "Invalid request parameters")
}
pub(super) fn service_unavailable() -> IcebergErrorResponse {
    IcebergErrorResponse::new(503, "ServiceUnavailableException", "Catalog is not ready")
}
fn unavailable() -> Response<IcebergBody> {
    response(
        503,
        serde_json::to_vec(&service_unavailable()).unwrap_or_default(),
    )
}
