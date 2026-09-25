use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use super::body::IcebergBody;
use super::connection::{ActiveIo, ConnectionActivity};
use super::file_http::FileHttp;
use super::metrics::{self, IcebergMetrics, IcebergMetricsSnapshot, RequestObservation};
use super::namespace_read::NamespaceHttp;
use super::routes::{InstalledRoutes, Route};
use crowdb_access_iceberg::catalog::{
    Capabilities, CatalogError, CatalogLifecycle, CatalogRepository, ManagementPrivilege, RootState,
};
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
    files: Option<Arc<FileHttp>>,
    tables: Option<super::table_read::TableHttp>,
    table_writes: Option<super::table_write::TableWrites>,
    table_credentials: Option<super::table_credentials::TableCredentials>,
    metrics: Arc<IcebergMetrics>,
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
            files: None,
            tables: None,
            table_writes: None,
            table_credentials: None,
            metrics: Arc::new(IcebergMetrics::default()),
        }
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> IcebergMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// # Errors
    /// Rejects invalid native file listener limits or signing configuration.
    pub fn with_fileio<Store: crowdb_access_iceberg::file::MultipartPartStore + 'static>(
        mut self,
        store: Arc<Store>,
        blocks: Arc<dyn crowdb_access_iceberg::file::FileBlockStore>,
        region: String,
    ) -> Result<Self, crowdb_access_iceberg::file::FileGrantError> {
        self.files = Some(Arc::new(FileHttp::new(
            store,
            blocks,
            self.authentication.namespace_token_key(),
            region,
        )?));
        Ok(self)
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

    /// Installs and advertises only generation-qualified reads for fixture-backed tests.
    /// Runtime activation awaits complete commit validation and credential vending.
    /// # Errors
    /// Rejects invalid table-list token signing configuration.
    #[cfg(feature = "test-util")]
    pub fn with_table_reads_for_tests<Store: crowdb_access_iceberg::namespace::NamespaceStore + 'static>(
        mut self,
        store: Arc<Store>,
        blocks: Arc<dyn crowdb_access_iceberg::file::FileBlockStore>,
    ) -> Result<Self, crowdb_access_iceberg::error::ValidationError> {
        self.tables = Some(super::table_read::TableHttp::new(
            store,
            blocks,
            &self.authentication.namespace_token_key(),
        )?);
        Ok(self)
    }

    /// # Errors
    /// Rejects invalid table token configuration.
    pub fn with_tables<Store: crowdb_access_iceberg::namespace::NamespaceStore + 'static>(
        mut self,
        store: Arc<Store>,
        blocks: Arc<dyn crowdb_access_iceberg::file::FileBlockStore>,
    ) -> Result<Self, crowdb_access_iceberg::error::ValidationError> {
        self.tables = Some(super::table_read::TableHttp::new(
            store.clone(),
            blocks.clone(),
            &self.authentication.namespace_token_key(),
        )?);
        self.table_writes = Some(super::table_write::TableWrites::new(store, blocks));
        Ok(self)
    }

    /// # Errors
    /// Requires installed table access and a configured external HTTP/S origin.
    pub fn with_table_credentials<Store: crowdb_access_iceberg::namespace::NamespaceStore + 'static>(
        mut self,
        store: Arc<Store>,
        endpoint: String,
    ) -> Result<Self, crowdb_access_iceberg::error::ValidationError> {
        let config = super::table_credentials::TableFileConfig::new(endpoint)?;
        self.tables
            .as_mut()
            .ok_or(crowdb_access_iceberg::error::ValidationError::Record)?
            .file_config = Some(config.clone());
        self.table_writes
            .as_mut()
            .ok_or(crowdb_access_iceberg::error::ValidationError::Record)?
            .file_config = Some(config);
        self.table_credentials = Some(
            super::table_credentials::TableCredentials::new(store, self.authentication.namespace_token_key())
                .map_err(|_| crowdb_access_iceberg::error::ValidationError::Record)?,
        );
        Ok(self)
    }

    async fn handle(
        &self,
        request: Request<Incoming>,
        deadline: tokio::time::Instant,
    ) -> Result<Response<IcebergBody>, Infallible> {
        let observation = RequestObservation::new(
            self.metrics.clone(),
            metrics::route_index(request.method(), request.uri().path()),
        );
        let head = request.method() == hyper::Method::HEAD;
        let result = Box::pin(tokio::time::timeout_at(
            deadline,
            metrics::observe(observation.clone(), self.dispatch(request)),
        ))
        .await;
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
        observation.dispatched(response.status().as_u16());
        let body = std::mem::replace(response.body_mut(), IcebergBody::new(Vec::new()));
        *response.body_mut() = body.with_observation(observation);
        Ok(response)
    }

    async fn dispatch(
        &self,
        request: Request<Incoming>,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        if request.uri().path().starts_with("/iceberg-") {
            return Ok(match &self.files {
                Some(files) => {
                    Box::pin(files.dispatch(&self.repository, request, self.request_timeout)).await
                }
                None => super::file_http::unavailable(request.uri().path()),
            });
        }
        let mut authorizations = request.headers().get_all(hyper::header::AUTHORIZATION).iter();
        let authorization = match (authorizations.next(), authorizations.next()) {
            (Some(value), None) => value.to_str().unwrap_or_default(),
            _ => "",
        };
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
        let route = Route::classify(request.method(), request.uri().path())
            .filter(|route| route.enabled(&self.installed_routes()))
            .ok_or_else(super::table_read::unsupported)?;
        if route == Route::AdminMetrics {
            if principal.management != ManagementPrivilege::Manage {
                return Err(IcebergErrorResponse::new(
                    403,
                    "ForbiddenException",
                    "Management privilege is required",
                ));
            }
            return Ok(response(
                200,
                serde_json::to_vec(&self.metrics.snapshot()).map_err(|_| service_unavailable())?,
            ));
        }
        let (root, authority) = self
            .repository
            .status()
            .await
            .map_err(|_| service_unavailable())?;
        if root.state != RootState::Ready
            || authority.lifecycle != CatalogLifecycle::Ready
            || self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_millis(authority.admission_bounds.request_ms)
        {
            return Err(service_unavailable());
        }
        if route == Route::Config {
            if authority.capabilities.bits() == 0 {
                return Err(service_unavailable());
            }
            return self.config(request.uri().query(), authority.capabilities);
        }
        if !route.supported(authority.capabilities) {
            return Err(super::table_read::unsupported());
        }
        match route {
            Route::TableCredentials => {
                self.table_credentials
                    .as_ref()
                    .ok_or_else(super::table_read::unsupported)?
                    .load(&self.repository, root.context, principal, &request)
                    .await
            }
            Route::TableCreate | Route::TableUpdate | Route::TableDrop | Route::TableRename => {
                let writes = self
                    .table_writes
                    .as_ref()
                    .ok_or_else(super::table_read::unsupported)?;
                Box::pin(writes.execute(root.context, authority.capabilities, principal, request)).await
            }
            Route::TableList | Route::TableLoad | Route::TableExists => {
                self.tables
                    .as_ref()
                    .ok_or_else(super::table_read::unsupported)?
                    .read(root.context, authority.capabilities, &request)
                    .await
            }
            _ => {
                self.namespaces
                    .as_ref()
                    .ok_or_else(super::table_read::unsupported)?
                    .dispatch(root.context, principal, request)
                    .await
            }
        }
    }

    fn installed_routes(&self) -> InstalledRoutes {
        let mut bits = 0;
        if self.namespaces.is_some() {
            bits |= InstalledRoutes::NAMESPACES;
        }
        if self.tables.is_some() {
            bits |= InstalledRoutes::TABLES;
        }
        if self.table_writes.is_some() {
            bits |= InstalledRoutes::WRITES;
        }
        if self.table_credentials.is_some() {
            bits |= InstalledRoutes::CREDENTIALS;
        }
        InstalledRoutes(bits)
    }

    fn config(
        &self,
        query: Option<&str>,
        capabilities: Capabilities,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        let warehouse = warehouse(query)?;
        let mut config = CatalogConfig::for_capabilities(warehouse.as_deref(), capabilities)?;
        config.endpoints = Route::endpoints(&self.installed_routes(), capabilities);
        if self.namespaces.is_some() {
            config.idempotency_key_lifetime = Some("PT24H".into());
        }
        Ok(response(
            200,
            serde_json::to_vec(&config).map_err(|_| service_unavailable())?,
        ))
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
                    let lifetime = service.request_timeout;
                    let activity = ConnectionActivity::new();
                    let deadline = activity.dispatch_deadline(lifetime);
                    let stream = ActiveIo::new(stream, activity.clone());
                    let handler = service_fn(move |request| { let service = Arc::clone(&service); async move { Box::pin(service.handle(request, deadline)).await } });
                    let connection = http1::Builder::new().keep_alive(false).max_buf_size(64 * 1024)
                        .serve_connection(TokioIo::new(stream), handler);
                    tokio::select! {
                        result = connection => {
                            if let Err(error) = result {
                                tracing::debug!(%peer, %error, "Iceberg HTTP connection failed");
                            }
                        }
                        () = activity.expired(Duration::from_secs(300), lifetime) => {
                            tracing::debug!(%peer, "Iceberg HTTP connection lifetime or idle deadline exhausted");
                        }
                    }
                });
            }
        }
    }
    drop(listener);
    if tokio::time::timeout(Duration::from_secs(300), async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
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
