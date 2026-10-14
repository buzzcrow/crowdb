//! Dataset HTTP listener. Framing and authentication live here; read
//! semantics are delegated to `crowdb-access-dataset`.

mod api;
mod legacy;
mod models;

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use crowdb_access_dataset::{
    AuthorityError, ChunkKvDatasetStore, DatasetAuthority, DatasetReadService, DatasetSnapshotRequest,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig as ChunkKvConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct DatasetHttpService {
    reads: Arc<DatasetReadService>,
    bearer: Option<Arc<str>>,
    request_timeout: Duration,
    api_admission: Arc<api::ApiAdmission>,
}

impl DatasetHttpService {
    #[must_use]
    pub fn new(reads: Arc<DatasetReadService>, request_timeout: Duration) -> Self {
        Self {
            reads,
            bearer: None,
            request_timeout,
            api_admission: Arc::new(api::ApiAdmission::new(3)),
        }
    }

    /// Protects every Dataset route with one bearer token, including
    /// publication, reads, retention, reclamation, and cancellation.
    #[must_use]
    pub fn with_bearer(mut self, token: impl Into<Arc<str>>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    /// Configures JSON control admission. Each request reserves an 8-MiB
    /// input and 8-MiB output window; zero disables new API admission.
    #[must_use]
    pub fn with_api_capacity(mut self, max_in_flight: usize) -> Self {
        self.api_admission = Arc::new(api::ApiAdmission::new(max_in_flight));
        self
    }
}

fn status_for_authority(error: &AuthorityError) -> StatusCode {
    match error {
        AuthorityError::Invalid(_) => StatusCode::BAD_REQUEST,
        AuthorityError::AlreadyExists
        | AuthorityError::ParentConflict
        | AuthorityError::HeadConflict
        | AuthorityError::CursorConflict
        | AuthorityError::SnapshotProtected => StatusCode::CONFLICT,
        AuthorityError::NotFound | AuthorityError::SnapshotNotFound => StatusCode::NOT_FOUND,
        AuthorityError::AncestryCycle | AuthorityError::Corrupt | AuthorityError::Store(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn decode_snapshot_request(
    request: Request<Incoming>,
    timeout: Duration,
) -> Option<DatasetSnapshotRequest> {
    let body = tokio::time::timeout(timeout, request.into_body().collect())
        .await
        .ok()?
        .ok()?
        .to_bytes();
    bincode::deserialize(&body).ok()
}

/// Runs Dataset as a standalone Access Server command.
///
/// # Errors
/// Returns configuration, Chunk-KV discovery, authentication, or listener
/// failures.
pub async fn run(arguments: Vec<String>) -> Result<(), BoxError> {
    run_with_shutdown(arguments, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

/// Runs Dataset with a caller-owned shutdown signal for combined processes.
///
/// # Errors
/// Returns configuration, Chunk-KV discovery, authentication, or listener
/// failures.
pub async fn run_with_shutdown(
    arguments: Vec<String>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), BoxError> {
    let (access, arguments) = crate::config::load_args(arguments)?;
    if !arguments.is_empty() && arguments != ["serve"] {
        return Err("unexpected Dataset server arguments".into());
    }
    let address = access
        .dataset
        .listen
        .clone()
        .or_else(|| std::env::var("CROWDB_DATASET_LISTEN").ok())
        .unwrap_or_else(|| "127.0.0.1:9093".into());
    let _: std::net::SocketAddr = address.parse()?;
    let seeds = if access.common.management_seeds.is_empty() {
        std::env::var("CROWDB_MANAGEMENT_SEEDS")?
            .split(',')
            .map(str::trim)
            .filter(|seed| !seed.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>()
    } else {
        access.common.management_seeds.clone()
    };
    if seeds.is_empty() {
        return Err("one or more management seeds are required".into());
    }
    let control = Arc::new(CrowdbKvClient::new(KvConfig::new(seeds)));
    let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::clone(&control)));
    let chunk_config = ChunkKvConfig::default();
    let transport = Arc::new(ChunkKvRpcTransport::new(chunk_config.max_owner_connections, 1, 2));
    let client = Arc::new(ChunkKvClient::new(chunk_config, source, transport)?);
    client.refresh_catalog().await?;
    let store = Arc::new(ChunkKvDatasetStore::new(client));
    let authority = Arc::new(DatasetAuthority::new(store));
    let identity = crowdb_access_dataset::DatasetIdentity::new(
        crowdb_access_dataset::NamespacePath::root(),
        std::env::var("CROWDB_DATASET_NAME").unwrap_or_else(|_| "default".into()),
    )?;
    let reads = Arc::new(DatasetReadService::with_ttl(
        authority,
        identity,
        access.dataset.max_in_flight,
        unix_seconds(),
        access.dataset.lease_ttl_seconds,
    )?);
    let mut service =
        DatasetHttpService::new(reads, Duration::from_secs(access.dataset.request_timeout_seconds))
            .with_api_capacity(access.dataset.max_in_flight);
    if let Ok(token) = std::env::var("CROWDB_DATASET_READ_TOKEN") {
        if token.is_empty() {
            return Err("CROWDB_DATASET_READ_TOKEN must not be empty".into());
        }
        service = service.with_bearer(Arc::<str>::from(token));
    }
    let listener = TcpListener::bind(address).await?;
    serve(listener, Arc::new(service), shutdown).await?;
    Ok(())
}

/// Runs the Dataset listener until shutdown resolves.
///
/// # Errors
/// Returns listener or connection acceptance failures.
pub async fn serve(
    listener: TcpListener,
    service: Arc<DatasetHttpService>,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept(), if connections.len() < 128 => {
                let (stream, _) = accepted?;
                let service = Arc::clone(&service);
                connections.spawn(async move {
                    let handler = service_fn(move |request| {
                        let service = Arc::clone(&service);
                        async move { Ok::<_, Infallible>(service.handle(request).await) }
                    });
                    let _ = http1::Builder::new().keep_alive(true)
                        .timer(TokioTimer::new())
                        .header_read_timeout(Duration::from_secs(30))
                        .serve_connection(TokioIo::new(stream), handler).await;
                });
            }
        }
    }
    drop(listener);
    if tokio::time::timeout(Duration::from_secs(10), async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    Ok(())
}

fn response(status: StatusCode, body: Vec<u8>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn cancelled_status() -> StatusCode {
    StatusCode::from_u16(499).expect("499 is a valid HTTP status")
}

fn encode<T: serde::Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
    match bincode::serialize(value) {
        Ok(body) => response(status, body),
        Err(_) => response(
            StatusCode::INTERNAL_SERVER_ERROR,
            b"response encoding failed".to_vec(),
        ),
    }
}

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_secs())
}
