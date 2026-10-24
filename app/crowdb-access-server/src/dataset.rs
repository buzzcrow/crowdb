//! Dataset HTTP listener. Framing and authentication live here; read
//! semantics are delegated to `crowdb-access-dataset`.

mod api;
mod legacy;
mod models;

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use crowdb_access_dataset::{
    AuthorityError, ChunkKvDatasetStore, ChunkPayloadStorage, DatasetAuthority, DatasetReadService,
    DatasetSnapshotRequest, DatasetUploads, UploadExpiryCursor, UploadedChunkReader,
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
type DatasetBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;

fn body(bytes: Bytes) -> DatasetBody {
    Full::new(bytes).map_err(|never| match never {}).boxed_unsync()
}

pub struct DatasetHttpService {
    reads: Arc<DatasetReadService>,
    bearer: Option<Arc<str>>,
    request_timeout: Duration,
    api_admission: Arc<api::ApiAdmission>,
    uploads: Option<Arc<DatasetUploads>>,
}

impl DatasetHttpService {
    async fn handle(&self, request: Request<Incoming>) -> Response<DatasetBody> {
        if request.uri().path().starts_with("/api/dataset/") {
            self.handle_api(request).await
        } else {
            self.handle_legacy(request)
                .await
                .map(|full| full.map_err(|never| match never {}).boxed_unsync())
        }
    }
    #[must_use]
    pub fn new(reads: Arc<DatasetReadService>, request_timeout: Duration) -> Self {
        Self {
            reads,
            bearer: None,
            request_timeout,
            api_admission: Arc::new(api::ApiAdmission::new(3)),
            uploads: None,
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

    #[must_use]
    pub fn with_uploads(mut self, uploads: Arc<DatasetUploads>) -> Self {
        self.uploads = Some(uploads);
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
        | AuthorityError::ReclaimPending { .. }
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
    let control = Arc::new(CrowdbKvClient::new(KvConfig::new(seeds.clone())));
    let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::clone(&control)));
    let chunk_config = ChunkKvConfig::default();
    let transport = Arc::new(ChunkKvRpcTransport::with_config(
        chunk_config.max_owner_connections,
        1,
        2,
        &access.common.chunk_kv_rpc,
    )?);
    let client = Arc::new(ChunkKvClient::new(chunk_config, source, transport)?);
    client.refresh_catalog().await?;
    let store = Arc::new(ChunkKvDatasetStore::new(client));
    let chunks = connect_payloads(
        seeds,
        control,
        access.dataset.max_in_flight,
        access.dataset_small_write(),
    )
    .await?;
    let uploads = Arc::new(DatasetUploads::new(
        store.clone(),
        Arc::new(ChunkPayloadStorage::new(chunks.clone())),
    ));
    let authority = Arc::new(DatasetAuthority::with_chunk_reader(
        store,
        Arc::new(UploadedChunkReader::new((*uploads).clone())),
    ));
    let identity = crowdb_access_dataset::DatasetIdentity::new(
        crowdb_access_dataset::NamespacePath::root(),
        std::env::var("CROWDB_DATASET_NAME").unwrap_or_else(|_| "default".into()),
    )?;
    let reads = Arc::new(DatasetReadService::new(
        Arc::clone(&authority),
        identity,
        access.dataset.max_in_flight,
    )?);
    let mut service =
        DatasetHttpService::new(reads, Duration::from_secs(access.dataset.request_timeout_seconds))
            .with_api_capacity(access.dataset.max_in_flight)
            .with_uploads(Arc::clone(&uploads));
    let bearer = std::env::var("CROWDB_DATASET_READ_TOKEN").ok().or_else(|| {
        std::env::var("CROWDB_S3_MASTER_KEY")
            .ok()
            .filter(|key| !key.is_empty())
            .map(|key| crowdb_protocol::dataset_auth::deployment_bearer(&key))
    });
    if let Some(token) = bearer {
        if token.is_empty() {
            return Err("CROWDB_DATASET_READ_TOKEN must not be empty".into());
        }
        service = service.with_bearer(Arc::<str>::from(token));
    }
    let listener = TcpListener::bind(address).await?;
    let expiry = start_cleanup(authority, uploads);
    let result = serve(listener, Arc::new(service), shutdown).await;
    expiry.abort();
    let _ = expiry.await;
    chunks.shutdown_small_writes().await?;
    result?;
    Ok(())
}

fn start_cleanup(
    authority: Arc<DatasetAuthority>,
    uploads: Arc<DatasetUploads>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(15));
        let mut after = None;
        let mut upload_cursor = UploadExpiryCursor::default();
        let mut next_upload = tokio::time::Instant::now();
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if let Err(error) = authority.reclaim_due_page(&uploads, &mut after,
                        &crowdb_access_dataset::ReadCancellation::new()).await {
                        if matches!(error, crowdb_access_dataset::SnapshotReclaimError::Limit) {
                            tracing::debug!("Dataset snapshot cleanup yielded at its work deadline");
                        } else {
                            tracing::error!(%error, "Dataset snapshot cleanup failed; durable deletion marks retained for retry");
                        }
                    }
                }
                () = tokio::time::sleep_until(next_upload) => {
                    let result = uploads.expire_step(unix_seconds(), &mut upload_cursor).await;
                    let delay = if upload_cursor.has_more() {
                        if result.is_ok() { Duration::from_millis(50) } else { Duration::from_millis(500) }
                    } else { Duration::from_secs(15) };
                    next_upload = tokio::time::Instant::now() + delay;
                    if let Err(error) = result {
                        tracing::error!(%error, "Dataset upload cleanup failed; retaining cleanup records for retry");
                    }
                }
            }
        }
    })
}

async fn connect_payloads(
    seeds: Vec<String>,
    control: Arc<CrowdbKvClient>,
    capacity: usize,
    mut small_write: crowdb_chunk_client::SmallWritePolicy,
) -> Result<crowdb_chunk_client::ChunkIoClient, BoxError> {
    use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig};
    small_write.object_limit = 4 * 1024 * 1024;
    small_write.max_batch_bytes = 4 * 1024 * 1024;
    small_write.max_pipelines = capacity.clamp(1, 8);
    small_write.memory_budget = 128 * 1024 * 1024;
    Ok(ChunkIoClient::connect_with_kv(
        ChunkIoClientConfig {
            management_seeds: seeds,
            diskio_connections_per_endpoint: 4,
            diskio_rpc_workers: 2,
            small_write,
        },
        control,
    )
    .await?)
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
