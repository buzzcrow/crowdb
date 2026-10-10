//! Dataset HTTP listener. Framing and authentication live here; read
//! semantics are delegated to `crowdb-access-dataset`.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use crowdb_access_dataset::{
    AuthorityError, ChunkKvDatasetStore, DatasetAuthority, DatasetManifestResponse, DatasetPublishRequest,
    DatasetPublishResponse, DatasetReadRequest, DatasetReadService, DatasetReclaimResponse,
    DatasetRetentionResponse, DatasetScanRequest, DatasetSnapshotRequest, DatasetSnapshotResponse,
    DatasetSnapshotsResponse, OperationId, ReadTransportError,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig as ChunkKvConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct DatasetHttpService {
    reads: Arc<DatasetReadService>,
    bearer: Option<Arc<str>>,
    request_timeout: Duration,
}

impl DatasetHttpService {
    #[must_use]
    pub fn new(reads: Arc<DatasetReadService>, request_timeout: Duration) -> Self {
        Self {
            reads,
            bearer: None,
            request_timeout,
        }
    }

    /// Protects every Dataset route with one bearer token, including
    /// publication, reads, retention, reclamation, and cancellation.
    #[must_use]
    pub fn with_bearer(mut self, token: impl Into<Arc<str>>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        if !self.authorized(&request) {
            return response(StatusCode::UNAUTHORIZED, b"unauthorized".to_vec());
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/read" {
            return self.read(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/scan" {
            return self.scan(request).await;
        }
        if request.method() == Method::POST
            && matches!(
                request.uri().path(),
                "/v1/dataset/start-read-plan"
                    | "/v1/dataset/next-batch"
                    | "/v1/dataset/save-progress"
                    | "/v1/dataset/resume"
            )
        {
            return self.scan(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/create" {
            return self.create().await;
        }
        if request.method() == Method::GET && request.uri().path() == "/v1/dataset/open" {
            return self.open().await;
        }
        if request.method() == Method::GET && request.uri().path() == "/v1/dataset/latest" {
            return self.latest().await;
        }
        if request.method() == Method::GET && request.uri().path() == "/v1/dataset/snapshots" {
            return self.snapshots().await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/publish" {
            return self.publish(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/prepare" {
            return self.publish(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/retain" {
            return self.retain(request, true).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/release" {
            return self.retain(request, false).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/snapshot" {
            return self.snapshot(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/manifest" {
            return self.manifest(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/stable" {
            return self.stable(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/reclaim" {
            return self.reclaim(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/reclaim/status" {
            return self.reclaim_status(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/cancel" {
            self.reads.cancel();
            return response(StatusCode::NO_CONTENT, Vec::new());
        }
        response(StatusCode::NOT_FOUND, b"not found".to_vec())
    }

    async fn open(&self) -> Response<Full<Bytes>> {
        match self.reads.authority().get_dataset(self.reads.identity()).await {
            Ok(record) => encode(StatusCode::OK, &record),
            Err(_) => response(StatusCode::NOT_FOUND, b"dataset not found".to_vec()),
        }
    }

    async fn create(&self) -> Response<Full<Bytes>> {
        match self
            .reads
            .authority()
            .create_dataset(self.reads.identity().clone())
            .await
        {
            Ok(id) => encode(StatusCode::OK, &id),
            Err(crowdb_access_dataset::AuthorityError::AlreadyExists) => {
                response(StatusCode::CONFLICT, b"dataset already exists".to_vec())
            }
            Err(_) => response(StatusCode::SERVICE_UNAVAILABLE, b"dataset unavailable".to_vec()),
        }
    }

    async fn latest(&self) -> Response<Full<Bytes>> {
        match self.reads.authority().latest(self.reads.identity()).await {
            Ok(snapshot) => encode(StatusCode::OK, &snapshot),
            Err(_) => response(StatusCode::SERVICE_UNAVAILABLE, b"dataset unavailable".to_vec()),
        }
    }

    async fn snapshots(&self) -> Response<Full<Bytes>> {
        match self.reads.authority().list_snapshots(self.reads.identity()).await {
            Ok(snapshots) => encode(StatusCode::OK, &DatasetSnapshotsResponse { snapshots }),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn publish(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let body = match tokio::time::timeout(self.request_timeout, request.into_body().collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::REQUEST_TIMEOUT, b"request timeout".to_vec()),
        };
        if body.len() > MAX_REQUEST_BYTES {
            return response(StatusCode::PAYLOAD_TOO_LARGE, b"request too large".to_vec());
        }
        let request: DatasetPublishRequest = match bincode::deserialize(&body) {
            Ok(request) => request,
            Err(_) => return response(StatusCode::BAD_REQUEST, b"invalid publication request".to_vec()),
        };
        if request.validate().is_err() {
            return response(StatusCode::BAD_REQUEST, b"invalid publication request".to_vec());
        }
        let operation = request.operation.unwrap_or_else(OperationId::random);
        match self
            .reads
            .authority()
            .publish_snapshot_with_operation(
                self.reads.identity(),
                request.parent,
                request.manifest,
                operation,
            )
            .await
        {
            Ok(snapshot) => encode(StatusCode::OK, &DatasetPublishResponse { snapshot }),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn retain(&self, request: Request<Incoming>, retain: bool) -> Response<Full<Bytes>> {
        let body = match tokio::time::timeout(self.request_timeout, request.into_body().collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::REQUEST_TIMEOUT, b"request timeout".to_vec()),
        };
        let request: DatasetSnapshotRequest = match bincode::deserialize(&body) {
            Ok(request) => request,
            Err(_) => return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec()),
        };
        let result = if retain {
            self.reads
                .authority()
                .retain_snapshot(self.reads.identity(), request.snapshot)
                .await
        } else {
            self.reads
                .authority()
                .release_snapshot(self.reads.identity(), request.snapshot)
                .await
        };
        match result {
            Ok(()) => encode(
                StatusCode::OK,
                &DatasetRetentionResponse {
                    snapshot: request.snapshot,
                    retained: retain,
                },
            ),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn snapshot(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .get_snapshot(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(snapshot) => encode(StatusCode::OK, &DatasetSnapshotResponse { snapshot }),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn manifest(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .get_manifest(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(manifest) => encode(StatusCode::OK, &DatasetManifestResponse { manifest }),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn stable(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .set_stable(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(()) => encode(
                StatusCode::OK,
                &DatasetRetentionResponse {
                    snapshot: request.snapshot,
                    retained: true,
                },
            ),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn reclaim(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .reclaim_snapshot_metadata(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(()) => encode(
                StatusCode::OK,
                &DatasetReclaimResponse {
                    snapshot: request.snapshot,
                    reclaimed: true,
                },
            ),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn reclaim_status(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .reclaim_progress(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(progress) => encode(StatusCode::OK, &progress),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    fn authorized(&self, request: &Request<Incoming>) -> bool {
        let Some(expected) = &self.bearer else { return true };
        request
            .headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|value| value.as_bytes() == expected.as_bytes())
    }

    async fn read(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let body = match tokio::time::timeout(self.request_timeout, request.into_body().collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::REQUEST_TIMEOUT, b"request timeout".to_vec()),
        };
        if body.len() > MAX_REQUEST_BYTES {
            return response(StatusCode::PAYLOAD_TOO_LARGE, b"request too large".to_vec());
        }
        let request: DatasetReadRequest = match bincode::deserialize(&body) {
            Ok(request) => request,
            Err(_) => return response(StatusCode::BAD_REQUEST, b"invalid dataset request".to_vec()),
        };
        match self.reads.read(request, unix_seconds()).await {
            Ok(result) => encode(StatusCode::OK, &result),
            Err(ReadTransportError::WindowFull) => {
                response(StatusCode::TOO_MANY_REQUESTS, b"read window full".to_vec())
            }
            Err(ReadTransportError::Invalid(crowdb_access_dataset::DatasetError::ReadCancelled)) => {
                response(cancelled_status(), b"dataset read cancelled".to_vec())
            }
            Err(ReadTransportError::Invalid(_)) => {
                response(StatusCode::BAD_REQUEST, b"invalid dataset request".to_vec())
            }
            Err(ReadTransportError::Authority(error)) => {
                response(status_for_authority(&error), error.to_string().into_bytes())
            }
        }
    }

    async fn scan(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let body = match tokio::time::timeout(self.request_timeout, request.into_body().collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::REQUEST_TIMEOUT, b"request timeout".to_vec()),
        };
        if body.len() > MAX_REQUEST_BYTES {
            return response(StatusCode::PAYLOAD_TOO_LARGE, b"request too large".to_vec());
        }
        let request: DatasetScanRequest = match bincode::deserialize(&body) {
            Ok(request) => request,
            Err(_) => return response(StatusCode::BAD_REQUEST, b"invalid scan request".to_vec()),
        };
        match self.reads.scan(request, unix_seconds()).await {
            Ok(result) => encode(StatusCode::OK, &result),
            Err(ReadTransportError::WindowFull) => {
                response(StatusCode::TOO_MANY_REQUESTS, b"read window full".to_vec())
            }
            Err(ReadTransportError::Invalid(crowdb_access_dataset::DatasetError::ReadCancelled)) => {
                response(cancelled_status(), b"dataset read cancelled".to_vec())
            }
            Err(ReadTransportError::Invalid(_)) => {
                response(StatusCode::BAD_REQUEST, b"invalid scan request".to_vec())
            }
            Err(ReadTransportError::Authority(error)) => {
                response(status_for_authority(&error), error.to_string().into_bytes())
            }
        }
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
        DatasetHttpService::new(reads, Duration::from_secs(access.dataset.request_timeout_seconds));
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
                    let _ = http1::Builder::new().keep_alive(false)
                        .serve_connection(TokioIo::new(stream), handler).await;
                });
            }
        }
    }
    drop(listener);
    while connections.join_next().await.is_some() {}
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crowdb_access_dataset::{
        CasOutcome, DatasetPublishRequest, DatasetPublishResponse, DatasetReadService, DatasetScanRequest,
        DatasetScanResponse, DatasetSnapshotRequest, DatasetStore, FieldDefinition, FieldLocator,
        FieldRecord, ManifestRecord, NamespacePath, Ordering, ReadPlan, SchemaRecord, Selection, SnapshotId,
        StoredValue,
    };
    use md5::{Digest, Md5};
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Mutex;

    struct MemoryStore(Mutex<HashMap<Vec<u8>, StoredValue>>);

    #[async_trait]
    impl DatasetStore for MemoryStore {
        async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, crowdb_access_dataset::StoreError> {
            Ok(self.0.lock().await.get(key).cloned())
        }

        async fn compare_exchange(
            &self,
            key: &[u8],
            expected: Option<&[u8]>,
            value: &[u8],
        ) -> Result<CasOutcome, crowdb_access_dataset::StoreError> {
            let mut values = self.0.lock().await;
            let current = values.get(key).cloned();
            if current.as_ref().map(|item| item.bytes.as_slice()) != expected {
                return Ok(CasOutcome::Conflict(current));
            }
            let revision = current.map_or(1, |item| item.revision + 1);
            values.insert(
                key.to_vec(),
                StoredValue {
                    bytes: value.to_vec(),
                    revision,
                },
            );
            Ok(CasOutcome::Applied(revision))
        }

        async fn delete(&self, key: &[u8]) -> Result<(), crowdb_access_dataset::StoreError> {
            self.0.lock().await.remove(key);
            Ok(())
        }
    }

    #[tokio::test]
    async fn dataset_listener_authenticates_and_serves_create_open() {
        let identity = crowdb_access_dataset::DatasetIdentity::new(NamespacePath::root(), "e2e").unwrap();
        let authority = Arc::new(DatasetAuthority::new(Arc::new(MemoryStore(Mutex::new(
            HashMap::new(),
        )))));
        let reads = Arc::new(DatasetReadService::new(authority, identity, 2, 10).unwrap());
        let service = Arc::new(
            DatasetHttpService::new(reads, Duration::from_secs(2)).with_bearer(Arc::<str>::from("token")),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve(listener, Arc::clone(&service), async {
            let _ = shutdown_rx.await;
        }));

        let mut unauthenticated = tokio::net::TcpStream::connect(address).await.unwrap();
        unauthenticated
            .write_all(b"GET /v1/dataset/open HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut body = Vec::new();
        unauthenticated.read_to_end(&mut body).await.unwrap();
        assert!(body.starts_with(b"HTTP/1.1 401"));

        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(b"POST /v1/dataset/create HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer token\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200"));

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn dataset_http_publish_persist_and_scan_round_trip() {
        let identity =
            crowdb_access_dataset::DatasetIdentity::new(NamespacePath::root(), "round-trip").unwrap();
        let store = Arc::new(MemoryStore(Mutex::new(HashMap::new())));
        let authority = Arc::new(DatasetAuthority::new(store));
        let reads =
            Arc::new(DatasetReadService::new(Arc::clone(&authority), identity, 2, unix_seconds()).unwrap());
        let service = Arc::new(DatasetHttpService::new(reads, Duration::from_secs(2)));
        authority
            .create_dataset(service.reads.identity().clone())
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve(listener, Arc::clone(&service), async {
            let _ = shutdown_rx.await;
        }));

        let value = b"payload".to_vec();
        let mut digest = Md5::new();
        digest.update(&value);
        let manifest = ManifestRecord {
            version: 1,
            snapshot: SnapshotId::random(),
            schema: SchemaRecord {
                version: 1,
                fields: vec![FieldDefinition {
                    name: "value".into(),
                    required: true,
                }],
            },
            samples: vec![crowdb_access_dataset::SampleRecord {
                sample_id: b"sample-1".to_vec(),
                fields: vec![FieldRecord {
                    name: "value".into(),
                    value: FieldLocator::Inline {
                        value,
                        md5: digest.finalize().into(),
                    },
                }],
            }],
        };
        let publication = DatasetPublishRequest {
            parent: None,
            manifest: bincode::serialize(&manifest).unwrap(),
            operation: None,
        };
        let publish_body = bincode::serialize(&publication).unwrap();
        let mut publish = tokio::net::TcpStream::connect(address).await.unwrap();
        publish.write_all(format!("POST /v1/dataset/publish HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n", publish_body.len()).as_bytes()).await.unwrap();
        publish.write_all(&publish_body).await.unwrap();
        let mut publish_response = Vec::new();
        publish.read_to_end(&mut publish_response).await.unwrap();
        assert!(publish_response.starts_with(b"HTTP/1.1 200"));
        let header_end = publish_response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4;
        let published: DatasetPublishResponse =
            bincode::deserialize(&publish_response[header_end..]).unwrap();
        let manifest_request = DatasetSnapshotRequest {
            snapshot: published.snapshot,
        };
        let manifest_body = bincode::serialize(&manifest_request).unwrap();
        let mut manifest_stream = tokio::net::TcpStream::connect(address).await.unwrap();
        manifest_stream
            .write_all(format!("POST /v1/dataset/manifest HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n", manifest_body.len()).as_bytes())
            .await
            .unwrap();
        manifest_stream.write_all(&manifest_body).await.unwrap();
        let mut manifest_response = Vec::new();
        manifest_stream.read_to_end(&mut manifest_response).await.unwrap();
        assert!(manifest_response.starts_with(b"HTTP/1.1 200"));
        let manifest_header_end = manifest_response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4;
        let persisted: crowdb_access_dataset::DatasetManifestResponse =
            bincode::deserialize(&manifest_response[manifest_header_end..]).unwrap();
        assert_eq!(persisted.manifest.snapshot, published.snapshot);
        assert_eq!(persisted.manifest.samples.len(), 1);

        let request = DatasetScanRequest {
            plan: ReadPlan {
                snapshot: published.snapshot,
                selection: Selection::Prefix(b"sample".to_vec()),
                ordering: Ordering::SampleId,
                projection: vec!["value".into()],
                batch_size: 1,
            },
            shuffle: None,
            cursor: None,
        };
        let body = bincode::serialize(&request).unwrap();
        let mut scan = tokio::net::TcpStream::connect(address).await.unwrap();
        scan.write_all(format!("POST /v1/dataset/scan HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes()).await.unwrap();
        scan.write_all(&body).await.unwrap();
        let mut scan_response = Vec::new();
        scan.read_to_end(&mut scan_response).await.unwrap();
        assert!(
            scan_response.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&scan_response)
        );
        let header_end = scan_response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4;
        let scanned: DatasetScanResponse = bincode::deserialize(&scan_response[header_end..]).unwrap();
        assert!(scanned.end);
        assert_eq!(scanned.samples.len(), 1);
        assert_eq!(scanned.snapshot, published.snapshot);
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }
}
