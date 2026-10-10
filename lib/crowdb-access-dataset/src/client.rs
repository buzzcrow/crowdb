use std::sync::Arc;

use crate::{
    AuthorityError, DatasetId, DatasetIdentity, DatasetManifestResponse, DatasetPublishRequest,
    DatasetPublishResponse, DatasetReadRequest, DatasetReadResponse, DatasetReadService,
    DatasetReclaimResponse, DatasetRecord, DatasetRetentionResponse, DatasetScanRequest, DatasetScanResponse,
    DatasetSnapshotRequest, DatasetSnapshotResponse, DatasetSnapshotsResponse, ReadSurface, ReclaimProgress,
    SnapshotId,
};

#[derive(Debug, thiserror::Error)]
pub enum DatasetClientError {
    #[error("dataset HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("dataset HTTP returned status {status}: {body}")]
    Status { status: u16, body: String },
    #[error("dataset response was malformed")]
    Codec(#[from] Box<bincode::ErrorKind>),
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error(transparent)]
    Transport(#[from] crate::ReadTransportError),
}

/// Thin client for the Dataset HTTP contract. It serializes the same wire
/// requests used by the Access Server and does not duplicate read semantics.
pub struct DatasetHttpClient {
    client: reqwest::Client,
    base: String,
    bearer: Option<String>,
}

#[allow(clippy::missing_errors_doc)]
impl DatasetHttpClient {
    /// Creates a client rooted at an Access Server Dataset listener.
    pub fn new(base: impl Into<String>, bearer: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base: base.into().trim_end_matches('/').to_owned(),
            bearer,
        }
    }

    async fn call<T: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        value: &T,
    ) -> Result<R, DatasetClientError> {
        let mut request = self
            .client
            .post(format!("{}{path}", self.base))
            .body(bincode::serialize(value)?);
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(DatasetClientError::Status {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        Ok(bincode::deserialize(&body)?)
    }

    async fn get<R: serde::de::DeserializeOwned>(&self, path: &str) -> Result<R, DatasetClientError> {
        let mut request = self.client.get(format!("{}{path}", self.base));
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(DatasetClientError::Status {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        Ok(bincode::deserialize(&body)?)
    }

    pub async fn create(&self) -> Result<DatasetId, DatasetClientError> {
        self.call("/v1/dataset/create", &()).await
    }

    pub async fn open(&self) -> Result<DatasetRecord, DatasetClientError> {
        self.get("/v1/dataset/open").await
    }

    pub async fn latest(&self) -> Result<Option<SnapshotId>, DatasetClientError> {
        self.get("/v1/dataset/latest").await
    }

    pub async fn list_snapshots(&self) -> Result<DatasetSnapshotsResponse, DatasetClientError> {
        self.get("/v1/dataset/snapshots").await
    }

    pub async fn retain(&self, snapshot: SnapshotId) -> Result<DatasetRetentionResponse, DatasetClientError> {
        self.call("/v1/dataset/retain", &DatasetSnapshotRequest { snapshot })
            .await
    }

    pub async fn release(
        &self,
        snapshot: SnapshotId,
    ) -> Result<DatasetRetentionResponse, DatasetClientError> {
        self.call("/v1/dataset/release", &DatasetSnapshotRequest { snapshot })
            .await
    }

    pub async fn snapshot(
        &self,
        snapshot: SnapshotId,
    ) -> Result<DatasetSnapshotResponse, DatasetClientError> {
        self.call("/v1/dataset/snapshot", &DatasetSnapshotRequest { snapshot })
            .await
    }

    pub async fn manifest(
        &self,
        snapshot: SnapshotId,
    ) -> Result<DatasetManifestResponse, DatasetClientError> {
        self.call("/v1/dataset/manifest", &DatasetSnapshotRequest { snapshot })
            .await
    }

    pub async fn set_stable(&self, snapshot: SnapshotId) -> Result<(), DatasetClientError> {
        let _: DatasetRetentionResponse = self
            .call("/v1/dataset/stable", &DatasetSnapshotRequest { snapshot })
            .await?;
        Ok(())
    }

    pub async fn reclaim(&self, snapshot: SnapshotId) -> Result<DatasetReclaimResponse, DatasetClientError> {
        self.call("/v1/dataset/reclaim", &DatasetSnapshotRequest { snapshot })
            .await
    }

    pub async fn reclaim_status(&self, snapshot: SnapshotId) -> Result<ReclaimProgress, DatasetClientError> {
        self.call("/v1/dataset/reclaim/status", &DatasetSnapshotRequest { snapshot })
            .await
    }

    pub async fn publish(
        &self,
        request: DatasetPublishRequest,
    ) -> Result<DatasetPublishResponse, DatasetClientError> {
        self.call("/v1/dataset/publish", &request).await
    }

    pub async fn prepare(
        &self,
        request: DatasetPublishRequest,
    ) -> Result<DatasetPublishResponse, DatasetClientError> {
        self.call("/v1/dataset/prepare", &request).await
    }

    pub async fn read(&self, request: DatasetReadRequest) -> Result<DatasetReadResponse, DatasetClientError> {
        self.call("/v1/dataset/read", &request).await
    }

    pub async fn scan(&self, request: DatasetScanRequest) -> Result<DatasetScanResponse, DatasetClientError> {
        self.call("/v1/dataset/scan", &request).await
    }

    pub async fn start_read_plan(
        &self,
        request: DatasetScanRequest,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        self.call("/v1/dataset/start-read-plan", &request).await
    }

    pub async fn next_batch(
        &self,
        request: DatasetScanRequest,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        self.call("/v1/dataset/next-batch", &request).await
    }

    pub async fn save_progress(
        &self,
        request: DatasetScanRequest,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        self.call("/v1/dataset/save-progress", &request).await
    }

    pub async fn resume(
        &self,
        request: DatasetScanRequest,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        self.call("/v1/dataset/resume", &request).await
    }
}

/// Fat client facade for direct access to the same Dataset authority and
/// bounded transport. Storage routing remains inside the supplied authority.
pub struct DatasetDirectClient {
    service: Arc<DatasetReadService>,
}

#[allow(clippy::missing_errors_doc)]
impl DatasetDirectClient {
    #[must_use]
    pub fn new(service: Arc<DatasetReadService>) -> Self {
        Self { service }
    }

    pub async fn read(
        &self,
        request: DatasetReadRequest,
        now_seconds: u64,
    ) -> Result<DatasetReadResponse, DatasetClientError> {
        Ok(self
            .service
            .read_surface(ReadSurface::Native, request, now_seconds)
            .await?)
    }

    pub async fn scan(
        &self,
        request: DatasetScanRequest,
        now_seconds: u64,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        Ok(self
            .service
            .scan_surface(ReadSurface::Native, request, now_seconds)
            .await?)
    }

    pub async fn start_read_plan(
        &self,
        request: DatasetScanRequest,
        now_seconds: u64,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        self.scan(request, now_seconds).await
    }

    pub async fn next_batch(
        &self,
        request: DatasetScanRequest,
        now_seconds: u64,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        self.scan(request, now_seconds).await
    }

    pub async fn save_progress(
        &self,
        request: DatasetScanRequest,
        now_seconds: u64,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        self.scan(request, now_seconds).await
    }

    pub async fn resume(
        &self,
        request: DatasetScanRequest,
        now_seconds: u64,
    ) -> Result<DatasetScanResponse, DatasetClientError> {
        self.scan(request, now_seconds).await
    }

    #[must_use]
    pub fn identity(&self) -> &DatasetIdentity {
        self.service.identity()
    }
}
