use std::fmt::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::{
    CatalogError, CatalogLifecycle, CatalogRepository, CatalogStore, RootState,
};
use crowdb_access_iceberg::file::{
    resolve_range, FileBlockStore, FileGrantError, FileGrantIssuer, FileOperation, FileRecord,
    FileRepository, FileSealer, MultipartLister, MultipartPartStore, MultipartRepository, RangeError,
};
use crowdb_access_iceberg::gc::{GcRepository, GcStore};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::storage::{default_large_write, own_large_write};
use crowdb_access_s3::auth::{RawAuthRequest, StreamingPayloadVerifier};
use crowdb_access_s3::native_buffer::{NativeBodyAllocator, NativeBodyReceiver};
use crowdb_chunk_client::LargeWritePolicy;
use hyper::body::Incoming;
use hyper::http::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, ETAG, RANGE};
use hyper::{Method, Request, Response, StatusCode};

use super::body::IcebergBody;
use super::file_admission::{FileAdmissionError, FileServiceLimits, FileTransferAdmission};
use super::file_auth::authenticate_file_transfer;
use super::file_body::FileResponseBudget;
use super::file_encoding::FileUploadBody;
use super::file_request::{FileRequest, FileRequestError};
use super::file_response::{FileS3ErrorCode, MultipartResponses};
use super::file_upload::FileUploadBudget;

mod delete;
mod listing;
mod metrics;
mod multipart;
mod stream;

use metrics::UploadFlowMetrics;
pub use metrics::UploadFlowSnapshot;

pub(super) struct FileHttp {
    repository: FileRepository,
    store: Arc<dyn CatalogStore>,
    gc: GcRepository,
    multipart: MultipartRepository,
    lister: MultipartLister,
    blocks: Arc<dyn FileBlockStore>,
    issuer: FileGrantIssuer,
    responses: FileResponseBudget,
    uploads: FileUploadBudget,
    small_threshold_exclusive: usize,
    large_write: LargeWritePolicy,
    native_allocator: Option<Arc<NativeBodyAllocator>>,
    upload_metrics: Arc<UploadFlowMetrics>,
    region: String,
    limits: FileServiceLimits,
    list_tokens: crowdb_access_iceberg::file::FileListTokens,
}

impl FileHttp {
    pub(super) fn native_receive_metrics(
        &self,
    ) -> Option<crowdb_access_s3::native_buffer::NativeBufferMetricsSnapshot> {
        self.native_allocator
            .as_ref()
            .map(|allocator| allocator.metrics_snapshot())
    }

    pub(super) fn upload_metrics_snapshot(&self) -> UploadFlowSnapshot {
        self.upload_metrics.snapshot()
    }

    pub(super) fn chunk_metrics(
        &self,
    ) -> Option<(
        crowdb_chunk_client::ReadFlowMetricsSnapshot,
        crowdb_chunk_client::SmallWriteMetricsSnapshot,
    )> {
        self.blocks.chunk_metrics()
    }

    pub(super) fn new<Store: MultipartPartStore + GcStore + 'static>(
        store: Arc<Store>,
        blocks: Arc<dyn FileBlockStore>,
        secret: [u8; 32],
        region: String,
        native_allocator: Option<Arc<NativeBodyAllocator>>,
    ) -> Result<Self, FileGrantError> {
        if region.is_empty()
            || region.len() > 64
            || !region
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(FileGrantError::Invalid);
        }
        Ok(Self {
            list_tokens: crowdb_access_iceberg::file::FileListTokens::new(secret)
                .map_err(|_| FileGrantError::Invalid)?,
            repository: FileRepository::new(store.clone()),
            store: store.clone(),
            gc: GcRepository::new(store.clone()),
            multipart: MultipartRepository::new(store.clone()),
            lister: MultipartLister::new(store),
            blocks,
            issuer: FileGrantIssuer::new(secret, 15 * 60 * 1000)?,
            responses: FileResponseBudget::new(64).map_err(|_| FileGrantError::Invalid)?,
            uploads: FileUploadBudget::new(64).map_err(|_| FileGrantError::Invalid)?,
            small_threshold_exclusive: crate::config::SmallWriteConfig::default().threshold_exclusive(),
            large_write: default_large_write(),
            native_allocator,
            upload_metrics: Arc::new(UploadFlowMetrics::default()),
            region,
            limits: FileServiceLimits {
                max_request_bytes: 1024 * 1024 * 1024,
                max_file_bytes: 1024 * 1024 * 1024 * 1024,
                max_part_bytes: 1024 * 1024 * 1024,
                max_staged_bytes: 1024 * 1024 * 1024 * 1024,
            },
        })
    }

    pub(super) fn set_small_threshold(&mut self, threshold_exclusive: usize) -> Result<(), FileGrantError> {
        if threshold_exclusive == 0 || threshold_exclusive > 32 * 1024 * 1024 {
            return Err(FileGrantError::Invalid);
        }
        self.small_threshold_exclusive = threshold_exclusive;
        Ok(())
    }

    pub(super) fn set_large_write(&mut self, mut policy: LargeWritePolicy) -> Result<(), FileGrantError> {
        if policy.ec_scheme.data_num == 0
            || policy.ec_scheme.code_num == 0
            || policy.client.read_buffer_size == 0
        {
            return Err(FileGrantError::Invalid);
        }
        own_large_write(&mut policy);
        self.large_write = policy;
        Ok(())
    }

    pub(super) async fn dispatch(
        self: &Arc<Self>,
        catalog: &CatalogRepository,
        request: Request<Incoming>,
        request_timeout: Duration,
    ) -> Response<IcebergBody> {
        let path = request.uri().path().to_owned();
        match Box::pin(self.execute(catalog, request, request_timeout)).await {
            Ok(response) => response,
            Err(code) => {
                tracing::debug!(?code, %path, "native file request rejected");
                s3_error(code, &path)
            }
        }
    }

    async fn execute(
        self: &Arc<Self>,
        catalog: &CatalogRepository,
        mut request: Request<Incoming>,
        request_timeout: Duration,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        if request.method() == Method::POST && matches!(request.uri().query(), Some("delete" | "delete=")) {
            return self.delete_objects(catalog, request, request_timeout).await;
        }
        if let Some(list) = super::file_list_request::parse_file_list(request.method(), request.uri())
            .map_err(request_error)?
        {
            return self.list_files(catalog, &request, &list, request_timeout).await;
        }
        let file_request = FileRequest::parse(request.method(), request.uri()).map_err(request_error)?;
        let (root, authority) = catalog.status().await.map_err(catalog_error)?;
        if root.state != RootState::Ready
            || authority.lifecycle != CatalogLifecycle::Ready
            || request_timeout.is_zero()
            || request_timeout > Duration::from_millis(authority.admission_bounds.request_ms)
        {
            return Err(FileS3ErrorCode::SlowDown);
        }
        let now_ms = now_ms()?;
        let (grant, streaming) = authenticate_file_transfer(
            &self.issuer,
            root.context,
            RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
            &self.region,
            now_ms,
        )
        .map_err(|_| FileS3ErrorCode::AccessDenied)?;
        grant
            .authorize(file_request.operation, &file_request.location, 0, 0)
            .map_err(|_| FileS3ErrorCode::AccessDenied)?;
        let session = self.load_session(root.context, &file_request).await?;
        let admission =
            FileTransferAdmission::authorize(&grant, &file_request, self.limits, session.as_ref(), now_ms)
                .map_err(admission_error)?;
        let declared_receive = request
            .headers()
            .get("x-amz-decoded-content-length")
            .or_else(|| request.headers().get(CONTENT_LENGTH))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        let native_receiver = if matches!(
            file_request.operation,
            FileOperation::Put | FileOperation::UploadPart
        ) && streaming.is_none()
            && !request
                .headers()
                .get("content-encoding")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.split(',').any(|part| part.trim() == "aws-chunked"))
        {
            self.native_allocator.as_ref().and_then(|allocator| {
                if let Some(length) = declared_receive
                    .and_then(|length| usize::try_from(length).ok())
                    .filter(|length| *length > 0 && *length < self.small_threshold_exclusive)
                {
                    crate::http_receive::install_small_body_receive_provider(&mut request, allocator, length)
                } else {
                    Some(crate::http_receive::install_native_body_receive_provider(
                        &mut request,
                        allocator,
                    ))
                }
            })
        } else {
            None
        };
        match file_request.operation {
            FileOperation::Head | FileOperation::Get => {
                self.read(&file_request, &request, root.context, &admission).await
            }
            FileOperation::Put => {
                self.put(
                    &file_request,
                    request,
                    root.context,
                    &admission,
                    streaming,
                    native_receiver.as_deref(),
                )
                .await
            }
            FileOperation::DeleteObject => {
                self.delete_object(&file_request.location, root.context, now_ms)
                    .await
            }
            _ => {
                Box::pin(self.multipart_request(
                    &file_request,
                    request,
                    session,
                    &admission,
                    now_ms,
                    streaming,
                    native_receiver.as_deref(),
                ))
                .await
            }
        }
    }

    async fn put(
        &self,
        file_request: &FileRequest,
        request: Request<Incoming>,
        context: crowdb_access_iceberg::catalog::CatalogContext,
        admission: &FileTransferAdmission,
        streaming: Option<StreamingPayloadVerifier>,
        native_receiver: Option<&NativeBodyReceiver>,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let digest = if streaming.is_some() {
            None
        } else {
            multipart::signed_digest(request.headers().get("x-amz-content-sha256"))?
        };
        let (parts, body) = request.into_parts();
        let mut body = FileUploadBody::new(body, &parts.headers, streaming, admission.request_byte_limit())
            .map_err(multipart::encoding_error)?;
        if digest.is_none() && !body.has_integrity() {
            return Err(FileS3ErrorCode::InvalidRequest);
        }
        let length = body.decoded_length();
        let owner = crowdb_access_iceberg::file::FileIdentity {
            table: file_request.location.table(),
            file: crowdb_access_iceberg::key::FileId::random(),
        };
        if self.blocks.supports_stream_io() {
            let published = stream::upload(
                self.blocks.as_ref(),
                &self.uploads,
                admission,
                body,
                owner,
                file_request.location.clone(),
                length,
                digest,
                native_receiver,
                self.small_threshold_exclusive,
                &self.large_write,
                &self.upload_metrics,
                stream::Publication::Direct {
                    repository: &self.repository,
                    context,
                },
            )
            .await?;
            let stream::UploadedObject::Direct(published) = published else {
                return Err(FileS3ErrorCode::InternalError);
            };
            let mut response = Response::new(IcebergBody::new(Vec::new()));
            set_header(&mut response, ETAG, &etag(&published))?;
            return Ok(response);
        }
        let tree = admission
            .receive(
                &self.uploads,
                &mut body,
                self.blocks.clone(),
                owner,
                length,
                digest,
            )
            .await
            .map_err(|error| {
                body.failure()
                    .map_or_else(|| admission_error(error), multipart::encoding_error)
            })?;
        let sealed = FileSealer::new(self.blocks.clone(), self.limits.max_file_bytes)
            .map_err(|_| FileS3ErrorCode::InternalError)?
            .seal_uploaded(owner, file_request.location.clone(), tree)
            .await
            .map_err(multipart::seal_error)?;
        let published = self
            .repository
            .publish(context, &sealed)
            .await
            .map_err(catalog_error)?;
        let mut response = Response::new(IcebergBody::new(Vec::new()));
        set_header(&mut response, ETAG, &etag(&published))?;
        Ok(response)
    }

    async fn read(
        &self,
        file_request: &FileRequest,
        request: &Request<Incoming>,
        context: crowdb_access_iceberg::catalog::CatalogContext,
        admission: &FileTransferAdmission,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let record = self
            .repository
            .load(context, &file_request.location)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::NoSuchKey)?;
        let range = match request
            .headers()
            .get_all(RANGE)
            .iter()
            .collect::<Vec<_>>()
            .as_slice()
        {
            [] => None,
            [value] => Some(value.to_str().map_err(|_| FileS3ErrorCode::InvalidRange)?),
            _ => return Err(FileS3ErrorCode::InvalidRange),
        };
        let range = resolve_range(range, record.length).map_err(range_error)?;
        let bytes = range.map_or(record.length, |range| range.end - range.start);
        let body = if request.method() == Method::HEAD {
            admission.check_bytes(0, record.length).map_err(admission_error)?;
            IcebergBody::new(Vec::new())
        } else {
            IcebergBody::file(
                admission
                    .read_body(&self.responses, self.blocks.clone(), record.clone(), range)
                    .map_err(admission_error)?,
            )
        };
        let mut response = Response::new(body);
        *response.status_mut() = if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        set_header(&mut response, CONTENT_LENGTH, &bytes.to_string())?;
        set_header(&mut response, ETAG, &etag(&record))?;
        response
            .headers_mut()
            .insert(ACCEPT_RANGES, hyper::header::HeaderValue::from_static("bytes"));
        if let Some(range) = range {
            set_header(
                &mut response,
                CONTENT_RANGE,
                &format!("bytes {}-{}/{}", range.start, range.end - 1, record.length),
            )?;
        }
        Ok(response)
    }
}

fn set_header(
    response: &mut Response<IcebergBody>,
    name: hyper::header::HeaderName,
    value: &str,
) -> Result<(), FileS3ErrorCode> {
    response.headers_mut().insert(
        name,
        hyper::header::HeaderValue::from_str(value).map_err(|_| FileS3ErrorCode::InternalError)?,
    );
    Ok(())
}

fn etag(record: &FileRecord) -> String {
    if let Some(etag) = record.content.etag() {
        return format!("\"{etag}\"");
    }
    let mut value = String::from("\"");
    for byte in record.digest {
        write!(value, "{byte:02x}").expect("string writes do not fail");
    }
    value.push('"');
    value
}

fn now_ms() -> Result<u64, FileS3ErrorCode> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .ok_or(FileS3ErrorCode::InternalError)
}

fn request_error(error: FileRequestError) -> FileS3ErrorCode {
    match error {
        FileRequestError::Invalid | FileRequestError::Unsupported => FileS3ErrorCode::InvalidRequest,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn catalog_error(error: CatalogError) -> FileS3ErrorCode {
    match error {
        CatalogError::Forbidden => FileS3ErrorCode::AccessDenied,
        CatalogError::Conflict => FileS3ErrorCode::Conflict,
        CatalogError::Busy | CatalogError::Uninitialized => FileS3ErrorCode::SlowDown,
        CatalogError::Invalid(_) | CatalogError::Store(_) => FileS3ErrorCode::InternalError,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn admission_error(error: FileAdmissionError) -> FileS3ErrorCode {
    match error {
        FileAdmissionError::Grant(FileGrantError::Bounds)
        | FileAdmissionError::Bounds
        | FileAdmissionError::Upload(super::file_upload::FileUploadError::Bounds) => {
            FileS3ErrorCode::EntityTooLarge
        }
        FileAdmissionError::Scope | FileAdmissionError::Grant(_) => FileS3ErrorCode::AccessDenied,
        FileAdmissionError::State | FileAdmissionError::Read(super::file_body::FileBodyError::Busy) => {
            FileS3ErrorCode::SlowDown
        }
        FileAdmissionError::Upload(super::file_upload::FileUploadError::Digest) => FileS3ErrorCode::BadDigest,
        FileAdmissionError::Upload(
            super::file_upload::FileUploadError::Length | super::file_upload::FileUploadError::Trailers,
        ) => FileS3ErrorCode::InvalidRequest,
        FileAdmissionError::Upload(super::file_upload::FileUploadError::Busy) => FileS3ErrorCode::SlowDown,
        FileAdmissionError::Read(_) | FileAdmissionError::Upload(_) => FileS3ErrorCode::InternalError,
    }
}

fn range_error(error: RangeError) -> FileS3ErrorCode {
    match error {
        RangeError::Invalid | RangeError::Multiple | RangeError::Unsatisfiable => {
            FileS3ErrorCode::InvalidRange
        }
    }
}

pub(super) fn unavailable(path: &str) -> Response<IcebergBody> {
    s3_error(FileS3ErrorCode::SlowDown, path)
}

fn s3_error(code: FileS3ErrorCode, path: &str) -> Response<IcebergBody> {
    let resource = if path.len() <= 2048 { path } else { "/" };
    MultipartResponses::error(code, resource, &OperationId::random().to_string())
        .expect("bounded S3 error fields")
        .map(IcebergBody::new)
}
