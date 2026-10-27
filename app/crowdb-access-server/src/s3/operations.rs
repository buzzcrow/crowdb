// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use crowdb_access_s3::bucket::{BucketListRequest, BucketListSigner};
use crowdb_access_s3::condition::ObjectConditions;
use crowdb_access_s3::continuation::ContinuationTokenSigner;
use crowdb_access_s3::error::{S3Error, S3ErrorCode};
use crowdb_access_s3::execution::{ObjectListRequest, PutObjectInfo, S3Executor, S3WritePolicy};
use crowdb_access_s3::metadata::{ObjectRecord, TenantId};
use crowdb_access_s3::retrieval::{ObjectHeaders, RetrievalError};
use crowdb_access_s3::route::{S3Operation, S3Route};
use crowdb_access_s3::streaming::{PutErrorCode, PutOutcome};
use crowdb_chunk_client::{ChunkClientConfig, LargeWritePolicy};
use crowdb_common::ec::EcScheme;
use futures::stream;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MATCH, IF_MODIFIED_SINCE,
    IF_NONE_MATCH, IF_UNMODIFIED_SINCE, LAST_MODIFIED, RANGE,
};
use hyper::{Request, Response, StatusCode};
use percent_encoding::percent_decode_str;

use crate::storage::S3StorageClients;
use crate::upload_flow::body_encoding::{UploadBody, UploadEncodingError};
use crowdb_access_s3::auth::StreamingPayloadVerifier;

use super::{error_response, full_body, install_body_receive_provider, BoxError, ResponseBody};
use crowdb_access_s3::wire;

mod copy;
mod delete;
mod multipart;
mod upload;

pub(crate) use upload::write_object_body;

const DEFAULT_LIST_LIMIT: usize = 1_000;
const DEFAULT_LIST_SCAN_BYTES: usize = 4 * 1024 * 1024;

pub type S3OperationsFuture = Pin<Box<dyn Future<Output = Response<ResponseBody>> + Send + 'static>>;

pub trait S3Operations: Send + Sync + 'static {
    fn observe_dependencies(self: Arc<Self>) -> super::DependencyProbeFuture {
        Box::pin(async { None })
    }

    fn execute(
        self: Arc<Self>,
        route: S3Route,
        request: Request<Incoming>,
        request_id: String,
        host_id: String,
    ) -> S3OperationsFuture;
}

#[derive(Clone)]
pub struct S3ServiceConfig {
    pub tenant: TenantId,
    pub continuation_key: Vec<u8>,
    pub small_object_limit: usize,
    pub list_scan_items: usize,
    pub list_scan_bytes: usize,
    pub continuation_ttl_seconds: u64,
    pub large_write: LargeWritePolicy,
}

impl S3ServiceConfig {
    #[must_use]
    pub fn basic(tenant: TenantId, continuation_key: Vec<u8>, small_object_limit: usize) -> Self {
        Self {
            tenant,
            continuation_key,
            small_object_limit,
            list_scan_items: DEFAULT_LIST_LIMIT + 1,
            list_scan_bytes: DEFAULT_LIST_SCAN_BYTES,
            continuation_ttl_seconds: 900,
            large_write: LargeWritePolicy {
                ec_scheme: EcScheme::new(8, 4),
                client: Arc::new(ChunkClientConfig::new(
                    crowdb_protocol::chunkdb::rpc::ChunkType::S3,
                )),
            },
        }
    }
}

pub struct ProductionS3Operations {
    pub(super) storage: S3StorageClients,
    pub(super) config: S3ServiceConfig,
    signer: ContinuationTokenSigner,
    bucket_signer: BucketListSigner,
    execution: S3Executor,
    metrics: Option<Arc<crowdb_access_s3::metrics::S3Metrics>>,
}

impl ProductionS3Operations {
    /// Builds the concrete S3-to-storage operation boundary.
    ///
    /// # Errors
    ///
    /// Rejects an empty continuation-token signing key.
    pub fn new(storage: S3StorageClients, config: S3ServiceConfig) -> Result<Self, S3ErrorCode> {
        let signer = ContinuationTokenSigner::new(config.continuation_key.clone())
            .map_err(|_| S3ErrorCode::InvalidRequest)?;
        let bucket_signer = BucketListSigner::new(config.continuation_key.clone())
            .map_err(|_| S3ErrorCode::InvalidRequest)?;
        Ok(Self {
            execution: S3Executor::new(storage.clone(), config.tenant.clone()),
            storage,
            config,
            signer,
            bucket_signer,
            metrics: None,
        })
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<crowdb_access_s3::metrics::S3Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    async fn dispatch(
        self: &Arc<Self>,
        route: S3Route,
        request: Request<Incoming>,
        request_id: String,
        host_id: String,
    ) -> Response<ResponseBody> {
        let resource = request.uri().path().to_owned();
        let head_only = request.method() == hyper::Method::HEAD;
        let operation = route.operation;
        let result = match operation {
            S3Operation::CreateBucket => self.create_bucket(route).await,
            S3Operation::HeadBucket => self.head_bucket(route).await,
            S3Operation::ListBuckets => self.list_buckets(&request).await,
            S3Operation::DeleteBucket => self.delete_bucket(route).await,
            S3Operation::PutObject => self.put_object(route, request).await,
            S3Operation::HeadObject => self.head_object(route, &request).await,
            S3Operation::GetObject => self.get_object(route, &request, &request_id).await,
            S3Operation::ListObjectsV2 => self.list_objects(route, &request).await,
            S3Operation::DeleteObject => self.delete_object(route).await,
            S3Operation::DeleteObjects => self.delete_objects(route, request).await,
            S3Operation::CopyObject => self.copy_object(route, &request).await,
            S3Operation::UploadPartCopy => self.upload_part_copy(route, &request).await,
            S3Operation::CreateMultipartUpload => self.create_multipart_upload(route, &request).await,
            S3Operation::UploadPart => self.upload_part(route, request).await,
            S3Operation::ListParts => self.list_parts(route, &request).await,
            S3Operation::CompleteMultipartUpload => self.complete_multipart_upload(route, request).await,
            S3Operation::AbortMultipartUpload => self.abort_multipart_upload(route).await,
            S3Operation::ListMultipartUploads => self.list_multipart_uploads(route, &request).await,
        };
        result.unwrap_or_else(|code| {
            tracing::debug!(%request_id, ?code, "S3 request failed");
            error_response(&S3Error::new(code, resource, request_id, host_id), head_only)
        })
    }

    async fn create_bucket(&self, route: S3Route) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let name = required_bucket(&route)?;
        self.execution.create_bucket(name).await?;
        response(StatusCode::OK, Vec::new())
    }

    async fn head_bucket(&self, route: S3Route) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let name = required_bucket(&route)?;
        self.execution.resolve_bucket(name).await?;
        response(StatusCode::OK, Vec::new())
    }

    async fn list_buckets(&self, request: &Request<Incoming>) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let query = Query::new(request.uri().query());
        let prefix = query.bytes("prefix").unwrap_or_default();
        let continuation = query.text("continuation-token");
        let max_buckets = query
            .text("max-buckets")
            .map_or(Ok(DEFAULT_LIST_LIMIT), |value| value.parse::<usize>())
            .map_err(|_| S3ErrorCode::InvalidRequest)?;
        let page = self
            .execution
            .list_bucket_page(
                &self.bucket_signer,
                &BucketListRequest {
                    prefix: &prefix,
                    continuation: continuation.as_deref(),
                    max_buckets,
                    max_scan_items: self.config.list_scan_items,
                    max_scan_bytes: self.config.list_scan_bytes,
                    now: unix_seconds(),
                    ttl: self.config.continuation_ttl_seconds,
                },
            )
            .await?;
        xml_response(wire::list_bucket_page(
            self.config.tenant.as_bytes(),
            &prefix,
            &page,
        ))
    }

    async fn delete_bucket(&self, route: S3Route) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let name = required_bucket(&route)?;
        self.execution.delete_bucket(name).await?;
        response(StatusCode::NO_CONTENT, Vec::new())
    }

    async fn put_object(
        &self,
        route: S3Route,
        mut request: Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket_name = required_bucket(&route)?;
        let key = required_key(&route)?.to_vec();
        let streaming = take_streaming(&mut request)?;
        let content_length = upload_length(&request, streaming.is_some())?;
        let content_type = header(&request, CONTENT_TYPE)
            .unwrap_or("application/octet-stream")
            .to_owned();
        let user_metadata = crowdb_access_s3::metadata::UserMetadata::from_headers(request.headers())?;
        let content_md5 = content_md5_header(&request)?.map(str::to_owned);
        let payload_sha256 = strict_header(&request, "x-amz-content-sha256", S3ErrorCode::InvalidRequest)?
            .filter(|value| *value != "UNSIGNED-PAYLOAD" && streaming.is_none())
            .map(str::to_owned);
        let mut put = self
            .execution
            .prepare_put(
                bucket_name,
                &key,
                PutObjectInfo {
                    content_length,
                    content_type,
                    user_metadata,
                },
                &S3WritePolicy {
                    small_object_limit: self.config.small_object_limit,
                    large_write: self.config.large_write.clone(),
                },
            )
            .await?;
        let native_receiver = if put.is_large() && content_length.is_some() && streaming.is_none() {
            install_body_receive_provider(&mut request)
        } else {
            None
        };
        if let Some(receiver) = &native_receiver {
            receiver.enable_owner_handoff();
        }
        let (parts, body) = request.into_parts();
        let mut body = UploadBody::new(body, &parts.headers, streaming, 6 * 1024 * 1024 * 1024)
            .map_err(encoding_error)?;
        body.defer_md5();
        let write_result = write_object_body(
            &mut body,
            put.writer_mut(),
            native_receiver.as_deref(),
            content_length,
            content_md5.as_deref(),
            payload_sha256.as_deref(),
            self.config.large_write.client.large_held_buffers,
            self.metrics.as_deref(),
        )
        .await;
        let (etag, checksum) = match write_result {
            Ok(result) => result,
            Err(outcome) => {
                put.abort().await;
                return Err(body
                    .failure()
                    .map_or_else(|| map_put_outcome(&outcome), encoding_error));
            }
        };
        put.finish(etag.clone(), checksum)
            .await
            .map_err(|outcome| map_put_outcome(&outcome))?;
        Response::builder()
            .status(StatusCode::OK)
            .header(ETAG, format!("\"{etag}\""))
            .body(full_body(Vec::new().into()))
            .map_err(|_| S3ErrorCode::InternalError)
    }

    async fn head_object(
        &self,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let record = self.object(&route).await?;
        let headers = match self.execution.prepare_head(&record, &conditions(request)) {
            Ok(headers) => headers,
            Err(RetrievalError::NotModified) => return not_modified(&record.etag),
            Err(error) => return Err(map_retrieval(&error)),
        };
        object_response(StatusCode::OK, &headers, full_body(Vec::new().into()))
    }

    async fn get_object(
        &self,
        route: S3Route,
        request: &Request<Incoming>,
        request_id: &str,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let record = self.object(&route).await?;
        let prepared = match self
            .execution
            .prepare_get(&record, header(request, RANGE), &conditions(request))
        {
            Ok(prepared) => prepared,
            Err(RetrievalError::NotModified) => return not_modified(&record.etag),
            Err(error) => return Err(map_retrieval(&error)),
        };
        let body = match prepared.stream {
            Some(stream) => {
                let request_id = request_id.to_owned();
                let frames = stream::unfold((stream, request_id), |(mut stream, request_id)| async move {
                    stream.next_chunk().await.map(|result| {
                        let frame = result.map(Frame::data).map_err(|error| {
                            tracing::error!(%request_id, %error, "S3 GET stream terminated");
                            Box::new(error) as BoxError
                        });
                        (frame, (stream, request_id))
                    })
                });
                StreamBody::new(frames).boxed()
            }
            None => full_body(Vec::new().into()),
        };
        object_response(
            if prepared.partial {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            },
            &prepared.headers,
            body,
        )
    }

    async fn list_objects(
        &self,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket_name = required_bucket(&route)?;
        let bucket_id = self.resolve_bucket(bucket_name).await?;
        let query = Query::new(request.uri().query());
        let prefix = query.bytes("prefix").unwrap_or_default();
        let delimiter = query.bytes("delimiter");
        let start_after = query.bytes("start-after");
        let continuation = query.text("continuation-token");
        let url_encoding = match query.text("encoding-type").as_deref() {
            None => false,
            Some("url") => true,
            _ => return Err(S3ErrorCode::InvalidRequest),
        };
        let max_keys = query
            .text("max-keys")
            .map_or(Ok(DEFAULT_LIST_LIMIT), |value| value.parse::<usize>())
            .map_err(|_| S3ErrorCode::InvalidRequest)?
            .min(DEFAULT_LIST_LIMIT);
        let page = self
            .execution
            .list_objects(
                &self.signer,
                &ObjectListRequest {
                    bucket: bucket_id,
                    prefix: &prefix,
                    delimiter: delimiter.as_deref(),
                    start_after: start_after.as_deref(),
                    continuation_token: continuation.as_deref(),
                    max_keys,
                    max_scan_items: self.config.list_scan_items,
                    max_scan_bytes: self.config.list_scan_bytes,
                    now_unix_seconds: unix_seconds(),
                    token_ttl_seconds: self.config.continuation_ttl_seconds,
                },
            )
            .await?;
        xml_response(wire::list_objects(
            bucket_name,
            &prefix,
            delimiter.as_deref(),
            max_keys,
            &page,
            url_encoding,
            start_after.as_deref(),
        ))
    }

    async fn delete_object(&self, route: S3Route) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket_name = required_bucket(&route)?;
        let key = required_key(&route)?;
        self.execution.delete_object(bucket_name, key).await?;
        response(StatusCode::NO_CONTENT, Vec::new())
    }

    async fn resolve_bucket(&self, name: &[u8]) -> Result<crowdb_access_s3::metadata::BucketId, S3ErrorCode> {
        self.execution.resolve_bucket(name).await
    }

    async fn object(&self, route: &S3Route) -> Result<ObjectRecord, S3ErrorCode> {
        self.execution
            .object(required_bucket(route)?, required_key(route)?)
            .await
    }
}

impl S3Operations for ProductionS3Operations {
    fn observe_dependencies(self: Arc<Self>) -> super::DependencyProbeFuture {
        Box::pin(async move { Some(Box::pin(self.observe_storage()).await) })
    }

    fn execute(
        self: Arc<Self>,
        route: S3Route,
        request: Request<Incoming>,
        request_id: String,
        host_id: String,
    ) -> S3OperationsFuture {
        Box::pin(async move { self.dispatch(route, request, request_id, host_id).await })
    }
}

fn required_bucket(route: &S3Route) -> Result<&[u8], S3ErrorCode> {
    route.bucket.as_deref().ok_or(S3ErrorCode::InvalidRequest)
}

fn required_key(route: &S3Route) -> Result<&[u8], S3ErrorCode> {
    route
        .key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or(S3ErrorCode::InvalidRequest)
}

fn content_length(request: &Request<Incoming>) -> Result<Option<u64>, S3ErrorCode> {
    header(request, CONTENT_LENGTH)
        .map(str::parse)
        .transpose()
        .map_err(|_| S3ErrorCode::InvalidRequest)
}

fn take_streaming(request: &mut Request<Incoming>) -> Result<Option<StreamingPayloadVerifier>, S3ErrorCode> {
    request
        .extensions_mut()
        .remove::<Arc<StreamingPayloadVerifier>>()
        .map(|value| Arc::try_unwrap(value).map_err(|_| S3ErrorCode::InternalError))
        .transpose()
}

fn upload_length(request: &Request<Incoming>, streaming: bool) -> Result<Option<u64>, S3ErrorCode> {
    if streaming {
        strict_header(
            request,
            "x-amz-decoded-content-length",
            S3ErrorCode::InvalidRequest,
        )?
        .map(str::parse)
        .transpose()
        .map_err(|_| S3ErrorCode::InvalidRequest)
    } else {
        content_length(request)
    }
}

fn encoding_error(error: UploadEncodingError) -> S3ErrorCode {
    match error {
        UploadEncodingError::Checksum => S3ErrorCode::BadDigest,
        UploadEncodingError::Signature => S3ErrorCode::AccessDenied,
        _ => S3ErrorCode::InvalidRequest,
    }
}

fn content_md5_header(request: &Request<Incoming>) -> Result<Option<&str>, S3ErrorCode> {
    let value = strict_header(request, "content-md5", S3ErrorCode::InvalidDigest)?;
    if let Some(value) = value {
        let digest = base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(|_| S3ErrorCode::InvalidDigest)?;
        if digest.len() != 16 {
            return Err(S3ErrorCode::InvalidDigest);
        }
    }
    Ok(value)
}

fn conditions(request: &Request<Incoming>) -> ObjectConditions<'_> {
    ObjectConditions {
        if_match: header(request, IF_MATCH),
        if_none_match: header(request, IF_NONE_MATCH),
        if_modified_since: header(request, IF_MODIFIED_SINCE),
        if_unmodified_since: header(request, IF_UNMODIFIED_SINCE),
    }
}

fn header(request: &Request<Incoming>, name: hyper::header::HeaderName) -> Option<&str> {
    request.headers().get(name)?.to_str().ok()
}

fn strict_header<'a>(
    request: &'a Request<Incoming>,
    name: &str,
    invalid: S3ErrorCode,
) -> Result<Option<&'a str>, S3ErrorCode> {
    let mut values = request.headers().get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(invalid);
    }
    value.to_str().map(Some).map_err(|_| invalid)
}

fn map_retrieval(error: &RetrievalError) -> S3ErrorCode {
    match error {
        RetrievalError::PreconditionFailed => S3ErrorCode::PreconditionFailed,
        RetrievalError::Range(_) => S3ErrorCode::InvalidRange,
        RetrievalError::NotModified | RetrievalError::DataReference | RetrievalError::ChunkRead => {
            S3ErrorCode::InternalError
        }
    }
}

fn map_put_outcome(outcome: &PutOutcome) -> S3ErrorCode {
    if matches!(
        outcome,
        PutOutcome::Timeout
            | PutOutcome::Error {
                code: PutErrorCode::ChunkWrite
                    | PutErrorCode::LocationEncoding
                    | PutErrorCode::MetadataEncoding
                    | PutErrorCode::KvRejected,
                ..
            }
    ) {
        tracing::warn!(?outcome, "S3 PUT failed");
    }
    match outcome {
        PutOutcome::Success => S3ErrorCode::InternalError,
        PutOutcome::Timeout => S3ErrorCode::ServiceUnavailable,
        PutOutcome::Error { code, .. } => match code {
            PutErrorCode::InvalidDigest => S3ErrorCode::InvalidDigest,
            PutErrorCode::BadDigest => S3ErrorCode::BadDigest,
            PutErrorCode::PayloadMismatch => S3ErrorCode::XAmzContentSHA256Mismatch,
            PutErrorCode::InvalidPayloadDigest | PutErrorCode::InvalidKey | PutErrorCode::BodyRead => {
                S3ErrorCode::InvalidRequest
            }
            PutErrorCode::ChunkWrite
            | PutErrorCode::LocationEncoding
            | PutErrorCode::MetadataEncoding
            | PutErrorCode::KvRejected => S3ErrorCode::ServiceUnavailable,
        },
    }
}

fn response(status: StatusCode, body: Vec<u8>) -> Result<Response<ResponseBody>, S3ErrorCode> {
    Response::builder()
        .status(status)
        .body(full_body(body.into()))
        .map_err(|_| S3ErrorCode::InternalError)
}

fn xml_response(xml: String) -> Result<Response<ResponseBody>, S3ErrorCode> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(xml.into()))
        .map_err(|_| S3ErrorCode::InternalError)
}

fn object_response(
    status: StatusCode,
    headers: &ObjectHeaders,
    body: ResponseBody,
) -> Result<Response<ResponseBody>, S3ErrorCode> {
    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, headers.content_length)
        .header(ETAG, format!("\"{}\"", headers.etag))
        .header(LAST_MODIFIED, &headers.last_modified)
        .header(ACCEPT_RANGES, "bytes");
    if let Ok(value) = hyper::header::HeaderValue::from_str(&headers.content_type) {
        builder = builder.header(CONTENT_TYPE, value);
    }
    if let Some(content_range) = &headers.content_range {
        builder = builder.header(CONTENT_RANGE, content_range);
    }
    let metadata = crowdb_access_s3::metadata::UserMetadata::decode(&headers.attributes)
        .map_err(|_| S3ErrorCode::InternalError)?;
    metadata.append_headers(builder.headers_mut().ok_or(S3ErrorCode::InternalError)?)?;
    builder.body(body).map_err(|_| S3ErrorCode::InternalError)
}

fn not_modified(etag: &str) -> Result<Response<ResponseBody>, S3ErrorCode> {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(ETAG, format!("\"{etag}\""))
        .body(full_body(Vec::new().into()))
        .map_err(|_| S3ErrorCode::InternalError)
}

struct Query<'a> {
    raw: Option<&'a str>,
}

impl<'a> Query<'a> {
    const fn new(raw: Option<&'a str>) -> Self {
        Self { raw }
    }

    fn text(&self, name: &str) -> Option<String> {
        String::from_utf8(self.bytes(name)?).ok()
    }

    fn bytes(&self, name: &str) -> Option<Vec<u8>> {
        self.raw?.split('&').find_map(|field| {
            let (candidate, value) = field.split_once('=').unwrap_or((field, ""));
            (candidate == name).then(|| percent_decode_str(value).collect())
        })
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unix_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}
