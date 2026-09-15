// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_access_s3::bucket::{self, BucketError, DeleteBucketResult, RandomBucketIdGenerator};
use crowdb_access_s3::condition::ObjectConditions;
use crowdb_access_s3::continuation::ContinuationTokenSigner;
use crowdb_access_s3::error::{S3Error, S3ErrorCode};
use crowdb_access_s3::metadata::{ObjectRecord, TenantId};
use crowdb_access_s3::object::{self, ListObjectsV2Request, ObjectMetadataError};
use crowdb_access_s3::publication::PublicationRequest;
use crowdb_access_s3::retrieval::{self, ObjectHeaders, RetrievalError};
use crowdb_access_s3::route::{S3Operation, S3Route};
use crowdb_access_s3::streaming::{
    publish_completed_locations, write_body_with_checksums, PutErrorCode, PutOutcome,
};
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoWriter, IoError, LargeWritePolicy, PreparedLargeWrite, SharedObjectWriter,
};
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

use super::{error_response, full_body, BoxError, ResponseBody};
use crowdb_access_s3::wire;

const DEFAULT_LIST_LIMIT: usize = 1_000;
const DEFAULT_LIST_SCAN_BYTES: usize = 4 * 1024 * 1024;

pub type S3OperationsFuture = Pin<Box<dyn Future<Output = Response<ResponseBody>> + Send + 'static>>;

pub trait S3Operations: Send + Sync + 'static {
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
                client: Arc::new(ChunkClientConfig::default()),
            },
        }
    }
}

pub struct ProductionS3Operations {
    storage: S3StorageClients,
    config: S3ServiceConfig,
    signer: ContinuationTokenSigner,
    bucket_ids: RandomBucketIdGenerator,
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
        Ok(Self {
            storage,
            config,
            signer,
            bucket_ids: RandomBucketIdGenerator,
        })
    }

    async fn dispatch(
        &self,
        route: S3Route,
        request: Request<Incoming>,
        request_id: String,
        host_id: String,
    ) -> Response<ResponseBody> {
        let resource = request.uri().path().to_owned();
        let head_only = request.method() == hyper::Method::HEAD;
        let result = match route.operation {
            S3Operation::CreateBucket => self.create_bucket(route).await,
            S3Operation::HeadBucket => self.head_bucket(route).await,
            S3Operation::ListBuckets => self.list_buckets().await,
            S3Operation::DeleteBucket => self.delete_bucket(route).await,
            S3Operation::PutObject => self.put_object(route, request).await,
            S3Operation::HeadObject => self.head_object(route, &request).await,
            S3Operation::GetObject => self.get_object(route, &request, &request_id).await,
            S3Operation::ListObjectsV2 => self.list_objects(route, &request).await,
            S3Operation::DeleteObject => self.delete_object(route).await,
        };
        result.unwrap_or_else(|code| {
            tracing::debug!(%request_id, ?code, "S3 request failed");
            error_response(&S3Error::new(code, resource, request_id, host_id), head_only)
        })
    }

    async fn create_bucket(&self, route: S3Route) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let name = required_bucket(&route)?;
        bucket::create_bucket(
            &self.storage.metadata,
            &self.bucket_ids,
            &self.config.tenant,
            name,
        )
        .await
        .map_err(|error| map_bucket_error(&error))?;
        response(StatusCode::OK, Vec::new())
    }

    async fn head_bucket(&self, route: S3Route) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let name = required_bucket(&route)?;
        let exists = bucket::head_bucket(&self.storage.metadata, &self.config.tenant, name)
            .await
            .map_err(|error| map_bucket_error(&error))?
            .is_some();
        if !exists {
            return Err(S3ErrorCode::NoSuchBucket);
        }
        response(StatusCode::OK, Vec::new())
    }

    async fn list_buckets(&self) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let buckets = bucket::list_buckets(&self.storage.metadata, &self.config.tenant, DEFAULT_LIST_LIMIT)
            .await
            .map_err(|error| map_bucket_error(&error))?;
        xml_response(wire::list_buckets(self.config.tenant.as_bytes(), &buckets))
    }

    async fn delete_bucket(&self, route: S3Route) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let name = required_bucket(&route)?;
        match bucket::delete_bucket(&self.storage.metadata, &self.config.tenant, name)
            .await
            .map_err(|error| map_bucket_error(&error))?
        {
            DeleteBucketResult::Deleted => response(StatusCode::NO_CONTENT, Vec::new()),
            DeleteBucketResult::Missing => Err(S3ErrorCode::NoSuchBucket),
            DeleteBucketResult::NotEmpty => Err(S3ErrorCode::BucketNotEmpty),
            DeleteBucketResult::Conflict => Err(S3ErrorCode::ServiceUnavailable),
        }
    }

    async fn put_object(
        &self,
        route: S3Route,
        request: Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket_name = required_bucket(&route)?;
        let key = required_key(&route)?.to_vec();
        let bucket_id = self.resolve_bucket(bucket_name).await?;
        let content_length = content_length(&request)?;
        let content_type = header(&request, CONTENT_TYPE)
            .unwrap_or("application/octet-stream")
            .to_owned();
        let content_md5 =
            strict_header(&request, "content-md5", S3ErrorCode::InvalidDigest)?.map(str::to_owned);
        let payload_sha256 = strict_header(&request, "x-amz-content-sha256", S3ErrorCode::InvalidRequest)?
            .filter(|value| *value != "UNSIGNED-PAYLOAD")
            .map(str::to_owned);
        let mut route_key =
            Vec::with_capacity(self.config.tenant.as_bytes().len() + bucket_id.as_bytes().len() + key.len());
        route_key.extend_from_slice(self.config.tenant.as_bytes());
        route_key.extend_from_slice(bucket_id.as_bytes());
        route_key.extend_from_slice(&key);
        let mut writer = self.prepare_writer(content_length, &route_key).await?;
        let mut body = request.into_body();
        let (etag, checksum) = match write_body_with_checksums(
            &mut body,
            &mut writer,
            content_md5.as_deref(),
            payload_sha256.as_deref(),
        )
        .await
        {
            Ok(result) => result,
            Err(outcome) => {
                let _ = writer.on_error().await;
                return Err(map_put_outcome(&outcome));
            }
        };
        let locations = writer
            .on_finish()
            .await
            .map_err(|_| S3ErrorCode::ServiceUnavailable)?;
        let logical_length = locations.iter().map(|location| location.logical_length).sum();
        if content_length.is_some_and(|expected| expected != logical_length) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let now = unix_millis();
        let mut publication = PublicationRequest {
            tenant: self.config.tenant.clone(),
            object: ObjectRecord {
                bucket_id,
                key,
                logical_length,
                checksum,
                etag: etag.clone(),
                created_at_ms: now,
                modified_at_ms: now,
                content_type,
                attributes: Vec::new(),
                data_reference: vec![0],
                data_length: logical_length,
            },
        };
        match publish_completed_locations(&self.storage.metadata, &mut publication, &locations).await {
            PutOutcome::Success => Response::builder()
                .status(StatusCode::OK)
                .header(ETAG, format!("\"{etag}\""))
                .body(full_body(Vec::new().into()))
                .map_err(|_| S3ErrorCode::InternalError),
            outcome => Err(map_put_outcome(&outcome)),
        }
    }

    async fn head_object(
        &self,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let record = self.object(&route).await?;
        let headers = match retrieval::prepare_head(&record, &conditions(request)) {
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
        let prepared = match retrieval::prepare_get(
            &self.storage.chunks,
            &record,
            header(request, RANGE),
            &conditions(request),
        ) {
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
        let max_keys = query
            .text("max-keys")
            .map_or(Ok(DEFAULT_LIST_LIMIT), |value| value.parse::<usize>())
            .map_err(|_| S3ErrorCode::InvalidRequest)?
            .min(DEFAULT_LIST_LIMIT);
        let page = object::list_v2(
            &self.storage.metadata,
            &self.signer,
            &ListObjectsV2Request {
                tenant: &self.config.tenant,
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
        .await
        .map_err(|error| map_object_error(&error))?;
        xml_response(wire::list_objects(
            bucket_name,
            &prefix,
            delimiter.as_deref(),
            max_keys,
            &page,
        ))
    }

    async fn delete_object(&self, route: S3Route) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket_name = required_bucket(&route)?;
        let key = required_key(&route)?;
        let bucket_id = self.resolve_bucket(bucket_name).await?;
        object::delete(&self.storage.metadata, &self.config.tenant, bucket_id, key)
            .await
            .map_err(|error| map_object_error(&error))?;
        response(StatusCode::NO_CONTENT, Vec::new())
    }

    async fn resolve_bucket(&self, name: &[u8]) -> Result<crowdb_access_s3::metadata::BucketId, S3ErrorCode> {
        bucket::head_bucket(&self.storage.metadata, &self.config.tenant, name)
            .await
            .map_err(|error| map_bucket_error(&error))?
            .ok_or(S3ErrorCode::NoSuchBucket)
    }

    async fn object(&self, route: &S3Route) -> Result<ObjectRecord, S3ErrorCode> {
        let bucket = self.resolve_bucket(required_bucket(route)?).await?;
        object::head(
            &self.storage.metadata,
            &self.config.tenant,
            bucket,
            required_key(route)?,
        )
        .await
        .map_err(|error| map_object_error(&error))?
        .ok_or(S3ErrorCode::NoSuchKey)
    }

    async fn prepare_writer(
        &self,
        content_length: Option<u64>,
        key: &[u8],
    ) -> Result<ObjectWriter, S3ErrorCode> {
        if let Some(length) = content_length
            .and_then(|length| usize::try_from(length).ok())
            .filter(|length| *length <= self.config.small_object_limit)
        {
            match self.storage.chunks.prepare_small_write_for_key(length, key).await {
                Ok(writer) => return Ok(ObjectWriter::Small(writer)),
                Err(IoError::ObjectTooLarge { .. }) => {}
                Err(IoError::MemoryBudgetExhausted) => return Err(S3ErrorCode::SlowDown),
                Err(_) => return Err(S3ErrorCode::ServiceUnavailable),
            }
        }
        let mut prepared = self
            .storage
            .chunks
            .prepare_large_write(content_length, self.config.large_write.clone());
        prepared
            .wait_until_prepared()
            .await
            .map_err(|_| S3ErrorCode::ServiceUnavailable)?;
        Ok(ObjectWriter::Large(Box::new(prepared)))
    }
}

impl S3Operations for ProductionS3Operations {
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

enum ObjectWriter {
    Small(SharedObjectWriter),
    Large(Box<PreparedLargeWrite>),
}

#[async_trait::async_trait]
impl ChunkIoWriter for ObjectWriter {
    async fn on_data(
        &mut self,
        buffer: hyper::body::Bytes,
    ) -> crowdb_chunk_client::Result<crowdb_chunk_client::FeedStatus> {
        match self {
            Self::Small(writer) => writer.on_data(buffer).await,
            Self::Large(writer) => writer.on_data(buffer).await,
        }
    }

    async fn on_finish(&mut self) -> crowdb_chunk_client::Result<Vec<crowdb_chunk_client::ProtoLocation>> {
        match self {
            Self::Small(writer) => writer.on_finish().await,
            Self::Large(writer) => writer.on_finish().await,
        }
    }

    async fn on_error(&mut self) -> crowdb_chunk_client::Result<Vec<crowdb_chunk_client::ProtoLocation>> {
        match self {
            Self::Small(writer) => writer.on_error().await,
            Self::Large(writer) => writer.on_error().await,
        }
    }

    fn require_data(&self) -> bool {
        match self {
            Self::Small(writer) => writer.require_data(),
            Self::Large(writer) => writer.require_data(),
        }
    }

    fn input_complete(&self) -> bool {
        match self {
            Self::Small(writer) => writer.input_complete(),
            Self::Large(writer) => writer.input_complete(),
        }
    }

    async fn wait_for_capacity(&mut self) {
        match self {
            Self::Small(writer) => writer.wait_for_capacity().await,
            Self::Large(writer) => writer.wait_for_capacity().await,
        }
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

fn map_bucket_error(error: &BucketError) -> S3ErrorCode {
    match error {
        BucketError::Key(_) => S3ErrorCode::InvalidRequest,
        BucketError::Record(_) => S3ErrorCode::InternalError,
        BucketError::Store(_) => S3ErrorCode::ServiceUnavailable,
    }
}

fn map_object_error(error: &ObjectMetadataError) -> S3ErrorCode {
    match error {
        ObjectMetadataError::Key(_)
        | ObjectMetadataError::Continuation(_)
        | ObjectMetadataError::InvalidListLimit => S3ErrorCode::InvalidRequest,
        ObjectMetadataError::Record(_) => S3ErrorCode::InternalError,
        ObjectMetadataError::Store(_) => S3ErrorCode::ServiceUnavailable,
    }
}

fn map_put_outcome(outcome: &PutOutcome) -> S3ErrorCode {
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
