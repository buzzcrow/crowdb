// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated S3 multipart operations over durable session and part records.

use crowdb_access_s3::bucket;
use crowdb_access_s3::error::S3ErrorCode;
use crowdb_access_s3::integrity::{IntegrityError, SinglePartIntegrity};
use crowdb_access_s3::metadata::{
    new_upload_id, CompletionPart, MultipartPartRecord, MultipartPhase, MultipartRepository,
    MultipartRepositoryError, MultipartSessionRecord,
};
use crowdb_access_s3::route::{S3Operation, S3Route};
use crowdb_access_s3::wire;
use crowdb_chunk_client::ChunkIoWriter;
use http_body_util::BodyExt as _;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::header::ETAG;
use hyper::{Request, Response, StatusCode};

use super::{
    content_length, full_body, install_body_receive_provider, map_put_outcome, required_bucket, required_key,
    response, strict_header, unix_millis, write_object_body, xml_response, ProductionS3Operations, Query,
    ResponseBody,
};
use crate::multipart_complete::{CompleteRequestError, CompleteSelection};

const MAX_PARTS: u16 = 10_000;
const MAX_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;
const MAX_STAGED_BYTES: u64 = 10_000 * MAX_PART_BYTES;
const UPLOAD_LIFETIME_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const MAX_COMPLETE_BODY: usize = 2 * 1024 * 1024;

impl ProductionS3Operations {
    /// Walks bounded metadata pages and marks expired sessions terminal.
    ///
    /// # Errors
    /// Defers the sweep when bucket or upload metadata is unavailable.
    pub async fn expire_multipart_uploads(&self) -> Result<usize, S3ErrorCode> {
        let buckets = bucket::list_buckets(&self.storage.metadata, &self.config.tenant, 1_001)
            .await
            .map_err(|_| S3ErrorCode::ServiceUnavailable)?;
        if buckets.len() > 1_000 {
            return Err(S3ErrorCode::SlowDown);
        }
        let now = unix_millis();
        let mut expired = 0;
        for bucket in buckets {
            let mut cursor = None;
            loop {
                let page = self
                    .multipart()
                    .expire_page(bucket.bucket_id, cursor.as_deref(), now, 1_000)
                    .await
                    .map_err(|error| map_multipart_error(&error))?;
                expired += page.expired;
                let Some(next) = page.next else {
                    break;
                };
                cursor = Some(next);
                tokio::task::yield_now().await;
            }
        }
        Ok(expired)
    }

    fn multipart(&self) -> MultipartRepository {
        MultipartRepository::new(self.storage.metadata.clone(), self.config.tenant.clone())
    }

    async fn multipart_identity(&self, route: &S3Route) -> Result<MultipartSessionRecord, S3ErrorCode> {
        let bucket = self.resolve_bucket(required_bucket(route)?).await?;
        let key = required_key(route)?;
        let upload_id = route.upload_id.ok_or(S3ErrorCode::InvalidRequest)?;
        let session = self
            .multipart()
            .load_identity(bucket, key, &upload_id)
            .await
            .map_err(|error| map_multipart_error(&error))?
            .ok_or(S3ErrorCode::NoSuchUpload)?;
        if session.phase == MultipartPhase::Open && session.expires_ms <= unix_millis() {
            let expired = self
                .multipart()
                .abort(&session)
                .await
                .map_err(|error| map_multipart_error(&error))?;
            if route.operation != S3Operation::AbortMultipartUpload {
                return Err(S3ErrorCode::NoSuchUpload);
            }
            return Ok(expired);
        }
        Ok(session)
    }

    pub(super) async fn create_multipart_upload(
        &self,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket_name = required_bucket(&route)?;
        let key = required_key(&route)?;
        let bucket_id = self.resolve_bucket(bucket_name).await?;
        if content_length(request)?.is_some_and(|length| length != 0) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let now = unix_millis();
        let session = MultipartSessionRecord {
            bucket_id,
            object_key: key.to_vec(),
            upload_id: new_upload_id(now),
            revision: 1,
            phase: MultipartPhase::Open,
            created_ms: now,
            expires_ms: now
                .checked_add(UPLOAD_LIFETIME_MS)
                .ok_or(S3ErrorCode::InternalError)?,
            content_type: strict_header(request, "content-type", S3ErrorCode::InvalidRequest)?
                .unwrap_or("application/octet-stream")
                .to_owned(),
            max_parts: MAX_PARTS,
            max_part_bytes: MAX_PART_BYTES,
            max_object_bytes: MAX_OBJECT_BYTES,
            max_staged_bytes: MAX_STAGED_BYTES,
            part_count: 0,
            staged_bytes: 0,
            pending: None,
            selection: None,
            completion_request_digest: None,
            publication_ms: None,
            object_predecessor: None,
            etag: None,
        };
        self.multipart()
            .begin(&session)
            .await
            .map_err(|error| map_multipart_error(&error))?;
        xml_response(wire::create_multipart_upload(
            bucket_name,
            key,
            &session.upload_id,
        ))
    }

    pub(super) async fn upload_part(
        &self,
        route: S3Route,
        mut request: Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let session = self.multipart_identity(&route).await?;
        let number = route.part_number.ok_or(S3ErrorCode::InvalidRequest)?;
        let length = content_length(&request)?.ok_or(S3ErrorCode::InvalidRequest)?;
        if length > session.max_part_bytes {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let content_md5 =
            strict_header(&request, "content-md5", S3ErrorCode::InvalidDigest)?.map(str::to_owned);
        let payload_sha256 = strict_header(&request, "x-amz-content-sha256", S3ErrorCode::InvalidRequest)?
            .filter(|value| *value != "UNSIGNED-PAYLOAD")
            .map(str::to_owned);
        let mut route_key = self.config.tenant.as_bytes().to_vec();
        route_key.extend_from_slice(session.bucket_id.as_bytes());
        route_key.extend_from_slice(&session.upload_id);
        route_key.extend_from_slice(&number.to_be_bytes());
        let mut writer = self.prepare_writer(Some(length), &route_key).await?;
        let native_receiver = if writer.is_large() {
            install_body_receive_provider(&mut request)
        } else {
            None
        };
        if let Some(receiver) = &native_receiver {
            receiver.enable_owner_handoff();
        }
        let mut body = request.into_body();
        let written = write_object_body(
            &mut body,
            &mut writer,
            native_receiver.as_deref(),
            Some(length),
            content_md5.as_deref(),
            payload_sha256.as_deref(),
            self.config.large_write.client.large_held_buffers,
            self.metrics.as_deref(),
        )
        .await;
        let (etag, _) = match written {
            Ok(value) => value,
            Err(outcome) => {
                let _ = writer.on_error().await;
                return Err(map_put_outcome(&outcome));
            }
        };
        let locations = writer
            .on_finish()
            .await
            .map_err(|_| S3ErrorCode::ServiceUnavailable)?;
        let actual: u64 = locations.iter().map(|location| location.logical_length).sum();
        if actual != length {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let part = MultipartPartRecord {
            bucket_id: session.bucket_id,
            upload_id: session.upload_id,
            number,
            revision: 1,
            modified_ms: unix_millis(),
            length,
            raw_md5: parse_md5(&etag)?,
            locations,
        };
        let _saved = self
            .multipart()
            .put_stream_part(&session, &part, part.modified_ms)
            .await
            .map_err(|error| map_multipart_error(&error))?
            .ok_or(S3ErrorCode::SlowDown)?;
        Response::builder()
            .status(StatusCode::OK)
            .header(ETAG, format!("\"{etag}\""))
            .body(full_body(Vec::new().into()))
            .map_err(|_| S3ErrorCode::InternalError)
    }

    pub(super) async fn list_parts(
        &self,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let session = self.multipart_identity(&route).await?;
        let query = Query::new(request.uri().query());
        let marker = parse_number(query.text("part-number-marker"), 0, 0, 10_000)?;
        let limit = parse_number(query.text("max-parts"), 1_000, 1, 1_000)?;
        let page = self
            .multipart()
            .list_parts(&session, marker, usize::from(limit))
            .await
            .map_err(|error| map_multipart_error(&error))?;
        xml_response(wire::list_multipart_parts(
            required_bucket(&route)?,
            required_key(&route)?,
            &session.upload_id,
            marker,
            usize::from(limit),
            &page,
        ))
    }

    pub(super) async fn list_multipart_uploads(
        &self,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket_name = required_bucket(&route)?;
        let bucket = self.resolve_bucket(bucket_name).await?;
        let query = Query::new(request.uri().query());
        let prefix = query.bytes("prefix").unwrap_or_default();
        let key_marker = query.bytes("key-marker");
        let upload_marker = query
            .text("upload-id-marker")
            .as_deref()
            .map(parse_upload_id)
            .transpose()?;
        let limit = parse_number(query.text("max-uploads"), 1_000, 1, 1_000)?;
        let page = self
            .multipart()
            .list_uploads(
                bucket,
                &prefix,
                key_marker.as_deref(),
                upload_marker.as_ref(),
                usize::from(limit),
                unix_millis(),
            )
            .await
            .map_err(|error| map_multipart_error(&error))?;
        xml_response(wire::list_multipart_uploads(
            bucket_name,
            &prefix,
            key_marker.as_deref(),
            upload_marker.as_ref(),
            usize::from(limit),
            &String::from_utf8_lossy(self.config.tenant.as_bytes()),
            &page,
        ))
    }

    pub(super) async fn abort_multipart_upload(
        &self,
        route: S3Route,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let session = self.multipart_identity(&route).await?;
        self.multipart()
            .abort(&session)
            .await
            .map_err(|error| map_multipart_error(&error))?;
        response(StatusCode::NO_CONTENT, Vec::new())
    }

    pub(super) async fn complete_multipart_upload(
        &self,
        route: S3Route,
        request: Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let session = self.multipart_identity(&route).await?;
        let location = request.uri().path().to_owned();
        let content_md5 =
            strict_header(&request, "content-md5", S3ErrorCode::InvalidDigest)?.map(str::to_owned);
        let payload_sha256 = strict_header(&request, "x-amz-content-sha256", S3ErrorCode::InvalidRequest)?
            .filter(|value| *value != "UNSIGNED-PAYLOAD")
            .map(str::to_owned);
        let bytes = read_completion(request.into_body()).await?;
        let mut integrity = SinglePartIntegrity::new(payload_sha256.is_some());
        integrity.update(&Bytes::copy_from_slice(&bytes));
        integrity
            .finish_validated_checksums(content_md5.as_deref(), payload_sha256.as_deref())
            .map_err(map_integrity_error)?;
        let selection = CompleteSelection::parse(&bytes).map_err(|error| match error {
            CompleteRequestError::InvalidRequest => S3ErrorCode::InvalidRequest,
            CompleteRequestError::InvalidPartOrder => S3ErrorCode::InvalidPartOrder,
        })?;
        let requested: Vec<CompletionPart> = selection
            .parts()
            .iter()
            .map(|part| CompletionPart {
                number: part.number,
                etag: part.etag.clone(),
            })
            .collect();
        let frozen = self
            .multipart()
            .freeze_completion(&session, &requested, unix_millis())
            .await
            .map_err(|error| map_multipart_error(&error))?
            .ok_or(S3ErrorCode::ServiceUnavailable)?;
        let published = self
            .multipart()
            .publish_completion(&frozen)
            .await
            .map_err(|error| map_multipart_error(&error))?;
        xml_response(wire::complete_multipart_upload(
            &location,
            required_bucket(&route)?,
            required_key(&route)?,
            published.etag.as_deref().ok_or(S3ErrorCode::InternalError)?,
        ))
    }
}

fn map_multipart_error(error: &MultipartRepositoryError) -> S3ErrorCode {
    match error {
        MultipartRepositoryError::Key(_) | MultipartRepositoryError::Record(_) => S3ErrorCode::InvalidRequest,
        MultipartRepositoryError::Store(_) => S3ErrorCode::ServiceUnavailable,
        MultipartRepositoryError::Conflict => S3ErrorCode::NoSuchUpload,
        MultipartRepositoryError::Busy | MultipartRepositoryError::ScanBudgetExhausted => {
            S3ErrorCode::SlowDown
        }
        MultipartRepositoryError::InvalidPart => S3ErrorCode::InvalidPart,
        MultipartRepositoryError::EntityTooSmall => S3ErrorCode::EntityTooSmall,
    }
}

fn map_integrity_error(error: IntegrityError) -> S3ErrorCode {
    match error {
        IntegrityError::InvalidDigest => S3ErrorCode::InvalidDigest,
        IntegrityError::Mismatch => S3ErrorCode::BadDigest,
        IntegrityError::InvalidPayloadDigest => S3ErrorCode::InvalidRequest,
        IntegrityError::PayloadMismatch => S3ErrorCode::XAmzContentSHA256Mismatch,
    }
}

fn parse_number(value: Option<String>, default: u16, minimum: u16, maximum: u16) -> Result<u16, S3ErrorCode> {
    value
        .map_or(Ok(default), |value| value.parse::<u16>())
        .map_err(|_| S3ErrorCode::InvalidRequest)
        .and_then(|number| {
            (number >= minimum && number <= maximum)
                .then_some(number)
                .ok_or(S3ErrorCode::InvalidRequest)
        })
}

fn parse_upload_id(value: &str) -> Result<[u8; 16], S3ErrorCode> {
    if value.len() != 32 {
        return Err(S3ErrorCode::InvalidRequest);
    }
    let mut id = [0; 16];
    for (byte, pair) in id.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *byte = u8::from_str_radix(
            std::str::from_utf8(pair).map_err(|_| S3ErrorCode::InvalidRequest)?,
            16,
        )
        .map_err(|_| S3ErrorCode::InvalidRequest)?;
    }
    (id != [0; 16]).then_some(id).ok_or(S3ErrorCode::InvalidRequest)
}

fn parse_md5(value: &str) -> Result<[u8; 16], S3ErrorCode> {
    if value.len() != 32 {
        return Err(S3ErrorCode::InternalError);
    }
    let mut md5 = [0; 16];
    for (byte, pair) in md5.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *byte = u8::from_str_radix(
            std::str::from_utf8(pair).map_err(|_| S3ErrorCode::InternalError)?,
            16,
        )
        .map_err(|_| S3ErrorCode::InternalError)?;
    }
    Ok(md5)
}

async fn read_completion(mut body: Incoming) -> Result<Vec<u8>, S3ErrorCode> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| S3ErrorCode::InvalidRequest)?;
        let data = frame.into_data().map_err(|_| S3ErrorCode::InvalidRequest)?;
        if bytes.len().saturating_add(data.len()) > MAX_COMPLETE_BODY {
            return Err(S3ErrorCode::InvalidRequest);
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes)
}
