// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated S3 multipart operations over durable session and part records.

use crowdb_access_s3::error::S3ErrorCode;
use crowdb_access_s3::execution::{CopyMetadata, S3WritePolicy};
use crowdb_access_s3::integrity::{IntegrityError, SinglePartIntegrity};
use crowdb_access_s3::metadata::{
    CompletionPart, MultipartExpiryError, MultipartExpiryStep, MultipartExpirySweep, MultipartRepository,
    MultipartRepositoryError, MultipartSessionRecord,
};
use crowdb_access_s3::route::{S3Operation, S3Route};
use crowdb_access_s3::wire;
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

const MAX_COMPLETE_BODY: usize = 2 * 1024 * 1024;

impl ProductionS3Operations {
    /// Starts discardable expiry positions for the configured tenant.
    #[must_use]
    pub fn multipart_expiry_sweep(&self) -> MultipartExpirySweep {
        MultipartExpirySweep::new(self.storage.metadata.clone(), self.config.tenant.clone())
    }

    /// Marks expired sessions within one bounded background window.
    ///
    /// # Errors
    /// Defers the sweep when bucket or upload metadata is unavailable.
    pub async fn expire_multipart_uploads(
        &self,
        sweep: &mut MultipartExpirySweep,
    ) -> Result<MultipartExpiryStep, MultipartExpiryError> {
        sweep.step(unix_millis()).await
    }

    pub(super) fn multipart(&self) -> MultipartRepository {
        self.execution.multipart_repository()
    }

    pub(super) async fn multipart_identity(
        &self,
        route: &S3Route,
    ) -> Result<MultipartSessionRecord, S3ErrorCode> {
        self.execution
            .multipart_session(
                required_bucket(route)?,
                required_key(route)?,
                &route.upload_id.ok_or(S3ErrorCode::InvalidRequest)?,
                route.operation == S3Operation::AbortMultipartUpload,
            )
            .await
            .map_err(|error| error.code())
    }

    pub(super) async fn create_multipart_upload(
        &self,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket_name = required_bucket(&route)?;
        let key = required_key(&route)?;
        if content_length(request)?.is_some_and(|length| length != 0) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let session = self
            .execution
            .begin_multipart(
                bucket_name,
                key,
                CopyMetadata {
                    content_type: strict_header(request, "content-type", S3ErrorCode::InvalidRequest)?
                        .unwrap_or("application/octet-stream")
                        .to_owned(),
                    user_metadata: crowdb_access_s3::metadata::UserMetadata::from_headers(request.headers())?,
                },
            )
            .await
            .map_err(|error| error.code())?;
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
        let streaming = super::take_streaming(&mut request)?;
        let length =
            super::upload_length(&request, streaming.is_some())?.ok_or(S3ErrorCode::InvalidRequest)?;
        if length > session.max_part_bytes {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let content_md5 = super::content_md5_header(&request)?.map(str::to_owned);
        let payload_sha256 = strict_header(&request, "x-amz-content-sha256", S3ErrorCode::InvalidRequest)?
            .filter(|value| *value != "UNSIGNED-PAYLOAD" && streaming.is_none())
            .map(str::to_owned);
        let mut part = self
            .execution
            .prepare_part(
                &session,
                number,
                length,
                &S3WritePolicy {
                    small_object_limit: self.config.small_object_limit,
                    large_write: self.config.large_write.clone(),
                },
            )
            .await
            .map_err(|error| error.code())?;
        let native_receiver = if part.is_large() && streaming.is_none() {
            install_body_receive_provider(&mut request)
        } else {
            None
        };
        if let Some(receiver) = &native_receiver {
            receiver.enable_owner_handoff();
        }
        let (parts, body) = request.into_parts();
        let mut body = crate::upload_flow::body_encoding::UploadBody::new(
            body,
            &parts.headers,
            streaming,
            session.max_part_bytes.saturating_add(1024 * 1024),
        )
        .map_err(super::encoding_error)?;
        body.defer_md5();
        let written = write_object_body(
            &mut body,
            part.writer_mut(),
            native_receiver.as_deref(),
            Some(length),
            content_md5.as_deref(),
            payload_sha256.as_deref(),
            self.config.large_write.client.large_held_buffers,
            self.metrics.as_deref(),
        )
        .await;
        let (etag, checksum) = match written {
            Ok(value) => value,
            Err(outcome) => {
                part.abort().await;
                return Err(body
                    .failure()
                    .map_or_else(|| map_put_outcome(&outcome), super::encoding_error));
            }
        };
        part.finish(checksum).await.map_err(|error| error.code())?;
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
        self.execution
            .abort_multipart(&session)
            .await
            .map_err(|error| error.code())?;
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
        let published = self
            .execution
            .complete_multipart(&session, &requested)
            .await
            .map_err(|error| error.code())?;
        xml_response(wire::complete_multipart_upload(
            &location,
            required_bucket(&route)?,
            required_key(&route)?,
            &published.etag,
        ))
    }
}

pub(super) fn map_multipart_error(error: &MultipartRepositoryError) -> S3ErrorCode {
    crowdb_access_s3::execution::map_multipart_error(error)
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
