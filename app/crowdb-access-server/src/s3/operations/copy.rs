// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Streamed copies capture one source record and publish only after completion.

use crowdb_access_s3::condition::ObjectConditions;
use crowdb_access_s3::error::S3ErrorCode;
use crowdb_access_s3::integrity::SinglePartIntegrity;
use crowdb_access_s3::metadata::{MultipartPartRecord, ObjectRecord};
use crowdb_access_s3::publication::PublicationRequest;
use crowdb_access_s3::route::{S3Operation, S3Route};
use crowdb_access_s3::streaming::{publish_completed_locations, PutOutcome};
use crowdb_access_s3::{copy, retrieval, wire};
use crowdb_chunk_client::{ChunkIoWriter, ProtoLocation};
use hyper::{body::Incoming, Request, Response};
use std::sync::Arc;

use super::multipart::{map_multipart_error, parse_md5};
use super::{
    map_put_outcome, map_retrieval, required_bucket, required_key, strict_header, unix_millis, ObjectWriter,
    ProductionS3Operations, ResponseBody,
};

impl ProductionS3Operations {
    async fn copy_source(
        &self,
        request: &Request<Incoming>,
        part: bool,
    ) -> Result<ObjectRecord, S3ErrorCode> {
        copy::validate_query(request.uri().query(), part)?;
        let signed = strict_header(request, "authorization", S3ErrorCode::AccessDenied)?
            .and_then(|value| value.split("SignedHeaders=").nth(1))
            .and_then(|value| value.split(',').next())
            .map(str::trim)
            .map(str::to_owned)
            .or_else(|| super::Query::new(request.uri().query()).text("X-Amz-SignedHeaders"))
            .ok_or(S3ErrorCode::AccessDenied)?;
        copy::validate_headers(request.headers(), &signed, part)?;
        if strict_header(request, "content-length", S3ErrorCode::InvalidRequest)?
            .is_some_and(|length| length != "0")
        {
            return Err(S3ErrorCode::InvalidRequest);
        }
        if strict_header(request, "x-amz-content-sha256", S3ErrorCode::InvalidRequest)?.is_some_and(
            |digest| {
                digest != "UNSIGNED-PAYLOAD"
                    && digest != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            },
        ) {
            return Err(S3ErrorCode::XAmzContentSHA256Mismatch);
        }
        let value = strict_header(request, "x-amz-copy-source", S3ErrorCode::InvalidRequest)?
            .ok_or(S3ErrorCode::InvalidRequest)?;
        let (bucket, key) = copy::source(value)?;
        let source = S3Route {
            operation: S3Operation::GetObject,
            bucket: Some(bucket),
            key: Some(key),
            upload_id: None,
            part_number: None,
        };
        let record = self.object(&source).await?;
        let conditions = source_conditions(request)?;
        copy::check_conditions(&record, &conditions)?;
        Ok(record)
    }

    pub(super) async fn copy_object(
        self: &Arc<Self>,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        if request.headers().contains_key("x-amz-copy-source-range") {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let replace = match strict_header(request, "x-amz-metadata-directive", S3ErrorCode::InvalidRequest)? {
            None | Some("COPY") => false,
            Some("REPLACE") => true,
            _ => return Err(S3ErrorCode::InvalidRequest),
        };
        let attributes =
            crowdb_access_s3::metadata::UserMetadata::from_headers(request.headers())?.encode()?;
        let mut record = self.copy_source(request, false).await?;
        let destination = self.resolve_bucket(required_bucket(&route)?).await?;
        let key = required_key(&route)?.to_vec();
        copy::validate_object(&record, destination, &key, replace)?;
        let content_type = if replace {
            strict_header(request, "content-type", S3ErrorCode::InvalidRequest)?
                .unwrap_or("application/octet-stream")
                .to_owned()
        } else {
            record.content_type.clone()
        };
        let mut writer_key = self.config.tenant.as_bytes().to_vec();
        writer_key.extend_from_slice(destination.as_bytes());
        writer_key.extend_from_slice(&key);
        let source = record.clone();
        record.bucket_id = destination;
        record.key = key;
        record.content_type = content_type;
        if replace {
            record.attributes = attributes;
        }
        let operations = self.clone();
        crate::s3::copy_body::response(
            async move { operations.finish_object_copy(source, record, writer_key).await },
            request.uri().path().to_owned(),
        )
    }

    async fn finish_object_copy(
        &self,
        source: ObjectRecord,
        mut record: ObjectRecord,
        writer_key: Vec<u8>,
    ) -> Result<String, S3ErrorCode> {
        let (etag, checksum, locations) = self.stream_copy(&source, None, &writer_key).await?;
        let now = unix_millis();
        record.etag = etag.clone();
        record.checksum = checksum;
        record.created_at_ms = now;
        record.modified_at_ms = now;
        let mut publication = PublicationRequest {
            tenant: self.config.tenant.clone(),
            object: record,
        };
        match publish_completed_locations(&self.storage.metadata, &mut publication, &locations).await {
            PutOutcome::Success => Ok(wire::copy_result(&etag, now, false)),
            outcome => Err(map_put_outcome(&outcome)),
        }
    }

    pub(super) async fn upload_part_copy(
        self: &Arc<Self>,
        route: S3Route,
        request: &Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let session = self.multipart_identity(&route).await?;
        let number = route.part_number.ok_or(S3ErrorCode::InvalidRequest)?;
        let record = self.copy_source(request, true).await?;
        let range = strict_header(request, "x-amz-copy-source-range", S3ErrorCode::InvalidRequest)?;
        if let Some(range) = range {
            copy::validate_range(range, record.logical_length)?;
        }
        let prepared =
            retrieval::prepare_get(&self.storage.chunks, &record, range, &ObjectConditions::default())
                .map_err(|error| map_retrieval(&error))?;
        let length = prepared.headers.content_length;
        if length > session.max_part_bytes {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let mut writer_key = self.config.tenant.as_bytes().to_vec();
        writer_key.extend_from_slice(session.bucket_id.as_bytes());
        writer_key.extend_from_slice(&session.upload_id);
        writer_key.extend_from_slice(&number.to_be_bytes());
        let range = range.map(str::to_owned);
        let operations = self.clone();
        crate::s3::copy_body::response(
            async move {
                let (etag, _, locations) = operations
                    .stream_copy(&record, range.as_deref(), &writer_key)
                    .await?;
                let now = unix_millis();
                let part = MultipartPartRecord {
                    bucket_id: session.bucket_id,
                    upload_id: session.upload_id,
                    number,
                    revision: 1,
                    modified_ms: now,
                    length,
                    raw_md5: parse_md5(&etag)?,
                    locations,
                };
                operations
                    .multipart()
                    .put_stream_part(&session, &part, now)
                    .await
                    .map_err(|error| map_multipart_error(&error))?
                    .ok_or(S3ErrorCode::SlowDown)?;
                Ok(wire::copy_result(&etag, now, true))
            },
            request.uri().path().to_owned(),
        )
    }

    async fn stream_copy(
        &self,
        record: &ObjectRecord,
        range: Option<&str>,
        key: &[u8],
    ) -> Result<(String, Vec<u8>, Vec<ProtoLocation>), S3ErrorCode> {
        let mut prepared =
            retrieval::prepare_get(&self.storage.chunks, record, range, &ObjectConditions::default())
                .map_err(|error| map_retrieval(&error))?;
        let length = prepared.headers.content_length;
        let mut writer = self.prepare_writer(Some(length), key).await?;
        let result = feed_copy(&mut prepared.stream, &mut writer, length).await;
        match result {
            Ok((etag, checksum)) => {
                let Ok(locations) = writer.on_finish().await else {
                    let _ = writer.on_error().await;
                    return Err(S3ErrorCode::ServiceUnavailable);
                };
                Ok((etag, checksum, locations))
            }
            Err(error) => {
                let _ = writer.on_error().await;
                Err(error)
            }
        }
    }
}

async fn feed_copy(
    stream: &mut Option<retrieval::VerifiedGetStream>,
    writer: &mut ObjectWriter,
    length: u64,
) -> Result<(String, Vec<u8>), S3ErrorCode> {
    let mut integrity = SinglePartIntegrity::default();
    let mut actual = 0_u64;
    if let Some(stream) = stream {
        while let Some(bytes) = stream.next_chunk().await {
            let bytes = bytes.map_err(|_| S3ErrorCode::ServiceUnavailable)?;
            actual = actual
                .checked_add(bytes.len() as u64)
                .ok_or(S3ErrorCode::InternalError)?;
            integrity.update(&bytes);
            while !writer.require_data() {
                writer.wait_for_capacity().await;
            }
            writer
                .on_data(bytes)
                .await
                .map_err(|_| S3ErrorCode::ServiceUnavailable)?;
        }
    }
    if actual != length {
        return Err(S3ErrorCode::InternalError);
    }
    Ok(integrity.finish())
}

fn source_conditions(request: &Request<Incoming>) -> Result<ObjectConditions<'_>, S3ErrorCode> {
    Ok(ObjectConditions {
        if_match: strict_header(request, "x-amz-copy-source-if-match", S3ErrorCode::InvalidRequest)?,
        if_none_match: strict_header(
            request,
            "x-amz-copy-source-if-none-match",
            S3ErrorCode::InvalidRequest,
        )?,
        if_modified_since: strict_header(
            request,
            "x-amz-copy-source-if-modified-since",
            S3ErrorCode::InvalidRequest,
        )?,
        if_unmodified_since: strict_header(
            request,
            "x-amz-copy-source-if-unmodified-since",
            S3ErrorCode::InvalidRequest,
        )?,
    })
}
