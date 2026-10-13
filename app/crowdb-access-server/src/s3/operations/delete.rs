// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unversioned multi-object deletion with validation before mutation.

use crowdb_access_s3::delete::{DeleteSelection, MAX_DELETE_BODY};
use crowdb_access_s3::error::S3ErrorCode;
use crowdb_access_s3::{object, route::S3Route, wire};
use futures::{stream, StreamExt};
use http_body_util::BodyExt as _;
use http_body_util::StreamBody;
use hyper::{
    body::{Bytes, Frame, Incoming},
    header::CONTENT_TYPE,
    Request, Response, StatusCode,
};

use super::{
    map_object_error, required_bucket, strict_header, BoxError, ProductionS3Operations, ResponseBody,
};

impl ProductionS3Operations {
    pub(super) async fn delete_objects(
        &self,
        route: S3Route,
        request: Request<Incoming>,
    ) -> Result<Response<ResponseBody>, S3ErrorCode> {
        let bucket = self.resolve_bucket(required_bucket(&route)?).await?;
        let declared = strict_header(&request, "content-length", S3ErrorCode::InvalidRequest)?
            .map(str::parse::<usize>)
            .transpose()
            .map_err(|_| S3ErrorCode::InvalidRequest)?;
        let headers = request.headers().clone();
        let mut bytes = Vec::new();
        let mut oversized = declared.is_some_and(|length| length > MAX_DELETE_BODY);
        let mut body = request.into_body();
        while let Some(frame) = body.frame().await {
            let data = frame
                .map_err(|_| S3ErrorCode::InvalidRequest)?
                .into_data()
                .map_err(|_| S3ErrorCode::InvalidRequest)?;
            if oversized {
                continue;
            }
            if bytes.len().saturating_add(data.len()) > MAX_DELETE_BODY {
                oversized = true;
                bytes.clear();
                continue;
            }
            bytes.extend_from_slice(&data);
        }
        if oversized {
            return Err(S3ErrorCode::InvalidRequest);
        }
        if declared.is_some_and(|length| length != bytes.len()) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let bytes = Bytes::from(bytes);
        crowdb_access_s3::delete::validate_integrity(&headers, &bytes)?;
        let selection = DeleteSelection::parse(&bytes)?;
        let metadata = self.storage.metadata.clone();
        let tenant = self.config.tenant.clone();
        let quiet = selection.quiet;
        let results = selection.stream(move |key| {
            let metadata = metadata.clone();
            let tenant = tenant.clone();
            async move {
                object::delete(&metadata, &tenant, bucket, &key)
                    .await
                    .map_err(|error| map_object_error(&error))
            }
        })?;
        // Send progress while preserving one ordered mutation at a time.
        // Quiet successes emit XML whitespace so idle-read deadlines still see progress.
        let results = results.map(move |(key, result)| {
            let xml = wire::delete_object_result(&key, &result, quiet);
            let bytes = if xml.is_empty() {
                Bytes::from_static(b"\n")
            } else {
                Bytes::from(xml)
            };
            Ok::<_, BoxError>(Frame::data(bytes))
        });
        let frames = stream::once(std::future::ready(Ok::<_, BoxError>(Frame::data(Bytes::from(
            wire::delete_objects_start(),
        )))))
        .chain(results)
        .chain(stream::once(std::future::ready(Ok::<_, BoxError>(Frame::data(
            Bytes::from_static(b"</DeleteResult>"),
        )))));
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/xml")
            .body(http_body_util::BodyExt::boxed(StreamBody::new(
                sync_wrapper::SyncStream::new(frames),
            )))
            .map_err(|_| S3ErrorCode::InternalError)
    }
}
