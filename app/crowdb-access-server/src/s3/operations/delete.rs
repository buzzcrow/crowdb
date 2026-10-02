// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unversioned multi-object deletion with validation before mutation.

use crowdb_access_s3::delete::{DeleteSelection, MAX_DELETE_BODY};
use crowdb_access_s3::error::S3ErrorCode;
use crowdb_access_s3::{object, route::S3Route, wire};
use http_body_util::BodyExt as _;
use hyper::{
    body::{Bytes, Incoming},
    Request, Response,
};

use super::{
    map_object_error, required_bucket, strict_header, xml_response, ProductionS3Operations, ResponseBody,
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
        if declared.is_some_and(|length| length > MAX_DELETE_BODY) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let headers = request.headers().clone();
        let mut bytes = Vec::new();
        let mut body = request.into_body();
        while let Some(frame) = body.frame().await {
            let data = frame
                .map_err(|_| S3ErrorCode::InvalidRequest)?
                .into_data()
                .map_err(|_| S3ErrorCode::InvalidRequest)?;
            if bytes.len().saturating_add(data.len()) > MAX_DELETE_BODY {
                return Err(S3ErrorCode::InvalidRequest);
            }
            bytes.extend_from_slice(&data);
        }
        if declared.is_some_and(|length| length != bytes.len()) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let bytes = Bytes::from(bytes);
        crowdb_access_s3::delete::validate_integrity(&headers, &bytes)?;
        let selection = DeleteSelection::parse(&bytes)?;
        // Sequential work bounds concurrency and preserves duplicate-key order.
        let results = selection
            .execute(|key| async move {
                object::delete(&self.storage.metadata, &self.config.tenant, bucket, &key)
                    .await
                    .map_err(|error| map_object_error(&error))
            })
            .await?;
        xml_response(wire::delete_objects(&results, selection.quiet))
    }
}
