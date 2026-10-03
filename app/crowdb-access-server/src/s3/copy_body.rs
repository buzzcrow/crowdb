// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Copy progress owns its future until success/error XML is delivered or the body is dropped.

use super::{BoxError, ResponseBody};
use crowdb_access_s3::{S3Error, S3ErrorCode};
use futures::stream;
use http_body_util::{BodyExt, StreamBody};
use hyper::{
    body::{Bytes, Frame},
    Response,
};
use std::future::Future;
use std::time::Duration;

/// Sends bounded whitespace keepalives and the final copy result or embedded error.
/// # Errors
/// Rejects an invalid response builder.
pub fn response(
    future: impl Future<Output = Result<String, S3ErrorCode>> + Send + 'static,
    resource: String,
) -> Result<Response<ResponseBody>, S3ErrorCode> {
    let future = Box::pin(async move {
        match tokio::time::timeout(Duration::from_secs(300), future).await {
            Ok(result) => result,
            Err(_) => Err(S3ErrorCode::ServiceUnavailable),
        }
    });
    let frames = stream::unfold((Some(future), resource), |(future, resource)| async move {
        let mut future = future?;
        tokio::select! {
            result = &mut future => {
                let xml = result.unwrap_or_else(|code| S3Error::new(code, resource.clone(), String::new(), String::new()).to_xml());
                // XML declarations must precede whitespace, so emit only the root after keepalives.
                let xml = xml.split_once("?>").map_or(xml.as_str(), |(_, root)| root).to_owned();
                Some((Ok::<_, BoxError>(Frame::data(Bytes::from(xml))), (None, resource)))
            }
            () = tokio::time::sleep(Duration::from_secs(1)) => {
                Some((Ok(Frame::data(Bytes::from_static(b"\n"))), (Some(future), resource)))
            }
        }
    });
    Response::builder()
        .header("content-type", "application/xml")
        .body(StreamBody::new(sync_wrapper::SyncStream::new(frames)).boxed())
        .map_err(|_| S3ErrorCode::InternalError)
}
