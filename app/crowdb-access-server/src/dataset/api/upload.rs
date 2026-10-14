// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{error::ApiError, json, operation, read_json, ApiResult};
use crate::dataset::{unix_seconds, DatasetHttpService};
use crowdb_access_dataset::{DatasetIdentity, DatasetUploads, UploadRecord};
use hyper::{body::Incoming, Method, Request};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct UploadRequest {
    id: String,
    length: u64,
    part_size: u32,
    md5: String,
}
#[derive(Serialize)]
struct Part {
    index: u32,
    length: u64,
    md5: String,
}
#[derive(Serialize)]
struct Upload {
    id: String,
    length: u64,
    part_size: u32,
    md5: String,
    state: crowdb_access_dataset::UploadState,
    expires_at: u64,
    parts: Vec<Part>,
    cleanup_complete: bool,
}
impl DatasetHttpService {
    pub(super) async fn upload_route(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        self.reads.authority().get_dataset(identity).await?;
        let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
        match (request.method().clone(), tail) {
            (Method::POST, ["uploads"]) => {
                let input: UploadRequest = read_json(request).await?;
                let location = DatasetUploads::location(identity, operation(&input.id)?)?;
                let md5 = digest(&input.md5)?;
                let record = uploads
                    .create(
                        &location,
                        input.length,
                        input.part_size,
                        md5,
                        unix_seconds().saturating_add(86400),
                    )
                    .await?;
                json(&wire(input.id, record))
            }
            (Method::GET, ["uploads", id]) => {
                let location = DatasetUploads::location(identity, operation(id)?)?;
                json(&wire((*id).into(), uploads.get(&location).await?))
            }
            (Method::POST, ["uploads", id, "complete"]) => {
                super::read_body_limit(request, false, 4 * 1024 * 1024).await?;
                let location = DatasetUploads::location(identity, operation(id)?)?;
                json(&wire(
                    (*id).into(),
                    uploads.complete(&location, unix_seconds()).await?,
                ))
            }
            (Method::DELETE, ["uploads", id]) => {
                super::read_body_limit(request, false, 4 * 1024 * 1024).await?;
                let location = DatasetUploads::location(identity, operation(id)?)?;
                json(&wire((*id).into(), uploads.abort(&location).await?))
            }
            (Method::PUT, ["uploads", id, "parts", index]) => {
                if request
                    .headers()
                    .get(hyper::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    != Some("application/octet-stream")
                {
                    return Err(ApiError::invalid());
                }
                let location = DatasetUploads::location(identity, operation(id)?)?;
                let index = index.parse::<u32>().map_err(|_| ApiError::invalid())?;
                let data = super::read_body_limit(request, false, 4 * 1024 * 1024).await?;
                if data.len() > crowdb_access_dataset::MAX_UPLOAD_PART_BYTES {
                    return Err(ApiError::too_large());
                }
                let part = uploads.put_part(&location, index, data, unix_seconds()).await?;
                let receipt = Part {
                    index: part.index,
                    length: part.length,
                    md5: hex::encode(part.md5),
                };
                json(&receipt)
            }
            _ => Err(ApiError::not_found()),
        }
    }
}
fn digest(value: &str) -> Result<[u8; 16], ApiError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ApiError::invalid());
    }
    let mut digest = [0; 16];
    hex::decode_to_slice(value, &mut digest).map_err(|_| ApiError::invalid())?;
    Ok(digest)
}
fn wire(id: String, record: UploadRecord) -> Upload {
    Upload {
        id,
        length: record.length,
        part_size: record.part_size,
        md5: hex::encode(record.md5),
        state: record.state,
        expires_at: record.expires_at,
        cleanup_complete: record.cleanup_complete,
        parts: record
            .parts
            .into_iter()
            .map(|part| Part {
                index: part.index,
                length: part.length,
                md5: hex::encode(part.md5),
            })
            .collect(),
    }
}
