// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{json, operation, read_body, read_json, ApiError, ApiResult, DatasetHttpService},
    query::Query,
    split_models::decimal,
};
use crowdb_access_dataset::{DatasetIdentity, DatasetUploads, UploadRecord};
use hyper::{body::Incoming, Method, Request};
use serde::Deserialize;
use serde_json::{json as value, Value};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Create {
    id: String,
    length: String,
    part_size: u32,
    md5: String,
}
impl DatasetHttpService {
    pub(super) async fn v2_uploads(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        self.reads.authority().get_dataset(identity).await?;
        let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
        let now = crate::dataset::unix_seconds();
        if tail == ["uploads"] && request.method() == Method::POST {
            let input: Create = read_json(request).await?;
            let location = DatasetUploads::location(identity, operation(&input.id)?)?;
            let record = uploads
                .create(
                    &location,
                    decimal(&input.length)?,
                    input.part_size,
                    digest(&input.md5)?,
                    now.saturating_add(86400),
                )
                .await?;
            return json(&wire(&input.id, record));
        }
        let ["uploads", id, rest @ ..] = tail else {
            return Err(ApiError::not_found());
        };
        let location = DatasetUploads::location(identity, operation(id)?)?;
        match (request.method().clone(), rest) {
            (Method::GET, []) => {
                read_body(request, false).await?;
                json(&wire(id, uploads.get(&location).await?))
            }
            (Method::POST, ["complete"]) => {
                read_body(request, false).await?;
                json(&wire(id, uploads.complete(&location, now).await?))
            }
            (Method::DELETE, []) => {
                read_body(request, false).await?;
                json(&wire(id, uploads.abort(&location).await?))
            }
            (Method::PUT, ["parts", index]) => {
                if request
                    .headers()
                    .get(hyper::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    != Some("application/octet-stream")
                {
                    return Err(ApiError::invalid());
                }
                let index = u32::try_from(decimal(index)?).map_err(|_| ApiError::invalid())?;
                let data = super::super::read_body_limit(
                    request,
                    false,
                    crowdb_access_dataset::MAX_UPLOAD_PART_BYTES,
                )
                .await?;
                let part = uploads.put_part(&location, index, data, now).await?;
                json(
                    &value!({"index":part.index,"length":part.length.to_string(),"md5":hex::encode(part.md5)}),
                )
            }
            _ => Err(ApiError::not_found()),
        }
    }
}
pub(super) fn digest(value: &str) -> Result<[u8; 16], ApiError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ApiError::invalid());
    }
    let mut bytes = [0; 16];
    hex::decode_to_slice(value, &mut bytes).map_err(|_| ApiError::invalid())?;
    Ok(bytes)
}
fn wire(id: &str, record: UploadRecord) -> Value {
    let state = match record.state {
        crowdb_access_dataset::UploadState::Open => "open",
        crowdb_access_dataset::UploadState::Complete => "complete",
        crowdb_access_dataset::UploadState::Claimed => "claimed",
        crowdb_access_dataset::UploadState::Aborted => "aborted",
    };
    value!({"id":id,"length":record.length.to_string(),"part_size":record.part_size,"md5":hex::encode(record.md5),"state":state,"expires_at":record.expires_at.to_string(),"cleanup_complete":record.cleanup_complete,
        "parts":record.parts.into_iter().map(|p|value!({"index":p.index,"length":p.length.to_string(),"md5":hex::encode(p.md5)})).collect::<Vec<_>>()})
}
