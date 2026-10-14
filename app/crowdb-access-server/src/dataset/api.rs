// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned JSON control routes; stored authority records never cross this boundary.

mod admission;
mod conversion;
mod error;

pub(super) use admission::ApiAdmission;

use crowdb_access_dataset::{DatasetIdentity, NamespacePath, OperationId, SnapshotId};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    body::{Bytes, Incoming},
    Method, Request, Response, StatusCode,
};
use percent_encoding::percent_decode_str;
use serde::{de::DeserializeOwned, Serialize};

use super::{models, DatasetHttpService, MAX_REQUEST_BYTES};
use error::ApiError;

type ApiResult = Result<Response<Full<Bytes>>, ApiError>;

impl DatasetHttpService {
    pub(super) async fn handle_api(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let request_id = OperationId::random().to_string();
        if !self.authorized(&request) {
            return ApiError::unauthorized().response(&request_id);
        }
        let permit = match self.api_admission.acquire() {
            Ok(permit) => permit,
            Err(error) => return error.response(&request_id),
        };
        let result = tokio::time::timeout(self.request_timeout, self.route_api(request))
            .await
            .unwrap_or_else(|_| Err(ApiError::timeout()));
        let response = match result {
            Ok(mut response) => {
                response
                    .headers_mut()
                    .insert("x-request-id", request_id.parse().expect("hex header"));
                response
            }
            Err(error) => error.response(&request_id),
        };
        let (parts, body) = response.into_parts();
        let bytes = body.collect().await.expect("Full body is infallible").to_bytes();
        Response::from_parts(
            parts,
            Full::new(Bytes::from_owner(admission::ResponseOwner { bytes, permit })),
        )
    }

    async fn route_api(&self, request: Request<Incoming>) -> ApiResult {
        let parts = decode_path(request.uri().path())?;
        if parts.len() < 7
            || parts[..5] != ["api", "dataset", "v1", "namespaces", "dataset-ns"]
            || parts[5] != "datasets"
        {
            return Err(ApiError::not_found());
        }
        let identity =
            DatasetIdentity::new(NamespacePath::root(), &parts[6]).map_err(|_| ApiError::invalid())?;
        let tail: Vec<&str> = parts[7..].iter().map(String::as_str).collect();
        let method = request.method().clone();
        let authority = self.reads.authority();
        match (method, tail.as_slice()) {
            (Method::PUT, []) => {
                read_body(request, false).await?;
                authority.create_dataset(identity.clone()).await?;
                json(&conversion::dataset(&authority.get_dataset(&identity).await?))
            }
            (Method::GET, []) => json(&conversion::dataset(&authority.get_dataset(&identity).await?)),
            (Method::POST, ["snapshots", action @ ("prepare" | "publish")]) => {
                let publication: models::Publication = read_json(request).await?;
                let operation = operation(&publication.operation)?;
                let parent = publication.parent.as_deref().map(snapshot).transpose()?;
                let manifest = conversion::manifest(publication.manifest)?;
                let bytes = bincode::serialize(&manifest).map_err(|_| ApiError::integrity())?;
                if *action == "prepare" {
                    authority
                        .prepare_snapshot_with_operation(&identity, parent, bytes, operation)
                        .await?;
                } else {
                    authority
                        .publish_snapshot_with_operation(&identity, parent, bytes, operation)
                        .await?;
                }
                let record = authority.publication_status(&identity, operation).await?;
                let latest = authority.latest(&identity).await?;
                json(&conversion::snapshot(record, latest))
            }
            (Method::GET, ["operations", token]) => {
                let record = authority.publication_status(&identity, operation(token)?).await?;
                json(&conversion::snapshot(record, authority.latest(&identity).await?))
            }
            (Method::GET, ["snapshots", id]) => {
                let record = authority.get_snapshot(&identity, snapshot(id)?).await?;
                json(&conversion::snapshot(record, authority.latest(&identity).await?))
            }
            (Method::GET, ["snapshots", id, "manifest"]) => {
                let manifest = authority.get_manifest(&identity, snapshot(id)?).await?;
                json(&conversion::public_manifest(manifest)?)
            }
            (Method::PUT, ["names", "stable"]) => {
                let value: models::SnapshotRequest = read_json(request).await?;
                authority
                    .set_stable(&identity, snapshot(&value.snapshot)?)
                    .await?;
                json(&conversion::dataset(&authority.get_dataset(&identity).await?))
            }
            (Method::POST, ["snapshots", id, action @ ("retain" | "release")]) => {
                read_body(request, false).await?;
                let id = snapshot(id)?;
                if *action == "retain" {
                    authority.retain_snapshot(&identity, id).await?;
                } else {
                    authority.release_snapshot(&identity, id).await?;
                }
                json(&conversion::snapshot(
                    authority.get_snapshot(&identity, id).await?,
                    authority.latest(&identity).await?,
                ))
            }
            _ => Err(ApiError::not_found()),
        }
    }
}

fn json<T: Serialize>(value: &T) -> ApiResult {
    let mut writer = JsonWriter(Vec::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| ApiError::too_large())?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(writer.0)))
        .expect("static response"))
}

async fn read_json<T: DeserializeOwned>(request: Request<Incoming>) -> Result<T, ApiError> {
    let bytes = read_body(request, true).await?;
    serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid())
}

async fn read_body(request: Request<Incoming>, require_json: bool) -> Result<Bytes, ApiError> {
    if require_json
        && !request
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.split(';').next() == Some("application/json"))
    {
        return Err(ApiError::invalid());
    }
    if request
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|v| v > MAX_REQUEST_BYTES as u64)
    {
        return Err(ApiError::too_large());
    }
    Limited::new(request.into_body(), MAX_REQUEST_BYTES)
        .collect()
        .await
        .map(http_body_util::Collected::to_bytes)
        .map_err(|error| {
            if error.is::<http_body_util::LengthLimitError>() {
                ApiError::too_large()
            } else {
                ApiError::invalid()
            }
        })
}

fn decode_path(path: &str) -> Result<Vec<String>, ApiError> {
    path.strip_prefix('/')
        .ok_or_else(ApiError::invalid)?
        .split('/')
        .map(|part| {
            let bytes = part.as_bytes();
            let mut index = 0;
            while index < bytes.len() {
                if bytes[index] == b'%' {
                    if index + 2 >= bytes.len()
                        || !bytes[index + 1].is_ascii_hexdigit()
                        || !bytes[index + 2].is_ascii_hexdigit()
                    {
                        return Err(ApiError::invalid());
                    }
                    index += 3;
                } else {
                    index += 1;
                }
            }
            percent_decode_str(part)
                .decode_utf8()
                .map(std::borrow::Cow::into_owned)
                .map_err(|_| ApiError::invalid())
        })
        .collect()
}

fn id_bytes(value: &str) -> Result<[u8; 16], ApiError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ApiError::invalid());
    }
    let mut bytes = [0; 16];
    hex::decode_to_slice(value, &mut bytes).map_err(|_| ApiError::invalid())?;
    if bytes == [0; 16] {
        return Err(ApiError::invalid());
    }
    Ok(bytes)
}
fn operation(value: &str) -> Result<OperationId, ApiError> {
    OperationId::from_bytes(&id_bytes(value)?).map_err(|_| ApiError::invalid())
}
fn snapshot(value: &str) -> Result<SnapshotId, ApiError> {
    SnapshotId::from_bytes(&id_bytes(value)?).map_err(|_| ApiError::invalid())
}

struct JsonWriter(Vec<u8>);
impl std::io::Write for JsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_REQUEST_BYTES {
            return Err(std::io::Error::other("JSON limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
