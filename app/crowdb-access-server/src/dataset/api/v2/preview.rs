// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{json, read_body, read_json, ApiError, ApiResult, DatasetHttpService};
use super::{
    inspection_models::Sample,
    query::{bytes, Query},
    split_models::decimal,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{DatasetIdentity, FieldLocator, ReadCancellation, SnapshotId};
use hyper::{body::Incoming, Method, Request};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MatrixRequest {
    sample_ids: Vec<String>,
    fields: Vec<String>,
}
impl DatasetHttpService {
    pub(super) async fn v2_exact_matrix(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        request: Request<Incoming>,
    ) -> ApiResult {
        let query = Query::parse(request.uri().query(), &[])?;
        let input: MatrixRequest = read_json(request).await?;
        if input.sample_ids.len() > 20 {
            return Err(ApiError::too_large());
        }
        let ids = input
            .sample_ids
            .into_iter()
            .map(|id| bytes(&id, 16 * 1024, false))
            .collect::<Result<Vec<_>, _>>()?;
        let entries = self
            .reads
            .authority()
            .inspection_exact_matrix(
                identity,
                snapshot,
                &ids,
                &input.fields,
                query.budget()?,
                &ReadCancellation::new(),
            )
            .await?;
        json(&json!({"entries":entries.into_iter().map(Sample::from).collect::<Vec<_>>()}))
    }
    pub(super) async fn v2_preview(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        request: Request<Incoming>,
    ) -> ApiResult {
        if request.method() != Method::GET {
            return Err(ApiError::not_found());
        }
        let query = Query::parse(request.uri().query(), &["sample", "field", "max_bytes"])?;
        let id = bytes(
            query.value("sample").ok_or_else(ApiError::invalid)?,
            16 * 1024,
            false,
        )?;
        let field = query.value("field").ok_or_else(ApiError::invalid)?.to_owned();
        let limit = decimal(query.value("max_bytes").unwrap_or("65536"))?;
        if !(1..=65536).contains(&limit) {
            return Err(ApiError::invalid());
        }
        read_body(request, false).await?;
        let authority = self.reads.authority();
        authority.check_snapshot_readable(identity, snapshot).await?;
        let mut rows = authority
            .resolve_inspection_batch_locators(
                identity,
                snapshot,
                &[id],
                std::slice::from_ref(&field),
                &ReadCancellation::new(),
            )
            .await?;
        let value = rows.pop().ok_or_else(ApiError::integrity)?.fields.remove(&field);
        let limit = usize::try_from(limit).map_err(|_| ApiError::invalid())?;
        let preview =
            tokio::time::timeout(Duration::from_secs(5), self.preview_value(identity, value, limit))
                .await
                .map_err(|_| ApiError::timeout())??;
        json(&preview)
    }
    async fn preview_value(
        &self,
        identity: &DatasetIdentity,
        value: Option<FieldLocator>,
        limit: usize,
    ) -> Result<Value, ApiError> {
        let (length, md5, value) = match value {
            None => {
                return Ok(
                    json!({"state":"missing", "length":null, "md5":null, "data":null, "truncated":false}),
                )
            }
            Some(FieldLocator::Tombstone) => {
                return Ok(
                    json!({"state":"tombstone", "length":"0", "md5":null, "data":null, "truncated":false}),
                )
            }
            Some(FieldLocator::Inline { value, md5 }) => {
                let length = value.len() as u64;
                (length, md5, value[..value.len().min(limit)].to_vec())
            }
            Some(FieldLocator::Chunk {
                location,
                length,
                md5,
            }) => {
                crowdb_access_dataset::DatasetUploads::upload_id(identity, &location)
                    .map_err(|_| ApiError::unsupported())?;
                let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
                let metadata = uploads.open_read(&location).await?;
                if metadata.length() != length || metadata.md5() != md5 {
                    return Err(ApiError::integrity());
                }
                let mut value = Vec::new();
                let mut part = 0;
                while value.len() < limit && (value.len() as u64) < length {
                    let (bytes, _) = metadata.read_part_verified(part).await?;
                    let count = bytes.len().min(limit - value.len());
                    value.extend_from_slice(&bytes[..count]);
                    part += 1;
                }
                (length, md5, value)
            }
        };
        Ok(
            json!({"state":"value", "length":length.to_string(), "md5":hex::encode(md5), "data":STANDARD.encode(&value), "truncated":(value.len() as u64) < length}),
        )
    }
}
