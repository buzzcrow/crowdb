// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{batch::BatchRequest, json, read_json, ApiError, ApiResult, DatasetHttpService};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{
    DatasetIdentity, DatasetReads, ReadCancellation, ReadPartition, ReadShuffle, ReadSpec, Selection,
};
use hyper::{body::Incoming, Method, Request};
use serde::Deserialize;

#[derive(Deserialize)]
struct Plan {
    snapshot: String,
    selection: Predicate,
    fields: Vec<String>,
    batch_size: usize,
    partition: ReadPartition,
    #[serde(deserialize_with = "required_shuffle")]
    shuffle: Option<ReadShuffle>,
}
fn required_shuffle<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<ReadShuffle>, D::Error> {
    Option::<ReadShuffle>::deserialize(d)
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Predicate {
    Explicit { ids: Vec<String> },
    Prefix { value: String },
    Keyword { value: String },
    Range { start: String, end: String },
    FieldMatch { field: String, value: String },
    And { items: Vec<Predicate> },
    Or { items: Vec<Predicate> },
    Not { item: Box<Predicate> },
}
fn bytes(value: &str) -> Result<Vec<u8>, ApiError> {
    let b = STANDARD.decode(value).map_err(|_| ApiError::invalid())?;
    if b.is_empty() || b.len() > 1024 || STANDARD.encode(&b) != *value {
        return Err(ApiError::invalid());
    }
    Ok(b)
}
impl Predicate {
    fn native(self, depth: usize, budget: &mut usize) -> Result<Selection, ApiError> {
        if depth > 16 || *budget == 0 {
            return Err(ApiError::too_large());
        }
        *budget -= 1;
        Ok(match self {
            Self::Explicit { ids } => {
                if ids.len() > 10000 {
                    return Err(ApiError::too_large());
                }
                Selection::Explicit(ids.into_iter().map(|v| bytes(&v)).collect::<Result<_, _>>()?)
            }
            Self::Prefix { value } => Selection::Prefix(bytes(&value)?),
            Self::Keyword { value } => Selection::Keyword(bytes(&value)?),
            Self::Range { start, end } => Selection::Range {
                start: bytes(&start)?,
                end: bytes(&end)?,
            },
            Self::FieldMatch { field, value } => Selection::FieldMatch {
                field,
                value: bytes(&value)?,
            },
            Self::And { items } => Selection::And(
                items
                    .into_iter()
                    .map(|p| p.native(depth + 1, budget))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Or { items } => Selection::Or(
                items
                    .into_iter()
                    .map(|p| p.native(depth + 1, budget))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Not { item } => Selection::Not(Box::new(item.native(depth + 1, budget)?)),
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NextRead {
    pub cursor: String,
}
impl DatasetHttpService {
    pub(super) async fn query_route(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        if request.method() != Method::POST {
            return Err(ApiError::not_found());
        }
        match tail {
            ["reads", "bind"] => {
                let p: Plan = read_json(request).await?;
                let spec = ReadSpec {
                    snapshot: p.snapshot,
                    selection: p.selection.native(0, &mut 1024)?,
                    fields: p.fields,
                    batch_size: p.batch_size,
                    partition: p.partition,
                    shuffle: p.shuffle,
                    split: None,
                };
                let binding = DatasetReads::new(self.reads.authority())
                    .bind(identity, spec)
                    .await?;
                json(&serde_json::json!({"snapshot":binding.snapshot.to_string(), "cursor":binding.cursor}))
            }
            ["reads", "next"] => self.query_next(identity, request).await,
            _ => Err(ApiError::not_found()),
        }
    }
    pub(super) async fn query_next(
        &self,
        identity: &DatasetIdentity,
        request: Request<Incoming>,
    ) -> ApiResult {
        let request_id = request
            .extensions()
            .get::<super::RequestId>()
            .ok_or_else(ApiError::integrity)?
            .0
            .clone();
        let input: NextRead = read_json(request).await?;
        let page = DatasetReads::new(self.reads.authority())
            .next(identity, &input.cursor, &ReadCancellation::new())
            .await?;
        let end = page.next.is_none();
        self.batch(
            identity,
            BatchRequest {
                snapshot: page.snapshot.to_string(),
                sample_ids: page.ids.iter().map(|id| STANDARD.encode(id)).collect(),
                fields: page.fields,
                sample_id_limit: Some(16 * 1024),
            },
            page.next,
            end,
            request_id,
        )
        .await
    }
}
