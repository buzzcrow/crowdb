// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{batch::BatchRequest, error::ApiError, json, operation, read_body, read_json, ApiResult};
use crate::dataset::{unix_seconds, DatasetHttpService};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{
    DatasetIdentity, DatasetSessions, Selection, SessionPartition, SessionShuffle, SessionSpec,
};
use hyper::{body::Incoming, Method, Request};
use serde::Deserialize;

#[derive(Deserialize)]
struct Plan {
    id: String,
    snapshot: String,
    selection: Predicate,
    fields: Vec<String>,
    batch_size: usize,
    partition: SessionPartition,
    #[serde(deserialize_with = "required_shuffle")]
    shuffle: Option<SessionShuffle>,
}
fn required_shuffle<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<SessionShuffle>, D::Error> {
    Option::<SessionShuffle>::deserialize(d)
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
struct Acknowledge {
    expected: String,
    next: String,
}
impl DatasetHttpService {
    pub(super) async fn session_route(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        let sessions = DatasetSessions::new(self.reads.authority());
        let now = unix_seconds();
        match (request.method().clone(), tail) {
            (Method::POST, ["read-plans"]) => {
                let p: Plan = read_json(request).await?;
                let spec = SessionSpec {
                    id: operation(&p.id)?,
                    snapshot: p.snapshot,
                    selection: p.selection.native(0, &mut 1024)?,
                    fields: p.fields,
                    batch_size: p.batch_size,
                    partition: p.partition,
                    shuffle: p.shuffle,
                };
                json(&sessions.start(identity, spec, now).await?)
            }
            (Method::GET, ["read-plans", id]) => json(&sessions.resume(identity, operation(id)?, now).await?),
            (Method::DELETE, ["read-plans", id]) => {
                read_body(request, false).await?;
                json(&sessions.close(identity, operation(id)?).await?)
            }
            (Method::POST, ["read-plans", id, "next"]) => {
                read_body(request, false).await?;
                let batch = sessions.next(identity, operation(id)?, now).await?;
                self.batch(
                    identity,
                    BatchRequest {
                        snapshot: batch.snapshot.to_string(),
                        sample_ids: batch.ids.iter().map(|id| STANDARD.encode(id)).collect(),
                        fields: batch.fields,
                    },
                    Some(batch.next),
                    batch.end,
                    Some(operation(id)?),
                )
                .await
            }
            (Method::PUT, ["read-plans", id, "progress"]) => {
                let ack: Acknowledge = read_json(request).await?;
                if ack.expected.len() > 4096 || ack.next.len() > 4096 {
                    return Err(ApiError::too_large());
                }
                let r = sessions
                    .acknowledge(identity, operation(id)?, &ack.expected, &ack.next, now)
                    .await?;
                json(&serde_json::json!({"plan":r.plan,"cursor":r.acknowledged}))
            }
            (Method::GET, ["read-plans", id, "progress"]) => {
                let r = sessions.resume(identity, operation(id)?, now).await?;
                json(&serde_json::json!({"plan":r.plan,"cursor":r.acknowledged}))
            }
            _ => Err(ApiError::not_found()),
        }
    }
}
