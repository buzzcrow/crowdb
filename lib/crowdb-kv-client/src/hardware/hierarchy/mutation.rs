// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared revision fence for status and hierarchy membership changes.

use crowdb_protocol::key::{RackKey, TextKey};
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::{BatchOp, CrowdbKvClient, Error, GetOutcome, ReadMode, Result};

pub(super) async fn read(kv: &CrowdbKvClient, path: &str) -> Result<Option<(bytes::Bytes, u64)>> {
    match kv
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await?
    {
        GetOutcome::Found { value, revision } => Ok(Some((value, revision))),
        GetOutcome::NotFound => Ok(None),
    }
}

pub(super) fn decode<T: serde::de::DeserializeOwned>(path: &str, value: &[u8]) -> Result<T> {
    serde_json::from_slice(value).map_err(|error| Error::SysdataDecode {
        key: path.into(),
        reason: error.to_string(),
    })
}

pub(super) fn put<T: serde::Serialize>(path: &str, value: &T) -> Result<BatchOp> {
    let bytes = serde_json::to_vec(value).map_err(|error| Error::SysdataDecode {
        key: path.into(),
        reason: error.to_string(),
    })?;
    Ok(BatchOp::Put {
        key: path.as_bytes().to_vec().into(),
        value: bytes.into(),
    })
}

pub(super) async fn update<T, F>(kv: &CrowdbKvClient, rack_id: u64, path: &str, change: F) -> Result<()>
where
    T: serde::de::DeserializeOwned + serde::Serialize + PartialEq,
    F: Fn(&mut T) -> Result<()>,
{
    let mut last_error = Error::CasBusy;
    let mut uncertain = false;
    for attempt in 0..10 {
        let rack_path = RackKey { rack_id }.to_path();
        let rack = read(kv, &rack_path)
            .await?
            .ok_or_else(|| Error::Server(format!("hardware rack does not exist: {rack_path}")))?;
        let (bytes, revision) = read(kv, path)
            .await?
            .ok_or_else(|| Error::Server(format!("hardware record does not exist: {path}")))?;
        let mut value: T = decode(path, &bytes)?;
        let original: T = decode(path, &bytes)?;
        change(&mut value)?;
        if value == original {
            return Ok(());
        }
        let mut operations = vec![put(path, &value)?];
        let (precondition, revision) = if path == rack_path {
            (path, revision)
        } else {
            operations.push(BatchOp::Put {
                key: rack_path.as_bytes().to_vec().into(),
                value: rack.0,
            });
            (rack_path.as_str(), rack.1)
        };
        match kv
            .batch_write_cas(0, 0, &operations, precondition.as_bytes(), revision)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error @ (Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown)) => {
                uncertain |= matches!(error, Error::OutcomeUnknown);
                last_error = error;
                if attempt < 9 {
                    // Give the in-flight rack mutation time to settle before
                    // re-reading its revision. Spread competing records across
                    // the next window instead of synchronizing their retries.
                    let mut hash = DefaultHasher::new();
                    (path, attempt).hash(&mut hash);
                    let window = 1 << attempt.min(6);
                    let delay = window + hash.finish() % window;
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Err(if uncertain {
        Error::OutcomeUnknown
    } else {
        last_error
    })
}
