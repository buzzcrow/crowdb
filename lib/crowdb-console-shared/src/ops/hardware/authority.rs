// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional Group 0 hardware publication with confirmed outcomes.

use crowdb_kv_client::{Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::key::TextKey;

use crate::error::{Error, Result};
use crate::ops::OpContext;

pub(super) async fn ready(ctx: &OpContext) -> Result<()> {
    ctx.kv().refresh_topology().await?;
    Ok(())
}

pub(super) async fn create<T: serde::Serialize>(ctx: &OpContext, key: impl TextKey, value: &T) -> Result<()> {
    ready(ctx).await?;
    let path = key.to_path();
    let intended = serde_json::to_value(value).map_err(|error| Error::Config(error.to_string()))?;
    let payload = serde_json::to_vec(value).map_err(|error| Error::Config(error.to_string()))?;
    match ctx.kv().put_cas(0, 0, path.as_bytes(), &payload, 0).await {
        Ok(_) => Ok(()),
        Err(error @ (KvError::CasFailed { .. } | KvError::OutcomeUnknown)) => {
            match ctx
                .kv()
                .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                GetOutcome::Found { value, .. } => {
                    let actual: serde_json::Value =
                        serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string()))?;
                    if actual == intended {
                        Ok(())
                    } else {
                        Err(Error::Conflict {
                            kind: "hardware".into(),
                            id: path,
                        })
                    }
                }
                GetOutcome::NotFound => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}
