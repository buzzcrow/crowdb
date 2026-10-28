// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Validate the optional versioned value of a failed remote comparison.

use bytes::Bytes;
use crowdb_protocol::fb_wrappers::kv_client::FBKvResponseRef;
use crowdb_protocol::kv_client_fb::FBKvClientRetCode;

use crate::{Error, KvCompareObservation, Result};

pub(super) fn decode(response: &FBKvResponseRef<'_>) -> Result<Option<Box<KvCompareObservation>>> {
    let Some(observation) = response.compare_observation() else {
        return Ok(None);
    };
    let invalid = || Error::Transport {
        endpoint: String::new(),
        status: "invalid comparison observation".into(),
    };
    let key = observation.key().ok_or_else(invalid)?;
    let value = observation.value().ok_or_else(invalid)?;
    let revision = observation.revision();
    if response.ret_code() != FBKvClientRetCode::CasFailed
        || response.ok()
        || response.not_found()
        || response.revision() != revision
        || (revision == 0 && !value.is_empty())
    {
        return Err(invalid());
    }
    Ok(Some(Box::new(KvCompareObservation {
        key: Bytes::copy_from_slice(key.bytes()),
        revision,
        value: Bytes::copy_from_slice(value.bytes()),
    })))
}
