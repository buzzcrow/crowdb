// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{state::state_key, SplitLifecycleError};
use crate::{DatasetIdentity, OperationId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EvaluationIntent {
    pub version: u16,
    pub pages: Vec<PageIntent>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PageIntent {
    pub key: Vec<u8>,
    pub digest: [u8; 32],
}
pub(super) fn work_key(
    identity: &DatasetIdentity,
    operation: OperationId,
    revision: u64,
    page: u64,
) -> Result<Vec<u8>, SplitLifecycleError> {
    let mut key = state_key(b"DS\0SW\0", identity, operation, None)?;
    key.extend_from_slice(&revision.to_be_bytes());
    key.extend_from_slice(&page.to_be_bytes());
    Ok(key)
}
