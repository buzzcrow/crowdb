// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::CompositionError;
use crate::{AuthorityError, DatasetIdentity, OperationId};
pub(super) fn scope(
    prefix: &[u8],
    identity: &DatasetIdentity,
    operation: OperationId,
) -> Result<Vec<u8>, CompositionError> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&identity.key().map_err(AuthorityError::from)?);
    key.extend_from_slice(operation.as_bytes());
    Ok(key)
}
pub(super) fn page(
    prefix: &[u8],
    identity: &DatasetIdentity,
    operation: OperationId,
    revision: u64,
    page: u64,
) -> Result<Vec<u8>, CompositionError> {
    let mut key = scope(prefix, identity, operation)?;
    key.extend_from_slice(&revision.to_be_bytes());
    key.extend_from_slice(&page.to_be_bytes());
    Ok(key)
}
