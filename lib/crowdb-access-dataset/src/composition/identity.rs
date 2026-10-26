// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::CompositionError;
use crate::{AuthorityError, OperationId};
use sha2::{Digest, Sha256};

pub(super) fn candidate_operation(
    operation: OperationId,
    revision: u64,
) -> Result<OperationId, CompositionError> {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-composition-candidate-v1\0");
    hash.update(operation.as_bytes());
    hash.update(revision.to_be_bytes());
    OperationId::from_bytes(&hash.finalize()[..16]).map_err(|e| AuthorityError::from(e).into())
}
