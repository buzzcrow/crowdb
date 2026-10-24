// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{OperationId, SplitRevisionId};
use sha2::{Digest, Sha256};

pub(crate) fn candidate_revision(
    operation: OperationId,
    revision: u64,
) -> Result<SplitRevisionId, crate::AuthorityError> {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-split-candidate-v1\0");
    hash.update(operation.as_bytes());
    hash.update(revision.to_be_bytes());
    Ok(SplitRevisionId::from_bytes(&hash.finalize()[..16])?)
}
