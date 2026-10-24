// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, state::CompositionState, CompositionError};
use crate::{DatasetIdentity, SampleRecord};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompositionPreview {
    pub input_revision: u64,
    pub next_page: u64,
    pub members: u64,
    pub complete: bool,
    pub binding: [u8; 32],
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Phase {
    Source { after: Option<Vec<u8>> },
    Edits { page: usize },
    Validate { after: Option<Vec<u8>> },
    Done,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Evaluation {
    pub preview: CompositionPreview,
    pub phase: Phase,
}
#[derive(Serialize, Deserialize)]
pub(super) struct RowChange {
    pub id: Vec<u8>,
    pub expected: Option<[u8; 32]>,
    pub sample: Option<SampleRecord>,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Step {
    pub next: Evaluation,
    pub rows: Vec<RowChange>,
}
pub(super) fn row_prefix(
    identity: &DatasetIdentity,
    state: &CompositionState,
) -> Result<Vec<u8>, CompositionError> {
    let mut prefix = key::scope(b"DS\0CM\0", identity, state.draft.record.operation)?;
    prefix.extend_from_slice(&state.draft.record.revision.to_be_bytes());
    Ok(prefix)
}
pub(super) fn row_key(
    identity: &DatasetIdentity,
    state: &CompositionState,
    id: &[u8],
) -> Result<Vec<u8>, CompositionError> {
    let mut key = row_prefix(identity, state)?;
    for byte in id {
        if *byte == 0 {
            key.extend_from_slice(&[0, 255]);
        } else {
            key.push(*byte);
        }
    }
    key.extend_from_slice(&[0, 0]);
    Ok(key)
}
