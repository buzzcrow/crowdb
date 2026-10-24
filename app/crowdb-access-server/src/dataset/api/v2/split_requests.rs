// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::ApiError,
    split_models::{required_option, DraftSplit, Policy},
};
use crowdb_access_dataset::CommitGeneration;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Begin {
    pub operation: String,
    #[serde(deserialize_with = "required_option")]
    pub expected_revision: Option<String>,
    pub policy: Policy,
    pub splits: Vec<DraftSplit>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SplitEdit {
    pub edit: String,
    pub expected_revision: String,
    pub policy: Policy,
    pub splits: Vec<DraftSplit>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Evaluate {
    pub expected_revision: String,
    pub expected_page: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Prepare {
    pub expected_revision: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Publish {
    pub attempt: String,
    pub revision: String,
    #[serde(deserialize_with = "required_option")]
    pub expected_head: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateHead {
    pub revision: String,
    #[serde(deserialize_with = "required_option")]
    pub expected_revision: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub expected_head: Option<String>,
}
pub(super) fn generation(value: Option<&str>) -> Result<Option<CommitGeneration>, ApiError> {
    value
        .map(|value| {
            if value.len() != 64
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(ApiError::invalid());
            }
            let mut bytes = [0; 32];
            hex::decode_to_slice(value, &mut bytes).map_err(|_| ApiError::invalid())?;
            Ok(CommitGeneration::from_bytes(bytes))
        })
        .transpose()
}
