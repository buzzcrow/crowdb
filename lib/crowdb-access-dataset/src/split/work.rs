// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key,
    state::{state_key, DraftState},
    MembershipPage, SplitLifecycleError,
};
use crate::{AuthorityError, DatasetAuthority, DatasetIdentity, OperationId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
impl DatasetAuthority {
    pub(super) async fn register_split_window(
        &self,
        identity: &DatasetIdentity,
        state: &DraftState,
        page_number: u64,
        pages: &[MembershipPage],
    ) -> Result<(), SplitLifecycleError> {
        let mut intent = EvaluationIntent {
            version: 1,
            pages: Vec::new(),
        };
        for page in pages {
            intent.pages.push(PageIntent {
                key: key::membership_page(identity, page.snapshot, page.revision, &page.name, page.page)
                    .map_err(AuthorityError::from)?,
                digest: Sha256::digest(page.encode()?).into(),
            });
        }
        self.put_split_state(
            &work_key(
                identity,
                state.record.operation,
                state.record.revision,
                page_number,
            )?,
            &super::codec::encode(&intent)?,
        )
        .await
    }
}
