// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::RETIRED_RESOURCE;
use crate::{
    key, AuthorityError, DatasetAuthority, DatasetIdentity, MetadataScan, MetadataScanBudget,
    ReadCancellation, SnapshotId,
};
use std::time::Duration;

impl DatasetAuthority {
    pub(super) async fn require_empty_dataset(
        &self,
        identity: &DatasetIdentity,
        cancel: &ReadCancellation,
    ) -> Result<(), AuthorityError> {
        if let Some(head) = self.store.get(&key::head(identity)?).await? {
            let head = super::super::head::decode_head(&head.bytes)?;
            if head.latest.is_some() || head.stable.is_some() {
                return Err(AuthorityError::DatasetNotEmpty);
            }
        }
        for prefix in [key::SNAPSHOT_PREFIX, b"DS\0U\0", b"DS\0CD\0"] {
            let mut scope = prefix.to_vec();
            scope.extend(identity.key()?);
            let mut after = None;
            loop {
                if cancel.is_cancelled() {
                    return Err(crate::DatasetError::ReadCancelled.into());
                }
                let page = self
                    .store
                    .scan_page(
                        &MetadataScan {
                            prefix: scope.clone(),
                            after,
                            budget: MetadataScanBudget {
                                items: 32,
                                bytes: 16 * 1024 * 1024,
                                timeout: Duration::from_secs(2),
                            },
                        },
                        cancel,
                    )
                    .await?;
                for (key, value) in page.entries {
                    if value.bytes == RETIRED_RESOURCE {
                        continue;
                    }
                    if prefix == b"DS\0U\0" {
                        if key.len() == scope.len() + 16 && !crate::upload::reclaimed_session(&value.bytes)? {
                            return Err(AuthorityError::DatasetNotEmpty);
                        }
                    } else if prefix == crate::key::SNAPSHOT_PREFIX {
                        let id =
                            SnapshotId::from_bytes(key.get(scope.len()..).ok_or(AuthorityError::Corrupt)?)?;
                        let completed = match self.inspect_snapshot_lifecycle(identity, id, cancel).await {
                            Ok(state) => state.completed,
                            Err(crate::inspection::InspectionError::Authority(
                                AuthorityError::SnapshotNotFound,
                            )) => false,
                            Err(crate::inspection::InspectionError::Authority(error)) => return Err(error),
                            Err(crate::inspection::InspectionError::Store(error)) => return Err(error.into()),
                            Err(crate::inspection::InspectionError::Cancelled) => {
                                return Err(crate::DatasetError::ReadCancelled.into())
                            }
                            Err(crate::inspection::InspectionError::Limit) => {
                                return Err(crate::DatasetError::ReadLimitExceeded.into())
                            }
                            Err(_) => return Err(AuthorityError::Corrupt),
                        };
                        if !completed {
                            return Err(AuthorityError::DatasetNotEmpty);
                        }
                    } else {
                        // Legacy draft facts remain resources until their retirement contract removes them.
                        return Err(AuthorityError::DatasetNotEmpty);
                    }
                }
                after = page.after;
                if after.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }
}
