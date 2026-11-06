// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::record::{operation_key, CandidateRecord};
use super::{catalog_key, KvRootCatalogStore};
use crowdb_paxos_kv_client::ReadMode;
use crowdb_tree_ffi::chunk::protection::CandidateStatus;
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::CtError;

impl KvRootCatalogStore {
    pub(super) async fn scan_candidates(
        &self,
        after: Option<StageId>,
        limit: u32,
    ) -> Result<Vec<CandidateStatus>, CtError> {
        if !(1..=64).contains(&limit) {
            return Err(CtError::InvalidArgument);
        }
        let mut prefix = catalog_key(self.tree_id, b"candidate", 0);
        prefix.truncate(prefix.len() - 8);
        let start = after
            .map(|id| operation_key(self.tree_id, b"candidate", id))
            .unwrap_or_default();
        let result = self
            .kv
            .scan(
                self.store_id,
                self.group_id,
                &prefix,
                &start,
                &[],
                limit,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await
            .map_err(|_| CtError::Unavailable)?;
        if result.timed_out {
            return Err(CtError::Unavailable);
        }
        result
            .items
            .into_iter()
            .map(|(key, value)| {
                if key.len() != prefix.len() + 24 {
                    return Err(CtError::Corruption);
                }
                let bytes = &key[prefix.len()..];
                let at = |offset| {
                    u64::from_be_bytes(
                        bytes[offset..offset + 8]
                            .try_into()
                            .expect("candidate key bounds"),
                    )
                };
                let id = StageId {
                    owner_epoch: at(0),
                    operation_high: at(8),
                    operation_low: at(16),
                };
                if after.is_some_and(|after| id <= after) {
                    return Err(CtError::Corruption);
                }
                Ok(CandidateRecord::decode(id, &value)?.status)
            })
            .collect()
    }
}
