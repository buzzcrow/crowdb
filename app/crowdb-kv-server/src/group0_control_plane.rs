// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Leader-tenure-bound access to the local group-0 replica.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crowdb_kv::cluster::group_operations::{
    KvGroupMutation, KvGroupOperationError, KvGroupOperations, KvGroupRead, KvGroupScanItem,
    KvGroupScanRequest, KvReadConsistency, KvRequestIdentity,
};
use crowdb_kv::cluster::PxKvStore;

static CONTROL_PLANE_INSTANCE_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct Group0ControlPlane {
    operations: KvGroupOperations,
    client_id: u64,
    next_sequence: Arc<AtomicU64>,
}

impl Group0ControlPlane {
    /// Acquire a linearizable barrier on local `(store=0, group=0)` and bind
    /// this facade to that exact leader term.
    ///
    /// # Errors
    ///
    /// Returns an error when store/group zero is absent or the local replica
    /// cannot establish leadership with a quorum.
    pub async fn acquire(store: &Arc<PxKvStore>) -> Result<Self, KvGroupOperationError> {
        if store.store_id != 0 {
            return Err(KvGroupOperationError::Internal(format!(
                "group-0 control plane requires store 0, got {}",
                store.store_id
            )));
        }
        let operations = store
            .group_operations(0)
            .ok_or_else(|| KvGroupOperationError::Unavailable("group 0 is not hosted locally".into()))?
            .bind_current_leader_tenure()
            .await?;
        Ok(Self {
            operations,
            client_id: new_client_id(),
            next_sequence: Arc::new(AtomicU64::new(1)),
        })
    }

    #[must_use]
    pub fn leader_term(&self) -> Option<u64> {
        self.operations.leader_term()
    }

    /// Read a group-0 key through a linearizable barrier.
    ///
    /// # Errors
    ///
    /// Returns an error when the bound tenure ends or the read is unavailable.
    pub async fn get(&self, key: &[u8]) -> Result<KvGroupRead, KvGroupOperationError> {
        self.operations.get(key, KvReadConsistency::Linearizable).await
    }

    /// Read one fixed-cutoff prefix page. Callers retain `scan_cutoff` and use
    /// the last returned key as `start_after` for the next page.
    ///
    /// # Errors
    ///
    /// Returns an error when the bound tenure ends, the cutoff is invalid, or
    /// the engine cannot complete the scan.
    pub async fn scan_prefix(
        &self,
        prefix: Bytes,
        start_after: Bytes,
        limit: usize,
        scan_cutoff: u64,
    ) -> Result<(Vec<KvGroupScanItem>, bool, u64), KvGroupOperationError> {
        let scan = self
            .operations
            .scan(&KvGroupScanRequest {
                prefix,
                start_after,
                end_key: Bytes::new(),
                limit,
                consistency: KvReadConsistency::Linearizable,
                keys_only: false,
                count_only: false,
                deadline_ms: 0,
                bounded: true,
                requested_scan_cutoff: scan_cutoff,
            })
            .await?;
        Ok((scan.items, scan.truncated, scan.scan_cutoff))
    }

    /// Read an entire prefix through fixed-cutoff pagination.
    ///
    /// # Errors
    ///
    /// Returns an error if any page cannot be read in the bound leader tenure.
    pub async fn scan_all_prefix(
        &self,
        prefix: Bytes,
        page_limit: usize,
    ) -> Result<Vec<KvGroupScanItem>, KvGroupOperationError> {
        let mut items = Vec::new();
        let mut start_after = Bytes::new();
        let mut scan_cutoff = 0;
        loop {
            let (page, truncated, cutoff) = self
                .scan_prefix(prefix.clone(), start_after.clone(), page_limit, scan_cutoff)
                .await?;
            scan_cutoff = cutoff;
            if let Some(last) = page.last() {
                start_after = last.key.clone();
            }
            let empty = page.is_empty();
            items.extend(page);
            if !truncated || empty {
                break;
            }
        }
        Ok(items)
    }

    /// Put a value only when the key has `expected_revision`.
    ///
    /// # Errors
    ///
    /// Returns a typed compare, leadership, admission, or Paxos error.
    pub async fn compare_and_put(
        &self,
        key: Bytes,
        value: Bytes,
        expected_revision: u64,
    ) -> Result<u64, KvGroupOperationError> {
        let identity = self.next_identity()?;
        self.operations
            .compare_and_write(
                &[KvGroupMutation::Put {
                    key: key.clone(),
                    value,
                }],
                key,
                expected_revision,
                identity,
            )
            .await
            .map(|write| write.chosen_slot)
    }

    /// Delete a value only when the key has `expected_revision`.
    ///
    /// # Errors
    ///
    /// Returns a typed compare, leadership, admission, or Paxos error.
    pub async fn compare_and_delete(
        &self,
        key: Bytes,
        expected_revision: u64,
    ) -> Result<u64, KvGroupOperationError> {
        let identity = self.next_identity()?;
        self.operations
            .compare_and_write(
                &[KvGroupMutation::Delete { key: key.clone() }],
                key,
                expected_revision,
                identity,
            )
            .await
            .map(|write| write.chosen_slot)
    }

    fn next_identity(&self) -> Result<KvRequestIdentity, KvGroupOperationError> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        if sequence == 0 || sequence == u64::MAX {
            return Err(KvGroupOperationError::Internal(
                "group-0 control-plane request sequence exhausted".into(),
            ));
        }
        Ok(KvRequestIdentity {
            client_id: self.client_id,
            sequence,
        })
    }
}

fn new_client_id() -> u64 {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        });
    let nonce = CONTROL_PLANE_INSTANCE_NONCE.fetch_add(1, Ordering::Relaxed);
    time.rotate_left(17).wrapping_add(nonce).max(1)
}
