// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::record::{decode_words, encode_words};
use super::{catalog_key, KvRootCatalogStore};
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, StreamProcessAuthority, StreamProcessLeaseClient};
use crowdb_protocol::chunk_stream::{StreamProcessIdentity, StreamPurpose};
use crowdb_tree_ffi::CtError;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(in super::super) struct RecoveryReceipt {
    root_generation: u64,
    stream_high: u64,
    stream_low: u64,
    applied_seq: u64,
    replay_end: u64,
}

impl KvRootCatalogStore {
    pub(in super::super::super) fn with_process(
        mut self,
        process: Option<Arc<StreamProcessAuthority>>,
    ) -> Result<Self, CtError> {
        self.writer_incarnation = Self::next_writer_incarnation()?;
        self.process = process;
        Ok(self)
    }

    pub(in super::super) fn process_identity(&self) -> Result<Option<StreamProcessIdentity>, CtError> {
        self.process
            .as_ref()
            .map(|process| {
                let identity = process.status(Instant::now()).lease.identity;
                if identity.purpose != StreamPurpose::Wal {
                    return Err(CtError::InvalidArgument);
                }
                Ok(identity)
            })
            .transpose()
    }

    pub(in super::super) fn check_process_owner(&self) -> Result<(), CtError> {
        let process = self.process.as_ref().ok_or(CtError::NotSupported)?;
        process.capture(Instant::now()).map(|_| ()).map_err(|error| {
            tracing::warn!(tree_id = self.tree_id, error = %error, "tree process authority is unavailable");
            CtError::Unavailable
        })
    }

    /// Called only after the real root and complete WAL replay have succeeded.
    /// Prepared transfer roots retain the receipt locally until publication
    /// authority has actually moved to this owner.
    pub(in super::super::super) async fn complete_recovery(
        &self,
        partition: &crowdb_chunk_kv::Partition,
        root_generation: u64,
    ) -> Result<(), CtError> {
        if self.process.is_none() {
            return Ok(());
        }
        self.check_process_owner()?;
        let snapshot = partition.snapshot();
        if partition.tree_id() != self.tree_id
            || snapshot.ownership_epoch != self.owner_epoch
            || snapshot.applied_seq != snapshot.journal_durable_seq
        {
            return Err(CtError::Unavailable);
        }
        self.recovered.store(Some(Arc::new(RecoveryReceipt {
            root_generation,
            stream_high: snapshot.stream_name.high,
            stream_low: snapshot.stream_name.low,
            applied_seq: snapshot.applied_seq,
            replay_end: snapshot.journal_durable_offset,
        })));
        let authority = self.protection_authority(false).await?;
        if authority.epoch < self.owner_epoch {
            return Ok(());
        }
        if authority.epoch != self.owner_epoch {
            return Err(CtError::Unavailable);
        }
        self.register_writer().await?;
        self.persist_completed_recovery().await
    }

    pub(in super::super) async fn persist_completed_recovery(&self) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            let Some(receipt) = self.recovered.load_full() else {
                return Ok(());
            };
            self.check_process_owner()?;
            let Some(identity) = self.process_identity()? else {
                return Err(CtError::NotSupported.into());
            };
            let authority = self.protection_authority(true).await?;
            if authority.generation < receipt.root_generation {
                return Err(CtError::Corruption.into());
            }
            let key = catalog_key(self.tree_id, b"completed-recovery", self.owner_epoch);
            let words = [
                1,
                identity.instance_id,
                identity.process_epoch,
                u64::from_le_bytes(identity.boot_nonce[..8].try_into().expect("nonce bounds")),
                u64::from_le_bytes(identity.boot_nonce[8..].try_into().expect("nonce bounds")),
                self.writer_incarnation,
                receipt.root_generation,
                receipt.stream_high,
                receipt.stream_low,
                receipt.applied_seq,
                receipt.replay_end,
            ];
            let value = encode_words(&words);
            if let Some((old, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? {
                if old.as_ref() == value {
                    return Ok(());
                }
                // Another boot must prove its own recovery, even at the same Tree epoch.
            }
            self.protection_write(
                authority,
                vec![BatchOp::Put {
                    key: Bytes::from(key),
                    value: Bytes::from(value),
                }],
            )
            .await?;
            self.check_process_owner().map_err(Into::into)
        })
        .await
    }

    pub(in super::super) async fn require_completed_recovery(&self) -> Result<(), CtError> {
        self.check_process_owner()?;
        let identity = self.process_identity()?.ok_or(CtError::NotSupported)?;
        let key = catalog_key(self.tree_id, b"completed-recovery", self.owner_epoch);
        let (value, _) = self
            .get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .ok_or(CtError::Unavailable)?;
        let words = decode_words::<11>(&value)?;
        if words[0] != 1
            || words[1] != identity.instance_id
            || words[2] != identity.process_epoch
            || words[3].to_le_bytes() != identity.boot_nonce[..8]
            || words[4].to_le_bytes() != identity.boot_nonce[8..]
            || words[5] != self.writer_incarnation
        {
            return Err(CtError::Unavailable);
        }
        Ok(())
    }

    pub(super) async fn process_is_excluded(&self, identity: StreamProcessIdentity) -> Result<bool, CtError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CtError::Unavailable)?;
        let now_ms = now.as_millis().try_into().map_err(|_| CtError::Unavailable)?;
        StreamProcessLeaseClient::new(self.kv.clone())
            .confirm_exclusion(identity, now_ms)
            .await
            .map(|proof| proof.is_some())
            .map_err(|_| CtError::Unavailable)
    }
}
