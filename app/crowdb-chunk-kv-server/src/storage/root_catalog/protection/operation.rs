// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Initial-pin release is the terminal boundary of one accepted native write.
use super::record::{decode_words, encode_words, operation_key};
use super::writer::WriterOwner;
use super::KvRootCatalogStore;
use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_protocol::timing::{MAX_WRITE_CLOCK_SKEW_MS, MAX_WRITE_REQUEST_AGE_MS};
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::CtError;

impl KvRootCatalogStore {
    // The caller atomically includes this record with releasing the initial
    // pin, after the native operation and every accepted local I/O tail ended.
    // Independent reader pins never imply that a writer is quiescent.
    pub(super) async fn operation_quiescence(
        &self,
        stage: StageId,
        owner: WriterOwner,
    ) -> Result<BatchOp, CtError> {
        if self.writer_owner()? != Some(owner) {
            return Err(CtError::Unavailable);
        }
        let key = operation_key(self.tree_id, b"stage-quiesced", stage);
        let value = if let Some((bytes, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? {
            decode_receipt(&bytes, owner)?;
            bytes
        } else {
            let mut words = [0; 10];
            words[..2].copy_from_slice(&[1, super::writer::wall_time_ms()?]);
            words[2..].copy_from_slice(&decode_words::<8>(&owner.encode())?);
            Bytes::from(encode_words(&words))
        };
        Ok(BatchOp::Put {
            key: Bytes::from(key),
            value,
        })
    }

    pub(super) async fn operation_is_excluded(
        &self,
        stage: StageId,
        owner: WriterOwner,
    ) -> Result<bool, CtError> {
        let key = operation_key(self.tree_id, b"stage-quiesced", stage);
        let Some((bytes, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? else {
            return self.writer_is_excluded(stage.owner_epoch, owner).await;
        };
        let released_at = decode_receipt(&bytes, owner)?;
        let deadline = released_at
            .checked_add(MAX_WRITE_CLOCK_SKEW_MS)
            .and_then(|value| value.checked_add(MAX_WRITE_REQUEST_AGE_MS))
            .ok_or(CtError::Corruption)?;
        if super::writer::wall_time_ms()? >= deadline {
            return Ok(true);
        }
        self.writer_is_excluded(stage.owner_epoch, owner).await
    }
}

fn decode_receipt(bytes: &[u8], owner: WriterOwner) -> Result<u64, CtError> {
    let words = decode_words::<10>(bytes)?;
    if words[0] != 1 || words[1] == 0 || WriterOwner::decode(&encode_words(&words[2..]))? != owner {
        return Err(CtError::Corruption);
    }
    Ok(words[1])
}
