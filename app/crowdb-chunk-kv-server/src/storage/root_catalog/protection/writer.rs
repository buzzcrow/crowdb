// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::pin_owner::PinOwner;
use super::record::{decode_words, encode_words};
use super::{catalog_key, KvRootCatalogStore};
use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::StreamProcessIdentity;
use crowdb_protocol::timing::{MAX_WRITE_CLOCK_SKEW_MS, MAX_WRITE_REQUEST_AGE_MS};
use crowdb_tree_ffi::CtError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_WRITER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct WriterOwner {
    pub process: StreamProcessIdentity,
    pub incarnation: u64,
}

impl WriterOwner {
    pub(super) fn encode(self) -> Vec<u8> {
        let owner = PinOwner {
            live: true,
            process: Some(self.process),
        };
        let mut words = decode_words::<8>(&owner.encode()).expect("valid process encoding");
        words[7] = self.incarnation;
        encode_words(&words)
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, CtError> {
        let mut words = decode_words::<8>(bytes)?;
        let incarnation = words[7];
        words[7] = 0;
        let owner = PinOwner::decode(&encode_words(&words))?;
        if !owner.live || incarnation == 0 {
            return Err(CtError::Corruption);
        }
        Ok(Self {
            process: owner.process.ok_or(CtError::Corruption)?,
            incarnation,
        })
    }
}

impl KvRootCatalogStore {
    pub(in super::super) fn next_writer_incarnation() -> Result<u64, CtError> {
        NEXT_WRITER
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
            .map_err(|_| CtError::ResourceExhausted)
    }

    pub(super) fn writer_owner(&self) -> Result<Option<WriterOwner>, CtError> {
        Ok(self.process_identity()?.map(|process| WriterOwner {
            process,
            incarnation: self.writer_incarnation,
        }))
    }

    fn quiescence_key(&self, epoch: u64, owner: WriterOwner) -> Vec<u8> {
        let mut key = catalog_key(self.tree_id, b"writer-quiesced", epoch);
        key.extend_from_slice(&owner.encode());
        key
    }

    async fn writer_was_closed(&self, epoch: u64, owner: WriterOwner) -> Result<bool, CtError> {
        let Some((value, _)) = self
            .get(&self.quiescence_key(epoch, owner))
            .await
            .map_err(|_| CtError::Unavailable)?
        else {
            return Ok(false);
        };
        let words = decode_words::<2>(&value)?;
        if words[0] != 1 || words[1] == 0 {
            return Err(CtError::Corruption);
        }
        Ok(true)
    }

    pub(super) async fn writer_is_quiesced(&self, epoch: u64, owner: WriterOwner) -> Result<bool, CtError> {
        match self
            .get(&self.quiescence_key(epoch, owner))
            .await
            .map_err(|_| CtError::Unavailable)?
        {
            None => Ok(false),
            Some((value, _)) => {
                let words = decode_words::<2>(&value)?;
                if words[0] != 1 {
                    return Err(CtError::Corruption);
                }
                let deadline = words[1]
                    .checked_add(MAX_WRITE_CLOCK_SKEW_MS)
                    .and_then(|value| value.checked_add(MAX_WRITE_REQUEST_AGE_MS))
                    .ok_or(CtError::Corruption)?;
                Ok(wall_time_ms()? >= deadline)
            }
        }
    }

    pub(super) async fn writer_is_excluded(&self, epoch: u64, owner: WriterOwner) -> Result<bool, CtError> {
        if self.writer_is_quiesced(epoch, owner).await? {
            return Ok(true);
        }
        self.process_is_excluded(owner.process).await
    }

    // Reading the registration after authority is essential: replacement also
    // advances that authority revision, fencing every already-prepared mutation.
    pub(in super::super) async fn check_registered_writer(&self) -> Result<(), CtError> {
        let Some(owner) = self.writer_owner()? else {
            return Ok(());
        };
        self.check_process_owner()?;
        if self.writer_quiesced.load(Ordering::Acquire) {
            return Err(CtError::Unavailable);
        }
        let key = catalog_key(self.tree_id, b"writer-owner", self.owner_epoch);
        let value = self
            .get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .ok_or(CtError::Unavailable)?
            .0;
        if WriterOwner::decode(&value)? != owner
            || self
                .get(&self.quiescence_key(self.owner_epoch, owner))
                .await
                .map_err(|_| CtError::Unavailable)?
                .is_some()
        {
            return Err(CtError::Unavailable);
        }
        Ok(())
    }

    pub(in super::super::super) async fn register_writer(&self) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            let Some(owner) = self.writer_owner()? else {
                return Ok(());
            };
            self.check_process_owner()?;
            if self.writer_quiesced.load(Ordering::Acquire) {
                return Err(CtError::Unavailable.into());
            }
            let authority = self.protection_authority(false).await.map_err(|error| {
                tracing::warn!(tree_id = self.tree_id, owner_epoch = self.owner_epoch, error = %error,
                    "writer registration could not load root authority");
                error
            })?;
            if authority.epoch != self.owner_epoch {
                if authority.epoch > self.owner_epoch {
                    tracing::warn!(tree_id = self.tree_id, authority_epoch = authority.epoch, owner_epoch = self.owner_epoch,
                        "writer registration has a stale owner epoch");
                    return Err(CtError::Unavailable.into());
                }
                self.check_prepared_staging(authority.epoch).await.map_err(|error| {
                    tracing::warn!(tree_id = self.tree_id, authority_epoch = authority.epoch, owner_epoch = self.owner_epoch,
                        prepared = self.prepared_assignment.is_some(), error = %error,
                        "writer registration does not match prepared staging");
                    error
                })?;
            }
            let key = catalog_key(self.tree_id, b"writer-owner", self.owner_epoch);
            if let Some((bytes, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? {
                let previous = WriterOwner::decode(&bytes)?;
                if previous == owner {
                    return self.check_registered_writer().await.map_err(Into::into);
                }
                // Writer replacement is fenced by this authority CAS immediately.
                // Reclaiming an old unsealed pack still waits for the separate
                // server request-age exclusion in writer_is_excluded.
                if !self.writer_was_closed(self.owner_epoch, previous).await?
                    && !self.process_is_excluded(previous.process).await?
                {
                    tracing::warn!(
                        tree_id = self.tree_id,
                        previous_incarnation = previous.incarnation,
                        "tree writer replacement waits for the previous writer to close"
                    );
                    return Err(CtError::Unavailable.into());
                }
            }
            self.protection_write(
                authority,
                vec![BatchOp::Put {
                    key: Bytes::from(key),
                    value: Bytes::from(owner.encode()),
                }],
            )
            .await?;
            self.check_process_owner().map_err(Into::into)
        })
        .await
    }

    // The caller must have closed write admission and drained accepted native
    // backend work first. Reader/export pins are deliberately untouched.
    pub(super) async fn publish_writer_quiescence(&self) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            let owner = self.writer_owner()?.ok_or(CtError::NotSupported)?;
            self.writer_quiesced.store(true, Ordering::Release);
            let authority = self.protection_authority(false).await?;
            let key = self.quiescence_key(self.owner_epoch, owner);
            if let Some((value, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? {
                let words = decode_words::<2>(&value)?;
                return if words[0] == 1 {
                    Ok(())
                } else {
                    Err(CtError::Corruption.into())
                };
            }
            self.protection_write(
                authority,
                vec![BatchOp::Put {
                    key: Bytes::from(key),
                    value: Bytes::from(encode_words(&[1, wall_time_ms()?])),
                }],
            )
            .await
        })
        .await
    }
}

pub(super) fn wall_time_ms() -> Result<u64, CtError> {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CtError::Unavailable)?;
    time.as_millis().try_into().map_err(|_| CtError::Unavailable)
}
