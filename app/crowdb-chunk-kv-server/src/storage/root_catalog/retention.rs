// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Pins and reclamation mutate the same authority revision as root publication.

use super::{
    catalog_key, catalog_pin_key, catalog_pin_prefix, decode_authority, decode_u64, KvRootCatalogStore,
};
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, ReadMode};
use crowdb_tree_ffi::CtError;

impl KvRootCatalogStore {
    async fn retention_authority(&self) -> Result<(Vec<u8>, Bytes, u64), CtError> {
        let key = catalog_key(self.tree_id, b"authority", 0);
        let (value, revision) = self
            .get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .ok_or(CtError::Unavailable)?;
        decode_authority(&value).ok_or(CtError::Corruption)?;
        Ok((key, value, revision))
    }

    pub(super) fn retain_generation(
        &self,
        tree_id: u64,
        high: u64,
        low: u64,
        generation: u64,
    ) -> Result<(), CtError> {
        if tree_id != self.tree_id || (high == 0 && low == 0) || generation == 0 {
            return Err(CtError::InvalidArgument);
        }
        self.wait(self.authority_mutation(|| async {
            let (authority_key, authority, revision) = self.retention_authority().await?;
            if self.process.is_some() {
                self.check_process_owner()?;
            }
            if self
                .get(&catalog_key(tree_id, b"manifest", generation))
                .await
                .map_err(|_| CtError::Unavailable)?
                .is_none()
            {
                return Err(CtError::NotFound.into());
            }
            self.check_legacy_pin_admission(generation).await?;
            let key = catalog_pin_key(tree_id, high, low);
            if let Some((value, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? {
                return if decode_u64(&value) == Some(generation) {
                    Ok(())
                } else {
                    Err(CtError::InvalidArgument.into())
                };
            }
            let ops = [
                BatchOp::Put {
                    key: Bytes::from(authority_key.clone()),
                    value: authority,
                },
                BatchOp::Put {
                    key: Bytes::from(key),
                    value: Bytes::copy_from_slice(&generation.to_be_bytes()),
                },
            ];
            self.kv
                .batch_write_cas(self.store_id, self.group_id, &ops, &authority_key, revision)
                .await
                .map(|_| ())
                .map_err(|error| super::transaction::MutationError::from_cas(&error))
        }))
    }

    pub(super) fn release_generation(&self, tree_id: u64, high: u64, low: u64) -> Result<(), CtError> {
        if tree_id != self.tree_id || (high == 0 && low == 0) {
            return Err(CtError::InvalidArgument);
        }
        self.wait(self.authority_mutation(|| async {
            let (authority_key, authority, revision) = self.retention_authority().await?;
            let key = catalog_pin_key(tree_id, high, low);
            if self.get(&key).await.map_err(|_| CtError::Unavailable)?.is_none() {
                return Ok(());
            }
            let ops = [
                BatchOp::Put {
                    key: Bytes::from(authority_key.clone()),
                    value: authority,
                },
                BatchOp::Delete {
                    key: Bytes::from(key),
                },
            ];
            self.kv
                .batch_write_cas(self.store_id, self.group_id, &ops, &authority_key, revision)
                .await
                .map(|_| ())
                .map_err(|error| super::transaction::MutationError::from_cas(&error))
        }))
    }

    pub(super) fn reclaim_generations(&self, tree_id: u64, generation: u64) -> u64 {
        if tree_id != self.tree_id || generation <= 2 {
            return 0;
        }
        self.wait(self.reclaim_generations_inner(generation)).unwrap_or(0)
    }

    async fn reclaim_generations_inner(&self, generation: u64) -> Result<u64, CtError> {
        self.authority_mutation(|| async {
            let (authority_key, authority, revision) = self.retention_authority().await?;
            self.check_registered_writer().await?;
            let (epoch, current_generation) = decode_authority(&authority).ok_or(CtError::Corruption)?;
            if epoch != self.owner_epoch {
                return Err(CtError::Unavailable.into());
            }
            let prefix = catalog_pin_prefix(self.tree_id);
            // A saturated pin window conservatively retains history for this pass.
            let pins = self
                .kv
                .scan(
                    self.store_id,
                    self.group_id,
                    &prefix,
                    &[],
                    &[],
                    1024,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await
                .map_err(|_| CtError::Unavailable)?;
            if pins.truncated || pins.timed_out {
                return Err(CtError::ResourceExhausted.into());
            }
            let mut generation = generation.min(current_generation);
            for (_, value) in pins.items {
                generation = generation.min(decode_u64(&value).ok_or(CtError::Corruption)?);
            }
            if generation <= 2 {
                return Ok(0);
            }
            let floor_key = catalog_key(self.tree_id, b"reclaim-floor", 0);
            let floor = match self.get(&floor_key).await.map_err(|_| CtError::Unavailable)? {
                Some((value, _)) => decode_u64(&value).ok_or(CtError::Corruption)?,
                None => 1,
            };
            let limit = generation.saturating_sub(1).min(floor.saturating_add(128));
            if floor >= limit {
                return Ok(0);
            }
            let mut end = floor;
            let mut ops = Vec::with_capacity(super::protection::MAX_METADATA_MUTATIONS);
            for old in floor..limit {
                let preserved = self.legacy_history_ops(old).await?;
                let candidate = self.reclaim_candidate_ops(old).await?;
                // Reserve the floor and authority keys; never split one
                // generation's manifest, candidate and legacy-history mutation.
                if ops.len() + 1 + preserved.len() + candidate.len() + 2
                    > super::protection::MAX_METADATA_MUTATIONS
                {
                    break;
                }
                ops.push(BatchOp::Delete {
                    key: Bytes::from(catalog_key(self.tree_id, b"manifest", old)),
                });
                let copied_legacy = !preserved.is_empty();
                ops.extend(preserved);
                ops.extend(candidate);
                end = old + 1;
                // Retain at most one newly copied legacy image in this pass.
                if copied_legacy {
                    break;
                }
            }
            ops.push(BatchOp::Put {
                key: Bytes::from(floor_key),
                value: Bytes::copy_from_slice(&end.to_be_bytes()),
            });
            ops.push(BatchOp::Put {
                key: Bytes::from(authority_key.clone()),
                value: authority,
            });
            self.kv
                .batch_write_cas(self.store_id, self.group_id, &ops, &authority_key, revision)
                .await
                .map_err(|error| super::transaction::MutationError::from_cas(&error))?;
            self.published_objects.rcu(|current| {
                let mut next = (**current).clone();
                for old in floor..end {
                    next.remove(&catalog_key(self.tree_id, b"manifest", old));
                }
                next
            });
            Ok(end - floor)
        })
        .await
    }
}
