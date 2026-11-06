// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{record, Collector, LegacyRetirementPending};
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, ReadMode, StreamProcessLeaseClient};
use crowdb_protocol::chunk_stream::StreamProcessIdentity;
use crowdb_tree_ffi::{chunk::legacy::LegacyManifest, CtError};
use std::time::{SystemTime, UNIX_EPOCH};

impl Collector {
    pub(super) async fn prove_key(&self, key: &[u8], state: &mut record::Namespace) -> Result<bool, CtError> {
        let (tree, kind, generation) = record::parse_key(key)?;
        if kind == b"authority" {
            self.freeze_tree(tree, state).await?;
        } else if kind == b"manifest" || kind == b"legacy-migration" {
            if let Some((bytes, _)) = self.get(key).await? {
                if record::is_legacy(&bytes) {
                    let manifest = LegacyManifest::decode(&bytes)?;
                    if manifest.tree_id() != tree || manifest.generation() != generation {
                        return Err(CtError::Corruption);
                    }
                    self.save_receipt(tree, generation, bytes.clone()).await?;
                    if !state.owners_done && !self.prove_pack_owners(manifest, state).await? {
                        return Ok(false);
                    }
                    return self.prove_generation(tree, generation, state).await;
                }
            }
        }
        Ok(state.pending.is_none())
    }

    async fn prove_pack_owners(
        &self,
        manifest: LegacyManifest<'_>,
        state: &mut record::Namespace,
    ) -> Result<bool, CtError> {
        let own_key = record::key(manifest.tree_id(), b"legacy-inventory", 0);
        if self
            .get(&own_key)
            .await?
            .map_or(true, |(value, _)| value.as_ref() != b"1")
        {
            // Still retain the original directory when old publication provenance is absent.
            state.pending = Some(LegacyRetirementPending::MissingOwnerProof {
                tree: manifest.tree_id(),
                generation: manifest.generation(),
            });
            return Ok(false);
        }
        for pack in manifest.packs().skip(state.pack_index).take(64) {
            let key = record::key(pack.owner_tree_id, b"legacy-inventory", 0);
            if self
                .get(&key)
                .await?
                .map_or(true, |(value, _)| value.as_ref() != b"1")
            {
                state.pending = Some(LegacyRetirementPending::MissingOwnerProof {
                    tree: pack.owner_tree_id,
                    generation: manifest.generation(),
                });
                return Ok(false);
            }
            state.pack_index += 1;
        }
        state.owners_done = state.pack_index == manifest.packs().len();
        Ok(state.owners_done)
    }

    async fn freeze_tree(&self, tree: u64, state: &mut record::Namespace) -> Result<(), CtError> {
        let authority_key = record::key(tree, b"authority", 0);
        let (authority, revision) = self.get(&authority_key).await?.ok_or(CtError::Corruption)?;
        if authority.len() != 16 {
            return Err(CtError::Corruption);
        }
        if let Some((current, _)) = self.get(&record::key(tree, b"current", 0)).await? {
            if record::is_legacy(&current) {
                LegacyManifest::decode(&current)?;
                state.pending = Some(LegacyRetirementPending::LegacyRoot { tree });
                return Ok(());
            }
        } else if authority[8..] != 0_u64.to_be_bytes() {
            return Err(CtError::Corruption);
        }
        let key = record::key(tree, b"legacy-frozen", 0);
        if self.get(&key).await?.is_some() {
            return Ok(());
        }
        let ops = [
            BatchOp::Put {
                key: Bytes::from(authority_key.clone()),
                value: authority,
            },
            BatchOp::Put {
                key: Bytes::from(key),
                value: Bytes::from_static(b"1"),
            },
        ];
        self.kv
            .batch_write_cas(self.store, self.group, &ops, &authority_key, revision)
            .await
            .map(|_| ())
            .map_err(|_| CtError::Unavailable)
    }

    async fn save_receipt(&self, tree: u64, generation: u64, manifest: Bytes) -> Result<(), CtError> {
        let key = record::key(tree, b"legacy-migration", generation);
        if let Some((old, _)) = self.get(&key).await? {
            return if old == manifest {
                Ok(())
            } else {
                Err(CtError::Corruption)
            };
        }
        self.put(&key, &manifest, 0).await
    }

    async fn prove_generation(
        &self,
        tree: u64,
        generation: u64,
        state: &mut record::Namespace,
    ) -> Result<bool, CtError> {
        if self
            .get(&record::key(tree, b"legacy-tracked", generation))
            .await?
            .map_or(true, |(bytes, _)| bytes.as_ref() != b"1")
            || self
                .get(&record::key(tree, b"legacy-untracked", generation))
                .await?
                .is_some()
        {
            state.pending = Some(LegacyRetirementPending::MissingOwnerProof { tree, generation });
            return Ok(false);
        }
        let Some((current, _)) = self.get(&record::key(tree, b"current", 0)).await? else {
            return Err(CtError::Corruption);
        };
        record::typed_root(&current, tree, generation)?;
        if !state.pins_done && !self.check_pins(tree, generation, state).await? {
            return Ok(false);
        }
        let mut prefix = record::key(tree, b"legacy-reader", generation);
        prefix.push(b'/');
        let owners = self
            .kv
            .scan(
                self.store,
                self.group,
                &prefix,
                &state.proof_after,
                &[],
                64,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await
            .map_err(|_| CtError::Unavailable)?;
        if owners.timed_out {
            return Err(CtError::Unavailable);
        }
        if owners.items.is_empty() && state.proof_after.is_empty() {
            state.pending = Some(LegacyRetirementPending::MissingOwnerProof { tree, generation });
            return Ok(false);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CtError::Unavailable)?;
        let now = u64::try_from(now.as_millis()).map_err(|_| CtError::Unavailable)?;
        let leases = StreamProcessLeaseClient::new(self.kv.clone());
        for (key, bytes) in owners.items {
            let owner: StreamProcessIdentity = record::decode(&bytes)?;
            if leases
                .confirm_exclusion(owner, now)
                .await
                .map_err(|_| CtError::Unavailable)?
                .is_none()
            {
                state.pending = Some(LegacyRetirementPending::LiveProcess {
                    tree,
                    generation,
                    instance: owner.instance_id,
                    epoch: owner.process_epoch,
                });
                return Ok(false);
            }
            state.proof_after = key.to_vec();
        }
        Ok(!owners.truncated)
    }
    async fn check_pins(
        &self,
        tree: u64,
        generation: u64,
        state: &mut record::Namespace,
    ) -> Result<bool, CtError> {
        let mut prefix = record::key(tree, b"pin", 0);
        prefix.truncate(prefix.len() - 8);
        let pins = self
            .kv
            .scan(
                self.store,
                self.group,
                &prefix,
                &state.proof_after,
                &[],
                64,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await
            .map_err(|_| CtError::Unavailable)?;
        if pins.timed_out {
            return Err(CtError::Unavailable);
        }
        for (key, value) in pins.items {
            let pinned = u64::from_be_bytes(value.as_ref().try_into().map_err(|_| CtError::Corruption)?);
            if pinned == generation {
                state.pending = Some(LegacyRetirementPending::GenerationPin { tree, generation });
                return Ok(false);
            }
            state.proof_after = key.to_vec();
        }
        if pins.truncated {
            return Ok(false);
        }
        state.proof_after.clear();
        state.pins_done = true;
        Ok(true)
    }
}
