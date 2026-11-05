// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable pre-CAS migration receipts also retain the legacy pack directory.
use super::{catalog_key, decode_authority, KvRootCatalogStore};
use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_tree_ffi::{chunk::legacy::LegacyManifest, CtError};

impl KvRootCatalogStore {
    pub(super) fn retain_legacy_migration(
        &self,
        tree: u64,
        generation: u64,
        manifest: &[u8],
    ) -> Result<(), CtError> {
        let original = LegacyManifest::decode(manifest)?;
        if tree != self.tree_id
            || original.tree_id() != tree
            || original.generation() != generation
            || original.owner_epoch() > self.owner_epoch
        {
            return Err(CtError::InvalidArgument);
        }
        self.wait(self.authority_mutation(|| async {
            let authority_key = catalog_key(tree, b"authority", 0);
            let (authority, revision) = self
                .get(&authority_key)
                .await
                .map_err(|_| CtError::Unavailable)?
                .ok_or(CtError::Unavailable)?;
            self.check_registered_writer().await?;
            let (epoch, current_generation) = decode_authority(&authority).ok_or(CtError::Corruption)?;
            if epoch != self.owner_epoch || current_generation < generation {
                return Err(CtError::Unavailable.into());
            }
            let key = catalog_key(tree, b"legacy-migration", generation);
            if let Some((existing, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? {
                return if existing.as_ref() == manifest {
                    Ok(())
                } else {
                    Err(CtError::Corruption.into())
                };
            }
            let current = self
                .get(&catalog_key(tree, b"current", 0))
                .await
                .map_err(|_| CtError::Unavailable)?
                .ok_or(CtError::Unavailable)?
                .0;
            if current_generation != generation || current.as_ref() != manifest {
                return Err(CtError::Unavailable.into());
            }
            let operations = [
                BatchOp::Put {
                    key: Bytes::from(authority_key.clone()),
                    value: authority,
                },
                BatchOp::Put {
                    key: Bytes::from(key),
                    value: Bytes::copy_from_slice(manifest),
                },
            ];
            self.kv
                .batch_write_cas(
                    self.store_id,
                    self.group_id,
                    &operations,
                    &authority_key,
                    revision,
                )
                .await
                .map(|_| ())
                .map_err(|error| super::transaction::MutationError::from_cas(&error))
        }))
    }
    pub(super) fn load_migration_receipt(
        &self,
        tree: u64,
        generation: u64,
    ) -> Result<Option<Vec<u8>>, CtError> {
        if tree != self.tree_id || generation == 0 {
            return Err(CtError::InvalidArgument);
        }
        self.wait(self.get(&catalog_key(tree, b"legacy-migration", generation)))
            .map(|value| value.map(|(bytes, _)| bytes.to_vec()))
            .map_err(|_| CtError::Unavailable)
    }
}
