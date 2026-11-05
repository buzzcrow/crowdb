// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Generation provenance is committed with publication; late readers share its authority.
use super::{catalog_key, KvRootCatalogStore};
use crate::storage::legacy_retirement::record::{is_legacy, NAMESPACE_KEY};
use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_tree_ffi::{chunk::legacy::LegacyManifest, CtError};

impl KvRootCatalogStore {
    fn legacy_owner_op(&self, generation: u64) -> Result<BatchOp, CtError> {
        let Some(owner) = self.process_identity()? else {
            return Ok(BatchOp::Put {
                key: Bytes::from(catalog_key(self.tree_id, b"legacy-untracked", generation)),
                value: Bytes::from_static(b"1"),
            });
        };
        self.check_process_owner()?;
        let mut key = catalog_key(self.tree_id, b"legacy-reader", generation);
        key.push(b'/');
        key.extend_from_slice(&owner.instance_id.to_be_bytes());
        key.extend_from_slice(&owner.process_epoch.to_be_bytes());
        key.extend_from_slice(&owner.boot_nonce);
        Ok(BatchOp::Put {
            key: Bytes::from(key),
            value: Bytes::from(serde_json::to_vec(&owner).map_err(|_| CtError::Internal)?),
        })
    }

    pub(super) async fn legacy_publication_ops(
        &self,
        generation: u64,
        bytes: &[u8],
    ) -> Result<Vec<BatchOp>, CtError> {
        if !is_legacy(bytes) {
            return Ok(Vec::new());
        }
        let manifest = LegacyManifest::decode(bytes)?;
        if manifest.tree_id() != self.tree_id || manifest.generation() != generation {
            return Err(CtError::InvalidArgument);
        }
        if self
            .get(NAMESPACE_KEY)
            .await
            .map_err(|_| CtError::Unavailable)?
            .is_some()
        {
            return Err(CtError::NotSupported);
        }
        let mut ops = vec![
            BatchOp::Put {
                key: Bytes::from(catalog_key(self.tree_id, b"legacy-tracked", generation)),
                value: Bytes::from_static(b"1"),
            },
            self.legacy_owner_op(generation)?,
        ];
        // Only the first publication can prove this lineage has no untracked history.
        if generation == 1 && self.process_identity()?.is_some() {
            ops.push(BatchOp::Put {
                key: Bytes::from(catalog_key(self.tree_id, b"legacy-inventory", 0)),
                value: Bytes::from_static(b"1"),
            });
        }
        Ok(ops)
    }

    pub(super) async fn track_legacy_reader(&self, bytes: &[u8]) -> Result<(), CtError> {
        if !is_legacy(bytes) {
            return Ok(());
        }
        let generation = LegacyManifest::decode(bytes)?.generation();
        self.authority_mutation(|| async {
            let authority = self.protection_authority(false).await?;
            if self
                .get(&catalog_key(self.tree_id, b"legacy-frozen", 0))
                .await
                .map_err(|_| CtError::Unavailable)?
                .is_some()
            {
                return Err(CtError::Unavailable.into());
            }
            let op = self.legacy_owner_op(generation)?;
            self.protection_write(authority, vec![op]).await
        })
        .await
    }

    pub(super) async fn check_legacy_pin_admission(&self, generation: u64) -> Result<(), CtError> {
        let Some((manifest, _)) = self
            .get(&catalog_key(self.tree_id, b"manifest", generation))
            .await
            .map_err(|_| CtError::Unavailable)?
        else {
            return Ok(());
        };
        if is_legacy(&manifest)
            && self
                .get(&catalog_key(self.tree_id, b"legacy-frozen", 0))
                .await
                .map_err(|_| CtError::Unavailable)?
                .is_some()
        {
            return Err(CtError::Unavailable);
        }
        Ok(())
    }

    pub(super) async fn legacy_history_ops(&self, generation: u64) -> Result<Vec<BatchOp>, CtError> {
        let Some((manifest, _)) = self
            .get(&catalog_key(self.tree_id, b"manifest", generation))
            .await
            .map_err(|_| CtError::Unavailable)?
        else {
            return Ok(Vec::new());
        };
        if !is_legacy(&manifest) {
            return Ok(Vec::new());
        }
        LegacyManifest::decode(&manifest)?;
        let key = catalog_key(self.tree_id, b"legacy-migration", generation);
        if let Some((old, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? {
            return if old == manifest {
                Ok(Vec::new())
            } else {
                Err(CtError::Corruption)
            };
        }
        Ok(vec![BatchOp::Put {
            key: Bytes::from(key),
            value: manifest,
        }])
    }
}
