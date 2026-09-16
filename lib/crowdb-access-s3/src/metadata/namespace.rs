// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use super::{BucketId, BucketNameRecord, MetadataRecordError, ObjectRecord, TenantId};

/// In-memory reference model for bucket-name lifecycle transitions.
#[derive(Default)]
pub struct BucketNamespace {
    names: BTreeMap<(TenantId, Vec<u8>), BucketNameRecord>,
    visible_objects: BTreeSet<(BucketId, Vec<u8>)>,
    objects: BTreeMap<(BucketId, Vec<u8>), ObjectRecord>,
    next_bucket_id: u128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketDeleteOutcome {
    Deleted,
    NotEmpty,
    Missing,
}

#[derive(Debug, thiserror::Error)]
pub enum BucketNamespaceError {
    #[error("bucket name cannot be empty")]
    EmptyName,
    #[error("bucket does not exist")]
    Missing,
}

impl BucketNamespace {
    /// Creates or returns the active bucket ID for a name.
    ///
    /// # Errors
    ///
    /// Returns an error when the bucket name is empty.
    pub fn create(&mut self, tenant: TenantId, name: Vec<u8>) -> Result<BucketId, BucketNamespaceError> {
        if name.is_empty() {
            return Err(BucketNamespaceError::EmptyName);
        }
        let key = (tenant.clone(), name.clone());
        if let Some(record) = self.names.get(&key).filter(|record| !record.tombstone) {
            return Ok(record.bucket_id);
        }
        self.next_bucket_id = self.next_bucket_id.saturating_add(1);
        let bucket_id = BucketId::new(self.next_bucket_id.to_be_bytes());
        self.names.insert(
            key,
            BucketNameRecord {
                tenant,
                name,
                bucket_id,
                tombstone: false,
            },
        );
        Ok(bucket_id)
    }

    #[must_use]
    pub fn head(&self, tenant: &TenantId, name: &[u8]) -> Option<BucketId> {
        self.names
            .get(&(tenant.clone(), name.to_vec()))
            .filter(|record| !record.tombstone)
            .map(|record| record.bucket_id)
    }

    /// Records a final object publication for an already-resolved bucket ID.
    pub fn publish(&mut self, bucket_id: BucketId, key: Vec<u8>) {
        self.visible_objects.insert((bucket_id, key));
    }

    /// Persists an immutable generation then directly publishes it for reads.
    ///
    /// The reference model intentionally takes no bucket delete permit or
    /// bucket-wide lease: a publication under an old ID becomes unreachable
    /// when delete tombstones its name mapping.
    ///
    /// # Errors
    ///
    /// Returns an error when the generation metadata is invalid.
    pub fn publish_object(&mut self, generation: &ObjectRecord) -> Result<(), MetadataRecordError> {
        generation.encode()?;
        let identity = (generation.bucket_id, generation.key.clone());
        self.objects.insert(identity, generation.clone());
        self.visible_objects
            .insert((generation.bucket_id, generation.key.clone()));
        Ok(())
    }

    /// Returns the generation selected for this reader without mutating older objects.
    #[must_use]
    pub fn read_object(&self, bucket_id: BucketId, key: &[u8]) -> Option<&ObjectRecord> {
        self.objects.get(&(bucket_id, key.to_vec()))
    }

    /// Tombstones an empty active mapping without waiting for old-ID publishers.
    pub fn delete(&mut self, tenant: &TenantId, name: &[u8]) -> BucketDeleteOutcome {
        let mapping_key = (tenant.clone(), name.to_vec());
        let bucket_id = match self.names.get(&mapping_key) {
            Some(mapping) if !mapping.tombstone => mapping.bucket_id,
            _ => return BucketDeleteOutcome::Missing,
        };
        if self
            .visible_objects
            .iter()
            .any(|(candidate, _)| *candidate == bucket_id)
        {
            return BucketDeleteOutcome::NotEmpty;
        }
        if let Some(mapping) = self.names.get_mut(&mapping_key) {
            mapping.tombstone = true;
        }
        BucketDeleteOutcome::Deleted
    }
}
