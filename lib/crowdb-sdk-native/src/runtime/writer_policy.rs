// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_client::SmallWritePolicy;

use super::{ClientRuntime, Storage};
use crate::{Error, Result};

impl ClientRuntime {
    /// Reuses a compatible writer group, or creates a separate bounded pool
    /// while retaining the same discovery, transports, read and SDK credits.
    /// # Errors
    /// Rejects closed admission, invalid policy or more than 32 writer groups.
    pub fn with_writer_policy(&self, policy: &SmallWritePolicy) -> Result<Self> {
        policy
            .validate()
            .map_err(|error| Error::Configuration(error.to_string()))?;
        loop {
            self.resources.check_open()?;
            let observed = self.groups.load_full();
            if let Some(storage) = observed.iter().find(|group| &group.policy == policy) {
                return Ok(Self {
                    shutdown: Arc::clone(&self.shutdown),
                    storage: Arc::clone(storage),
                    resources: Arc::clone(&self.resources),
                    groups: Arc::clone(&self.groups),
                });
            }
            if observed.len() >= 32 {
                return Err(Error::Configuration("writer group limit reached".into()));
            }
            let storage = Arc::new(Storage {
                owns_small_pool: true,
                owns_transports: false,
                control: Arc::clone(&self.storage.control),
                metadata: Arc::clone(&self.storage.metadata),
                chunks: Arc::new(
                    self.storage
                        .chunks
                        .with_writer_policy(policy.clone())
                        .map_err(|error| Error::Configuration(error.to_string()))?,
                ),
                chunk_type: policy.chunk_type,
                policy: policy.clone(),
                metadata_transport: Arc::clone(&self.storage.metadata_transport),
                catalog: Arc::clone(&self.storage.catalog),
                services: Arc::clone(&self.storage.services),
            });
            let mut next = observed.as_ref().clone();
            next.push(Arc::clone(&storage));
            let previous = self.groups.compare_and_swap(&observed, Arc::new(next));
            if Arc::ptr_eq(&previous, &observed) {
                // A new pool has no running manager until its first write.
                // A racing shutdown cannot leave an untracked active producer.
                self.resources.check_open()?;
                return Ok(Self {
                    shutdown: Arc::clone(&self.shutdown),
                    storage,
                    resources: Arc::clone(&self.resources),
                    groups: Arc::clone(&self.groups),
                });
            }
        }
    }
}
