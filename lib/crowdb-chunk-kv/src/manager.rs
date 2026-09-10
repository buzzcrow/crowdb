// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::{ChunkKvError, Partition, PartitionId, Result};

/// Lifecycle-only registry for independent partition handles in one process.
pub struct PartitionManager {
    max_partitions: usize,
    partitions: RwLock<HashMap<PartitionId, Partition>>,
}

impl PartitionManager {
    /// Creates a manager with an explicit hosted-partition bound.
    ///
    /// # Errors
    ///
    /// Returns an error when `max_partitions` is zero.
    pub fn new(max_partitions: usize) -> Result<Self> {
        if max_partitions == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "manager partition bound must be nonzero".into(),
            ));
        }
        Ok(Self {
            max_partitions,
            partitions: RwLock::new(HashMap::new()),
        })
    }

    /// Adds one prepared or serving partition.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate identity or exhausted manager capacity.
    pub async fn insert(&self, partition: Partition) -> Result<()> {
        let id = partition.snapshot().partition_id;
        let mut partitions = self.partitions.write().await;
        if partitions.contains_key(&id) {
            return Err(ChunkKvError::InvalidRequest("partition is already hosted".into()));
        }
        if partitions.len() >= self.max_partitions {
            return Err(ChunkKvError::Overloaded);
        }
        partitions.insert(id, partition);
        Ok(())
    }

    pub async fn get(&self, partition_id: PartitionId) -> Option<Partition> {
        self.partitions.read().await.get(&partition_id).cloned()
    }

    pub async fn remove(&self, partition_id: PartitionId) -> Option<Partition> {
        self.partitions.write().await.remove(&partition_id)
    }

    pub async fn len(&self) -> usize {
        self.partitions.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.partitions.read().await.is_empty()
    }
}
