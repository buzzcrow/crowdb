use std::collections::VecDeque;

use crate::DatasetError;

/// Stable worker/rank partition used by framework adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerPartition {
    pub worker: usize,
    pub workers: usize,
    pub rank: usize,
    pub world_size: usize,
}

impl WorkerPartition {
    /// # Errors
    /// Rejects zero dimensions and worker/rank values outside their domains.
    pub fn validate(self) -> Result<(), DatasetError> {
        if self.workers == 0
            || self.world_size == 0
            || self.worker >= self.workers
            || self.rank >= self.world_size
        {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(())
    }

    /// Partitions an ordered ID stream without materializing payload fields.
    ///
    /// # Errors
    /// Returns `InvalidManifest` for an invalid partition description.
    pub fn select<T>(&self, ids: impl IntoIterator<Item = T>) -> Result<Vec<T>, DatasetError> {
        self.validate()?;
        let slot = self.rank * self.workers + self.worker;
        let width = self.world_size * self.workers;
        Ok(ids
            .into_iter()
            .enumerate()
            .filter(|(index, _)| index % width == slot)
            .map(|(_, value)| value)
            .collect())
    }
}

/// Fixed-capacity prefetch queue for Python/framework bridges.
#[derive(Debug)]
pub struct BoundedPrefetch<T> {
    queue: VecDeque<T>,
    capacity: usize,
}

impl<T> BoundedPrefetch<T> {
    /// # Errors
    /// Rejects a zero queue capacity.
    pub fn new(capacity: usize) -> Result<Self, DatasetError> {
        if capacity == 0 {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(Self {
            queue: VecDeque::with_capacity(capacity),
            capacity,
        })
    }

    /// # Errors
    /// Returns the value unchanged when the bounded queue is full.
    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.queue.len() >= self.capacity {
            return Err(value);
        }
        self.queue.push_back(value);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}
