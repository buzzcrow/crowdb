// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{StoreError, StoredValue};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct MetadataScanBudget {
    pub items: usize,
    pub bytes: usize,
    pub timeout: Duration,
}

impl MetadataScanBudget {
    /// # Errors
    /// Rejects zero or excessive work and allocation budgets.
    pub fn validate(self) -> Result<(), StoreError> {
        if self.items == 0
            || self.items > 1024
            || self.bytes == 0
            || self.bytes > 16 * 1024 * 1024
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(30)
        {
            return Err(StoreError::Rejected);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MetadataScan {
    pub prefix: Vec<u8>,
    pub after: Option<Vec<u8>>,
    pub budget: MetadataScanBudget,
}

impl MetadataScan {
    /// # Errors
    /// Rejects invalid budgets, empty/oversized prefixes and foreign cursors.
    pub fn validate(&self) -> Result<(), StoreError> {
        self.budget.validate()?;
        if self.prefix.is_empty()
            || self.prefix.len() >= 64 * 1024
            || self
                .after
                .as_ref()
                .is_some_and(|key| !key.starts_with(&self.prefix) || key.len() >= 64 * 1024)
        {
            return Err(StoreError::Rejected);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataPage {
    pub entries: Vec<(Vec<u8>, StoredValue)>,
    pub after: Option<Vec<u8>>,
}
