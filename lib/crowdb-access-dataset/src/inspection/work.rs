// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::InspectionError;
use crate::{DatasetStore, MetadataPage, MetadataScan, MetadataScanBudget, ReadCancellation, StoredValue};
use std::time::Instant;

/// One aggregate storage byte/deadline budget, including visibility lookups.
pub(crate) struct InspectionWork<'a> {
    store: &'a dyn DatasetStore,
    cancel: &'a ReadCancellation,
    remaining: usize,
    deadline: Instant,
}
impl<'a> InspectionWork<'a> {
    pub(crate) fn new(
        store: &'a dyn DatasetStore,
        cancel: &'a ReadCancellation,
        budget: MetadataScanBudget,
    ) -> Result<Self, InspectionError> {
        budget.validate()?;
        Ok(Self {
            store,
            cancel,
            remaining: budget.bytes,
            deadline: Instant::now() + budget.timeout,
        })
    }
    fn budget(&self, bytes: usize, items: usize) -> Result<MetadataScanBudget, InspectionError> {
        if self.cancel.is_cancelled() {
            return Err(InspectionError::Cancelled);
        }
        let timeout = self
            .deadline
            .checked_duration_since(Instant::now())
            .ok_or(InspectionError::Limit)?;
        let bytes = bytes.min(self.remaining);
        if bytes == 0 {
            return Err(InspectionError::Limit);
        }
        Ok(MetadataScanBudget {
            items,
            bytes,
            timeout,
        })
    }
    pub(crate) async fn get(
        &mut self,
        key: &[u8],
        cap: usize,
    ) -> Result<Option<StoredValue>, InspectionError> {
        let budget = self.budget(cap, 1)?;
        let value = tokio::time::timeout(budget.timeout, self.store.get_bounded(key, budget, self.cancel))
            .await
            .map_err(|_| InspectionError::Limit)??;
        let used = key.len() + value.as_ref().map_or(0, |v| v.bytes.len());
        self.remaining = self.remaining.checked_sub(used).ok_or(InspectionError::Limit)?;
        Ok(value)
    }
    pub(crate) async fn scan(
        &mut self,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        items: usize,
    ) -> Result<MetadataPage, InspectionError> {
        let budget = self.budget(self.remaining, items)?;
        let request = MetadataScan {
            prefix,
            after,
            budget,
        };
        request.validate()?;
        let page = tokio::time::timeout(budget.timeout, self.store.scan_page(&request, self.cancel))
            .await
            .map_err(|_| InspectionError::Limit)??;
        let used = page
            .entries
            .iter()
            .try_fold(0usize, |sum, (k, v)| {
                sum.checked_add(k.len())?.checked_add(v.bytes.len())
            })
            .ok_or(InspectionError::Limit)?;
        self.remaining = self.remaining.checked_sub(used).ok_or(InspectionError::Limit)?;
        Ok(page)
    }
}
