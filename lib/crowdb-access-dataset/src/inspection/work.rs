// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::InspectionError;
use crate::{DatasetStore, MetadataPage, MetadataScan, MetadataScanBudget, ReadCancellation, StoredValue};
use futures::{stream::FuturesOrdered, StreamExt};
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
    /// Reserve response capacity before polling each read, including completed responses
    /// waiting behind an earlier request. Returned rows consume the shared request budget.
    pub(crate) async fn get_many(
        &mut self,
        keys: Vec<Vec<u8>>,
        cap: usize,
    ) -> Result<Vec<Option<StoredValue>>, InspectionError> {
        let mut keys = keys.into_iter().peekable();
        let mut pending = FuturesOrdered::new();
        let mut reserved = 0;
        let mut output = Vec::new();
        loop {
            while pending.len() < 16 && keys.peek().is_some() {
                let available = self
                    .remaining
                    .saturating_sub(reserved)
                    .min(2 * 1024 * 1024 - reserved);
                if available == 0 {
                    break;
                }
                // Reclaim unused reservations before issuing a smaller final read;
                // otherwise a legal row could fail only because peers are in flight.
                if available < cap && !pending.is_empty() {
                    break;
                }
                let budget = self.budget(cap.min(available), 1)?;
                let key = keys.next().ok_or(InspectionError::Corrupt)?;
                reserved += budget.bytes;
                let store = self.store;
                let cancel = self.cancel;
                let deadline = tokio::time::Instant::from_std(self.deadline);
                pending.push_back(async move {
                    let value = tokio::time::timeout_at(deadline, store.get_bounded(&key, budget, cancel))
                        .await
                        .map_err(|_| InspectionError::Limit)?;
                    Ok::<_, InspectionError>((key.len(), budget.bytes, value?))
                });
            }
            let Some(result) = pending.next().await else {
                return if keys.peek().is_some() {
                    Err(InspectionError::Limit)
                } else {
                    Ok(output)
                };
            };
            let (key_len, capacity, value) = result?;
            if self.cancel.is_cancelled() {
                return Err(InspectionError::Cancelled);
            }
            if Instant::now() >= self.deadline {
                return Err(InspectionError::Limit);
            }
            let used = key_len
                .checked_add(value.as_ref().map_or(0, |v| v.bytes.len()))
                .filter(|used| *used <= capacity)
                .ok_or(InspectionError::Limit)?;
            reserved -= capacity;
            self.remaining = self.remaining.checked_sub(used).ok_or(InspectionError::Limit)?;
            output.push(value);
        }
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
