// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{failure, stored_value, ChunkKvDatasetStore, ReadCancellation};
use crate::{DatasetError, MetadataPage, MetadataScan, MetadataScanBudget, StoreError, StoredValue};
use crowdb_chunk_kv_client::MultiScanRequest;
use crowdb_protocol::chunk_kv::ScanDirection;

impl ChunkKvDatasetStore {
    pub(super) async fn read_metadata_key(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        budget.validate()?;
        if key.is_empty() || key.len() >= 64 * 1024 {
            return Err(StoreError::Rejected);
        }
        let mut end = key.to_vec();
        end.push(0);
        let page = self
            .metadata_scan(
                Some(key.to_vec()),
                Some(end),
                MetadataScanBudget { items: 1, ..budget },
                cancel,
            )
            .await?;
        let mut entries = page.entries.into_iter();
        match entries.next() {
            Some((actual, value)) if actual == key => Ok(Some(value)),
            Some(_) => Err(StoreError::Response),
            None => Ok(None),
        }
    }

    pub(super) async fn read_metadata_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        request.validate()?;
        let start = request.after.as_ref().map_or_else(
            || request.prefix.clone(),
            |after| {
                let mut start = after.clone();
                start.push(0);
                start
            },
        );
        let end = prefix_end(&request.prefix);
        self.metadata_scan(Some(start), end, request.budget, cancel).await
    }

    async fn metadata_scan(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        if cancel.is_cancelled() {
            return Err(DatasetError::ReadCancelled.into());
        }
        let read = tokio::time::timeout(
            budget.timeout,
            self.client.scan_bounded(MultiScanRequest {
                start,
                end,
                direction: ScanDirection::Forward,
                max_items: budget.items,
                max_bytes: budget.bytes,
                continuation: None,
            }),
        );
        let cancelled = async {
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        let page = tokio::select! {
            result = read => result.map_err(|_| StoreError::Limit)?.map_err(|error| match error { crowdb_chunk_kv_client::ClientError::TooLarge => StoreError::Limit, error => StoreError::from(error) })?,
            () = cancelled => return Err(DatasetError::ReadCancelled.into()),
        };
        if cancel.is_cancelled() {
            return Err(DatasetError::ReadCancelled.into());
        }
        if let Some(error) = page.terminal_failure {
            if error.code == crowdb_protocol::chunk_kv::ChunkKvRpcErrorCode::ScanLimitExceeded {
                return Err(StoreError::Limit);
            }
            return Err(failure(&error));
        }
        let after = page.continuation.map(|cursor| cursor.last_key);
        let entries = page
            .items
            .into_iter()
            .map(|value| (value.key.clone(), stored_value(value)))
            .collect();
        Ok(MetadataPage { entries, after })
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let index = prefix.iter().rposition(|byte| *byte != u8::MAX)?;
    let mut end = prefix[..=index].to_vec();
    end[index] += 1;
    Some(end)
}
