// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::time::Instant;

use crowdb_protocol::chunk_kv::{
    BatchMutationItem, BatchMutationRequest, BatchMutationResult, ChunkKvRpcErrorCode, ClientRequestId,
    Id128, MultiGetRequest, PartitionRouting, PointOperation, RequestRouting, RpcFailure, RpcValue,
};
use futures::{stream, StreamExt};

use crate::client::wall_now_ms;
use crate::{ChunkKvClient, ClientError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComposedItemError {
    Client(ClientError),
    Server(RpcFailure),
}

pub type MultiGetItemResult = std::result::Result<Option<RpcValue>, ComposedItemError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchItem {
    pub operation: PointOperation,
    pub request_id: Option<ClientRequestId>,
}

struct MultiGetGroup {
    endpoint: String,
    map_revision: u64,
    partition_id: Id128,
    owner_epoch: u64,
    items: Vec<(usize, Vec<u8>)>,
}

struct BatchGroup {
    endpoint: String,
    map_revision: u64,
    partition_id: Id128,
    owner_epoch: u64,
    items: Vec<(usize, BatchMutationItem)>,
}

impl ChunkKvClient {
    /// Reads duplicate and cross-partition keys with one result per input index.
    ///
    /// The operation is not a global snapshot. Successful partition groups are
    /// retained while only transient unresolved groups are refreshed and retried.
    ///
    /// # Errors
    ///
    /// Returns a whole-operation error only for invalid size, cold discovery,
    /// deadline, or response-memory bounds. Partition failures remain per item.
    #[allow(clippy::too_many_lines)]
    pub async fn multi_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<MultiGetItemResult>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > self.config.max_batch_items {
            return Err(ClientError::TooLarge);
        }
        let deadline = Instant::now() + self.config.operation_timeout;
        let timeout_ms = u64::try_from(self.config.operation_timeout.as_millis())
            .map_err(|_| ClientError::InvalidRequest("operation timeout exceeds wire range".into()))?;
        let deadline_ms = wall_now_ms()
            .checked_add(timeout_ms)
            .ok_or(ClientError::Deadline)?;
        let mut results: Vec<Option<MultiGetItemResult>> = (0..keys.len()).map(|_| None).collect();
        let mut pending: Vec<usize> = (0..keys.len()).collect();
        let mut last_errors: HashMap<usize, ComposedItemError> = HashMap::new();
        let mut refreshes = 0_u32;

        for attempt in 0..self.config.max_attempts {
            let map = match self.cache.load() {
                Some(map) => map,
                None => self.refresh_with_deadline(deadline).await?,
            };
            let mut groups: HashMap<Id128, MultiGetGroup> = HashMap::new();
            for index in pending.drain(..) {
                let entry = map
                    .route(&keys[index])
                    .ok_or_else(|| ClientError::InvalidCatalog("cached map did not route key".into()))?;
                groups
                    .entry(entry.partition_id)
                    .or_insert_with(|| MultiGetGroup {
                        endpoint: entry.owner.rpc_endpoint.clone(),
                        map_revision: map.generation(),
                        partition_id: entry.partition_id,
                        owner_epoch: entry.owner_epoch,
                        items: Vec::new(),
                    })
                    .items
                    .push((index, keys[index].clone()));
            }
            let completed = stream::iter(groups.into_values().map(|group| async move {
                let request_id = self.identities.allocate();
                let response = match request_id {
                    Ok(request_id) => {
                        let request = MultiGetRequest {
                            routing: RequestRouting {
                                request_id,
                                map_revision: group.map_revision,
                                partition_id: group.partition_id,
                                owner_epoch: group.owner_epoch,
                                min_journal_position: None,
                                deadline_ms: Some(deadline_ms),
                            },
                            keys: group.items.iter().map(|(_, key)| key.clone()).collect(),
                        };
                        let remaining = deadline
                            .checked_duration_since(Instant::now())
                            .ok_or(ClientError::Deadline);
                        match remaining {
                            Ok(remaining) => tokio::time::timeout(
                                remaining,
                                self.transport.multi_get(&group.endpoint, &request),
                            )
                            .await
                            .map_err(|_| ClientError::Deadline)
                            .and_then(|result| result),
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err(error),
                };
                (group, response)
            }))
            .buffer_unordered(self.config.max_inflight_partition_groups)
            .collect::<Vec<_>>()
            .await;

            for (group, response) in completed {
                match response {
                    Ok(response) => match response.result {
                        Ok(values) if values.len() == group.items.len() => {
                            for ((index, _), value) in group.items.into_iter().zip(values) {
                                results[index] = Some(Ok(value));
                            }
                        }
                        Ok(_) => {
                            for (index, _) in group.items {
                                results[index] =
                                    Some(Err(ComposedItemError::Client(ClientError::InvalidRequest(
                                        "multi-get response cardinality changed".into(),
                                    ))));
                            }
                        }
                        Err(error) if transient(error.code) && attempt + 1 < self.config.max_attempts => {
                            for (index, _) in group.items {
                                last_errors.insert(index, ComposedItemError::Server(error.clone()));
                                pending.push(index);
                            }
                        }
                        Err(error) => {
                            for (index, _) in group.items {
                                results[index] = Some(Err(ComposedItemError::Server(error.clone())));
                            }
                        }
                    },
                    Err(error) if attempt + 1 < self.config.max_attempts => {
                        for (index, _) in group.items {
                            last_errors.insert(index, ComposedItemError::Client(error.clone()));
                            pending.push(index);
                        }
                    }
                    Err(error) => {
                        for (index, _) in group.items {
                            results[index] = Some(Err(ComposedItemError::Client(error.clone())));
                        }
                    }
                }
            }
            if pending.is_empty() {
                break;
            }
            if refreshes < self.config.max_route_refreshes {
                refreshes += 1;
                let _ = self.refresh_with_deadline(deadline).await;
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(ClientError::Deadline)?;
            tokio::time::sleep(self.config.retry_backoff.min(remaining)).await;
        }
        for index in pending {
            results[index] = Some(Err(last_errors
                .remove(&index)
                .unwrap_or(ComposedItemError::Client(ClientError::Deadline))));
        }
        let results: Vec<MultiGetItemResult> = results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(ComposedItemError::Client(ClientError::InvalidRequest(
                        "multi-get result was not resolved".into(),
                    )))
                })
            })
            .collect();
        let response_bytes: usize = results
            .iter()
            .filter_map(|result| result.as_ref().ok().and_then(Option::as_ref))
            .map(|value| value.key.len().saturating_add(value.value.len()))
            .sum();
        if response_bytes > self.config.max_response_bytes {
            return Err(ClientError::TooLarge);
        }
        Ok(results)
    }

    /// Executes a non-transactional batch with stable per-operation identities.
    ///
    /// Relative order is preserved within each partition; no order or atomicity
    /// is claimed across partition groups.
    ///
    /// # Errors
    ///
    /// Returns before dispatch for oversized input, read operations, identity
    /// exhaustion, cold discovery, or deadline failure.
    pub async fn batch_mutate(
        &self,
        items: Vec<BatchItem>,
    ) -> Result<Vec<std::result::Result<BatchMutationResult, ComposedItemError>>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        if items.len() > self.config.max_batch_items {
            return Err(ClientError::TooLarge);
        }
        if items.iter().any(|item| !item.operation.is_mutation()) {
            return Err(ClientError::InvalidRequest(
                "batch mutation cannot contain read operations".into(),
            ));
        }
        let mut stable = Vec::with_capacity(items.len());
        for item in items {
            let request_id = match item.request_id {
                Some(request_id) => {
                    request_id
                        .validate()
                        .map_err(|error| ClientError::InvalidRequest(error.to_string()))?;
                    request_id
                }
                None => self.identities.allocate()?,
            };
            stable.push(BatchMutationItem {
                request_id,
                operation: item.operation,
            });
        }
        self.batch_mutate_stable(stable).await
    }

    #[allow(clippy::too_many_lines)]
    async fn batch_mutate_stable(
        &self,
        stable: Vec<BatchMutationItem>,
    ) -> Result<Vec<std::result::Result<BatchMutationResult, ComposedItemError>>> {
        let deadline = Instant::now() + self.config.operation_timeout;
        let timeout_ms = u64::try_from(self.config.operation_timeout.as_millis())
            .map_err(|_| ClientError::InvalidRequest("operation timeout exceeds wire range".into()))?;
        let deadline_ms = wall_now_ms()
            .checked_add(timeout_ms)
            .ok_or(ClientError::Deadline)?;
        let mut results = (0..stable.len()).map(|_| None).collect::<Vec<_>>();
        let mut pending: Vec<usize> = (0..stable.len()).collect();
        let mut last_errors: HashMap<usize, ComposedItemError> = HashMap::new();
        let mut refreshes = 0_u32;

        for attempt in 0..self.config.max_attempts {
            let map = match self.cache.load() {
                Some(map) => map,
                None => self.refresh_with_deadline(deadline).await?,
            };
            let mut groups: HashMap<Id128, BatchGroup> = HashMap::new();
            for index in pending.drain(..) {
                let item = &stable[index];
                let entry = map.route(item.operation.key()).ok_or_else(|| {
                    ClientError::InvalidCatalog("cached map did not route batch key".into())
                })?;
                groups
                    .entry(entry.partition_id)
                    .or_insert_with(|| BatchGroup {
                        endpoint: entry.owner.rpc_endpoint.clone(),
                        map_revision: map.generation(),
                        partition_id: entry.partition_id,
                        owner_epoch: entry.owner_epoch,
                        items: Vec::new(),
                    })
                    .items
                    .push((index, item.clone()));
            }
            let completed = stream::iter(groups.into_values().map(|group| async move {
                let request = BatchMutationRequest {
                    routing: PartitionRouting {
                        map_revision: group.map_revision,
                        partition_id: group.partition_id,
                        owner_epoch: group.owner_epoch,
                        deadline_ms: Some(deadline_ms),
                    },
                    operations: group.items.iter().map(|(_, item)| item.clone()).collect(),
                };
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or(ClientError::Deadline);
                let response = match remaining {
                    Ok(remaining) => tokio::time::timeout(
                        remaining,
                        self.transport.batch_mutate(&group.endpoint, &request),
                    )
                    .await
                    .map_err(|_| ClientError::Deadline)
                    .and_then(|result| result),
                    Err(error) => Err(error),
                };
                (group, response)
            }))
            .buffer_unordered(self.config.max_inflight_partition_groups)
            .collect::<Vec<_>>()
            .await;

            for (group, response) in completed {
                match response {
                    Ok(response) => match response.result {
                        Ok(values) if values.len() == group.items.len() => {
                            for ((index, expected), value) in group.items.into_iter().zip(values) {
                                if value.request_id != expected.request_id {
                                    results[index] = Some(Err(ComposedItemError::Client(
                                        ClientError::InvalidRequest("batch response identity changed".into()),
                                    )));
                                } else if value
                                    .result
                                    .as_ref()
                                    .err()
                                    .is_some_and(|error| transient(error.code))
                                    && attempt + 1 < self.config.max_attempts
                                {
                                    last_errors.insert(
                                        index,
                                        ComposedItemError::Server(value.result.clone().unwrap_err()),
                                    );
                                    pending.push(index);
                                } else {
                                    results[index] = Some(Ok(value));
                                }
                            }
                        }
                        Ok(_) => {
                            for (index, _) in group.items {
                                results[index] = Some(Err(ComposedItemError::Client(
                                    ClientError::InvalidRequest("batch response cardinality changed".into()),
                                )));
                            }
                        }
                        Err(error) if transient(error.code) && attempt + 1 < self.config.max_attempts => {
                            for (index, _) in group.items {
                                last_errors.insert(index, ComposedItemError::Server(error.clone()));
                                pending.push(index);
                            }
                        }
                        Err(error) => {
                            for (index, _) in group.items {
                                results[index] = Some(Err(ComposedItemError::Server(error.clone())));
                            }
                        }
                    },
                    Err(error) if attempt + 1 < self.config.max_attempts => {
                        for (index, _) in group.items {
                            last_errors.insert(index, ComposedItemError::Client(error.clone()));
                            pending.push(index);
                        }
                    }
                    Err(error) => {
                        for (index, _) in group.items {
                            results[index] = Some(Err(ComposedItemError::Client(error.clone())));
                        }
                    }
                }
            }
            if pending.is_empty() {
                break;
            }
            if refreshes < self.config.max_route_refreshes {
                refreshes += 1;
                let _ = self.refresh_with_deadline(deadline).await;
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(ClientError::Deadline)?;
            tokio::time::sleep(self.config.retry_backoff.min(remaining)).await;
        }
        for index in pending {
            results[index] = Some(Err(last_errors
                .remove(&index)
                .unwrap_or(ComposedItemError::Client(ClientError::Deadline))));
        }
        let results: Vec<_> = results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(ComposedItemError::Client(ClientError::InvalidRequest(
                        "batch result was not resolved".into(),
                    )))
                })
            })
            .collect();
        let response_bytes: usize = results
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .map(|result| operation_result_bytes(&result.result))
            .sum();
        if response_bytes > self.config.max_response_bytes {
            return Err(ClientError::TooLarge);
        }
        Ok(results)
    }
}

fn operation_result_bytes(
    result: &std::result::Result<crowdb_protocol::chunk_kv::OperationResult, RpcFailure>,
) -> usize {
    match result {
        Ok(crowdb_protocol::chunk_kv::OperationResult::Value(value)) => value
            .as_ref()
            .map_or(0, |value| value.key.len().saturating_add(value.value.len())),
        Ok(crowdb_protocol::chunk_kv::OperationResult::Mutation { observed, .. }) => observed
            .as_ref()
            .map_or(0, |value| value.key.len().saturating_add(value.value.len())),
        Ok(crowdb_protocol::chunk_kv::OperationResult::Scan { items, .. }) => items
            .iter()
            .map(|value| value.key.len().saturating_add(value.value.len()))
            .sum(),
        Err(error) => error.message.len(),
    }
}

fn transient(code: ChunkKvRpcErrorCode) -> bool {
    matches!(
        code,
        ChunkKvRpcErrorCode::NotMyRange
            | ChunkKvRpcErrorCode::RefreshRequired
            | ChunkKvRpcErrorCode::Overloaded
            | ChunkKvRpcErrorCode::WriteStalled
            | ChunkKvRpcErrorCode::Recovering
            | ChunkKvRpcErrorCode::LeaseExpired
    )
}
