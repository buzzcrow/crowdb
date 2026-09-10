// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Instant;

use crowdb_protocol::chunk_kv::{
    ChunkKvResponse, ChunkKvRpcErrorCode, OperationResult, RequestRouting, RpcFailure, RpcValue,
    ScanContinuation, ScanDirection, ScanRequest, SeekKind, SeekRequest,
};

use crate::client::wall_now_ms;
use crate::{ChunkKvClient, ClientError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiScanContinuation {
    pub direction: ScanDirection,
    pub original_start: Option<Vec<u8>>,
    pub original_end: Option<Vec<u8>>,
    pub last_key: Vec<u8>,
    pub catalog_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiScanRequest {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub direction: ScanDirection,
    pub max_items: usize,
    pub max_bytes: usize,
    pub continuation: Option<MultiScanContinuation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiScanPage {
    pub items: Vec<RpcValue>,
    pub continuation: Option<MultiScanContinuation>,
    pub terminal_failure: Option<RpcFailure>,
}

impl ChunkKvClient {
    /// Executes one ordered seek directly against its routed partition.
    ///
    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn seek(&self, key: Vec<u8>, kind: SeekKind) -> Result<ChunkKvResponse> {
        let deadline = Instant::now() + self.config.operation_timeout;
        let timeout_ms = u64::try_from(self.config.operation_timeout.as_millis())
            .map_err(|_| ClientError::InvalidRequest("operation timeout exceeds wire range".into()))?;
        let deadline_ms = wall_now_ms()
            .checked_add(timeout_ms)
            .ok_or(ClientError::Deadline)?;
        let request_id = self.identities.allocate()?;
        let mut last_response = None;
        let mut last_error = None;
        let mut refreshes = 0_u32;
        for attempt in 0..self.config.max_attempts {
            let map = match self.cache.load() {
                Some(map) => map,
                None => self.refresh_with_deadline(deadline).await?,
            };
            let entry = map
                .route(&key)
                .ok_or_else(|| ClientError::InvalidCatalog("cached map did not route seek key".into()))?;
            let request = SeekRequest {
                routing: RequestRouting {
                    request_id,
                    map_revision: map.generation(),
                    partition_id: entry.partition_id,
                    owner_epoch: entry.owner_epoch,
                    min_journal_position: None,
                    deadline_ms: Some(deadline_ms),
                },
                key: key.clone(),
                kind,
            };
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(ClientError::Deadline)?;
            let refresh_route = match tokio::time::timeout(
                remaining,
                self.transport.seek(&entry.owner.rpc_endpoint, &request),
            )
            .await
            {
                Err(_) => return Err(ClientError::Deadline),
                Ok(Err(error)) => {
                    last_error = Some(error);
                    true
                }
                Ok(Ok(response)) if !response_is_transient(&response) => return Ok(response),
                Ok(Ok(response)) => {
                    let refresh = response_has_topology_failure(&response);
                    last_response = Some(response);
                    refresh
                }
            };
            if refresh_route && refreshes < self.config.max_route_refreshes {
                refreshes += 1;
                let _ = self.refresh_with_deadline(deadline).await;
            }
            if attempt + 1 < self.config.max_attempts {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or(ClientError::Deadline)?;
                tokio::time::sleep(self.config.retry_backoff.min(remaining)).await;
            }
        }
        last_response.map_or_else(
            || Err(last_error.unwrap_or_else(|| ClientError::Transport("seek retry budget exhausted".into()))),
            Ok,
        )
    }

    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn ceiling(&self, key: Vec<u8>) -> Result<ChunkKvResponse> {
        self.seek(key, SeekKind::Ceiling).await
    }

    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn higher(&self, key: Vec<u8>) -> Result<ChunkKvResponse> {
        self.seek(key, SeekKind::Higher).await
    }

    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn floor(&self, key: Vec<u8>) -> Result<ChunkKvResponse> {
        self.seek(key, SeekKind::Floor).await
    }

    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn lower(&self, key: Vec<u8>) -> Result<ChunkKvResponse> {
        self.seek(key, SeekKind::Lower).await
    }

    /// Composes a globally ordered bounded scan from partition-local RPCs.
    ///
    /// A topology failure refreshes and replans only beyond the last emitted
    /// key. The result is ordered but is not a cross-partition snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed bounds/token, cold discovery, transport
    /// deadline, identity exhaustion, or configured response-bound violation.
    #[allow(clippy::too_many_lines)]
    pub async fn scan(&self, request: MultiScanRequest) -> Result<MultiScanPage> {
        validate_scan(&request)?;
        if request.max_items == 0 || request.max_bytes == 0 {
            return Ok(MultiScanPage {
                items: Vec::new(),
                continuation: None,
                terminal_failure: None,
            });
        }
        let deadline = Instant::now() + self.config.operation_timeout;
        let timeout_ms = u64::try_from(self.config.operation_timeout.as_millis())
            .map_err(|_| ClientError::InvalidRequest("operation timeout exceeds wire range".into()))?;
        let deadline_ms = wall_now_ms()
            .checked_add(timeout_ms)
            .ok_or(ClientError::Deadline)?;
        let mut items = Vec::new();
        let mut bytes = 0_usize;
        let mut last_key = request.continuation.as_ref().map(|token| token.last_key.clone());
        let mut replans = 0_u32;

        'replan: loop {
            let map = match self.cache.load() {
                Some(map) => map,
                None => self.refresh_with_deadline(deadline).await?,
            };
            let mut entries: Vec<_> = map
                .entries()
                .iter()
                .filter(|entry| {
                    range_intersects(
                        &entry.range.start,
                        entry.range.end.as_deref(),
                        request.start.as_deref(),
                        request.end.as_deref(),
                    )
                })
                .cloned()
                .collect();
            if request.direction == ScanDirection::Reverse {
                entries.reverse();
            }
            for entry in entries {
                if partition_consumed(
                    &entry.range.start,
                    entry.range.end.as_deref(),
                    last_key.as_deref(),
                    request.direction,
                ) {
                    continue;
                }
                let mut partition_token = last_key.as_ref().and_then(|last_key| {
                    entry.range.contains(last_key).then(|| ScanContinuation {
                        direction: request.direction,
                        last_key: last_key.clone(),
                        partition_id: entry.partition_id,
                        owner_epoch: entry.owner_epoch,
                        map_revision: map.generation(),
                    })
                });
                let mut attempts = 0_u32;
                loop {
                    let remaining_items = request.max_items.saturating_sub(items.len());
                    if remaining_items == 0 || bytes >= request.max_bytes {
                        return Ok(scan_page(&request, items, last_key, map.generation(), None));
                    }
                    let rpc = ScanRequest {
                        routing: RequestRouting {
                            request_id: self.identities.allocate()?,
                            map_revision: map.generation(),
                            partition_id: entry.partition_id,
                            owner_epoch: entry.owner_epoch,
                            min_journal_position: None,
                            deadline_ms: Some(deadline_ms),
                        },
                        start: request.start.clone(),
                        end: request.end.clone(),
                        direction: request.direction,
                        limit: u32::try_from(remaining_items).unwrap_or(u32::MAX),
                        continuation: partition_token.clone(),
                    };
                    attempts += 1;
                    let remaining = deadline
                        .checked_duration_since(Instant::now())
                        .ok_or(ClientError::Deadline)?;
                    let response =
                        tokio::time::timeout(remaining, self.transport.scan(&entry.owner.rpc_endpoint, &rpc))
                            .await
                            .map_err(|_| ClientError::Deadline)?;
                    let response = match response {
                        Ok(response) => response,
                        Err(error) => {
                            if attempts >= self.config.max_attempts {
                                return Err(error);
                            }
                            if replans < self.config.max_route_refreshes {
                                replans += 1;
                                self.refresh_with_deadline(deadline).await?;
                                scan_backoff(self, deadline).await?;
                                continue 'replan;
                            }
                            scan_backoff(self, deadline).await?;
                            continue;
                        }
                    };
                    match response.result {
                        Ok(OperationResult::Scan {
                            items: page_items,
                            continuation,
                        }) => {
                            validate_scan_page(
                                &page_items,
                                continuation.as_ref(),
                                &rpc,
                                &entry.range.start,
                                entry.range.end.as_deref(),
                                last_key.as_deref(),
                            )?;
                            for item in page_items {
                                let item_bytes = item.key.len().saturating_add(item.value.len());
                                if items.len() == request.max_items
                                    || bytes.saturating_add(item_bytes) > request.max_bytes
                                    || bytes.saturating_add(item_bytes) > self.config.max_response_bytes
                                {
                                    if items.is_empty() {
                                        return Err(ClientError::TooLarge);
                                    }
                                    return Ok(scan_page(&request, items, last_key, map.generation(), None));
                                }
                                bytes += item_bytes;
                                last_key = Some(item.key.clone());
                                items.push(item);
                            }
                            attempts = 0;
                            match continuation {
                                Some(token) => partition_token = Some(token),
                                None => break,
                            }
                        }
                        Ok(_) => {
                            return Err(ClientError::InvalidRequest(
                                "scan RPC returned a non-scan result".into(),
                            ));
                        }
                        Err(failure) if topology_failure(failure.code) => {
                            if replans >= self.config.max_route_refreshes {
                                return Ok(scan_page(
                                    &request,
                                    items,
                                    last_key,
                                    map.generation(),
                                    Some(failure),
                                ));
                            }
                            replans += 1;
                            self.refresh_with_deadline(deadline).await?;
                            scan_backoff(self, deadline).await?;
                            continue 'replan;
                        }
                        Err(failure)
                            if transient_failure(failure.code) && attempts < self.config.max_attempts =>
                        {
                            scan_backoff(self, deadline).await?;
                        }
                        Err(failure) => {
                            return Ok(scan_page(
                                &request,
                                items,
                                last_key,
                                map.generation(),
                                Some(failure),
                            ));
                        }
                    }
                }
            }
            return Ok(MultiScanPage {
                items,
                continuation: None,
                terminal_failure: None,
            });
        }
    }
}

fn validate_scan(request: &MultiScanRequest) -> Result<()> {
    if request
        .start
        .as_ref()
        .zip(request.end.as_ref())
        .is_some_and(|(start, end)| start >= end)
    {
        return Err(ClientError::InvalidRequest(
            "scan bounds are empty or reversed".into(),
        ));
    }
    if let Some(token) = &request.continuation {
        if token.direction != request.direction
            || token.original_start != request.start
            || token.original_end != request.end
            || token.last_key.is_empty()
            || token.catalog_generation == 0
        {
            return Err(ClientError::InvalidRequest(
                "scan continuation does not match request".into(),
            ));
        }
    }
    Ok(())
}

fn range_intersects(
    partition_start: &[u8],
    partition_end: Option<&[u8]>,
    requested_start: Option<&[u8]>,
    requested_end: Option<&[u8]>,
) -> bool {
    requested_end.map_or(true, |end| partition_start < end)
        && partition_end.map_or(true, |end| requested_start.map_or(true, |start| end > start))
}

fn partition_consumed(
    partition_start: &[u8],
    partition_end: Option<&[u8]>,
    last_key: Option<&[u8]>,
    direction: ScanDirection,
) -> bool {
    last_key.is_some_and(|last_key| match direction {
        ScanDirection::Forward => partition_end.is_some_and(|end| end <= last_key),
        ScanDirection::Reverse => partition_start >= last_key,
    })
}

fn validate_scan_page(
    items: &[RpcValue],
    continuation: Option<&ScanContinuation>,
    request: &ScanRequest,
    partition_start: &[u8],
    partition_end: Option<&[u8]>,
    last_key: Option<&[u8]>,
) -> Result<()> {
    let ordered = items.windows(2).all(|pair| match request.direction {
        ScanDirection::Forward => pair[0].key < pair[1].key,
        ScanDirection::Reverse => pair[0].key > pair[1].key,
    });
    let after_last = items.first().map_or(true, |first| {
        last_key.map_or(true, |last| match request.direction {
            ScanDirection::Forward => first.key.as_slice() > last,
            ScanDirection::Reverse => first.key.as_slice() < last,
        })
    });
    let in_bounds = items.iter().all(|item| {
        item.key.as_slice() >= partition_start
            && partition_end.map_or(true, |end| item.key.as_slice() < end)
            && request
                .start
                .as_deref()
                .map_or(true, |start| item.key.as_slice() >= start)
            && request
                .end
                .as_deref()
                .map_or(true, |end| item.key.as_slice() < end)
    });
    let valid_continuation = continuation.map_or(true, |token| {
        !items.is_empty()
            && token.direction == request.direction
            && token.partition_id == request.routing.partition_id
            && token.owner_epoch == request.routing.owner_epoch
            && token.map_revision == request.routing.map_revision
            && items.last().is_some_and(|item| item.key == token.last_key)
    });
    if !ordered || !after_last || !in_bounds || !valid_continuation {
        return Err(ClientError::InvalidRequest(
            "partition scan response violates order, bounds, or continuation".into(),
        ));
    }
    Ok(())
}

fn scan_page(
    request: &MultiScanRequest,
    items: Vec<RpcValue>,
    last_key: Option<Vec<u8>>,
    catalog_generation: u64,
    terminal_failure: Option<RpcFailure>,
) -> MultiScanPage {
    MultiScanPage {
        continuation: last_key.map(|last_key| MultiScanContinuation {
            direction: request.direction,
            original_start: request.start.clone(),
            original_end: request.end.clone(),
            last_key,
            catalog_generation,
        }),
        items,
        terminal_failure,
    }
}

fn response_is_transient(response: &ChunkKvResponse) -> bool {
    response
        .result
        .as_ref()
        .err()
        .is_some_and(|failure| topology_failure(failure.code) || transient_failure(failure.code))
}

fn response_has_topology_failure(response: &ChunkKvResponse) -> bool {
    response
        .result
        .as_ref()
        .err()
        .is_some_and(|failure| topology_failure(failure.code))
}

async fn scan_backoff(client: &ChunkKvClient, deadline: Instant) -> Result<()> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(ClientError::Deadline)?;
    tokio::time::sleep(client.config.retry_backoff.min(remaining)).await;
    Ok(())
}

fn topology_failure(code: ChunkKvRpcErrorCode) -> bool {
    matches!(
        code,
        ChunkKvRpcErrorCode::NotMyRange | ChunkKvRpcErrorCode::RefreshRequired
    )
}

fn transient_failure(code: ChunkKvRpcErrorCode) -> bool {
    matches!(
        code,
        ChunkKvRpcErrorCode::Overloaded
            | ChunkKvRpcErrorCode::WriteStalled
            | ChunkKvRpcErrorCode::Recovering
            | ChunkKvRpcErrorCode::LeaseExpired
    )
}
