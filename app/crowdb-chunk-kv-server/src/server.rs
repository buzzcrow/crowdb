// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use crowdb_chunk_kv::{
    ChunkKvError, CompareCondition, JournalPosition, MutationOperation, MutationResult, Partition, RequestId,
    ValueRevision,
};
use crowdb_protocol::chunk_kv::{
    CatalogEntry, CatalogHead, CatalogPage, ChunkKvResponse, ChunkKvRpcErrorCode, Id128, OperationResult,
    OwnerHint, PointOperation, PointRequest, RequestRouting, RpcCompareCondition, RpcFailure,
    RpcJournalPosition, RpcValue,
};

use crate::{AuthorityError, CatalogError, ServerMetrics, ServingAuthority};

#[derive(Clone, Debug, Default)]
struct CatalogSnapshot {
    generation: u64,
    entries: Vec<CatalogEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerLifecycle {
    Prepared,
    Serving,
    Draining,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedPartitionHealth {
    pub partition_id: Id128,
    pub owner_epoch: u64,
    pub lifecycle: crowdb_chunk_kv::PartitionLifecycle,
    pub durable_seq: u64,
    pub applied_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerHealth {
    pub instance_id: u64,
    pub lifecycle: ServerLifecycle,
    pub catalog_generation: u64,
    pub partitions: Vec<HostedPartitionHealth>,
}

impl CatalogSnapshot {
    fn from_catalog(head: &CatalogHead, pages: &[CatalogPage]) -> Result<Self, CatalogError> {
        head.validate_pages(pages)?;
        Ok(Self {
            generation: head.generation,
            entries: pages
                .iter()
                .flat_map(|page| page.entries.iter().cloned())
                .collect(),
        })
    }

    fn entry_for_key(&self, key: &[u8]) -> Option<&CatalogEntry> {
        self.entries.iter().find(|entry| entry.range.contains(key))
    }
}

/// One process-local request surface hosting zero or more independent partitions.
///
/// Catalog and partition lookups are immutable lock-free snapshots. Lifecycle
/// changes replace a snapshot; admitted partition operations retain their own
/// handles until completion.
pub struct ChunkKvService {
    instance_id: u64,
    authority: Arc<ServingAuthority>,
    catalog: ArcSwap<CatalogSnapshot>,
    partitions: ArcSwap<HashMap<Id128, Partition>>,
    max_partitions: usize,
    admitting: AtomicBool,
    metrics: ServerMetrics,
}

impl ChunkKvService {
    /// Creates an empty, fenced service instance.
    ///
    /// # Errors
    ///
    /// Returns an invalid-request error for zero identity or capacity.
    pub fn new(instance_id: u64, max_partitions: usize) -> Result<Self, ChunkKvError> {
        if instance_id == 0 || max_partitions == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "server instance identity and partition capacity must be nonzero".into(),
            ));
        }
        Ok(Self {
            instance_id,
            authority: Arc::new(ServingAuthority::new(instance_id)),
            catalog: ArcSwap::from_pointee(CatalogSnapshot::default()),
            partitions: ArcSwap::from_pointee(HashMap::new()),
            max_partitions,
            admitting: AtomicBool::new(true),
            metrics: ServerMetrics::default(),
        })
    }

    #[must_use]
    pub fn authority(&self) -> &Arc<ServingAuthority> {
        &self.authority
    }

    #[must_use]
    pub fn metrics(&self) -> &ServerMetrics {
        &self.metrics
    }

    /// Stops new admission and clears serving authority. Operations already
    /// admitted to an R142 sequencer retain their partition handle and finish.
    pub fn begin_drain(&self) {
        self.admitting.store(false, Ordering::Release);
        self.authority.clear();
    }

    #[must_use]
    pub fn health(&self, now_monotonic_ms: u64) -> ServerHealth {
        let catalog_generation = self.catalog.load().generation;
        let mut partitions: Vec<HostedPartitionHealth> = self
            .partitions
            .load()
            .values()
            .map(|partition| {
                let snapshot = partition.snapshot();
                HostedPartitionHealth {
                    partition_id: Id128 {
                        high: snapshot.partition_id.high,
                        low: snapshot.partition_id.low,
                    },
                    owner_epoch: snapshot.ownership_epoch,
                    lifecycle: snapshot.lifecycle,
                    durable_seq: snapshot.journal_durable_seq,
                    applied_seq: snapshot.applied_seq,
                }
            })
            .collect();
        partitions.sort_unstable_by_key(|partition| partition.partition_id);
        let lifecycle = if !self.admitting.load(Ordering::Acquire) {
            ServerLifecycle::Draining
        } else if catalog_generation != 0 && self.authority.has_live_grant(now_monotonic_ms) {
            ServerLifecycle::Serving
        } else {
            ServerLifecycle::Prepared
        };
        ServerHealth {
            instance_id: self.instance_id,
            lifecycle,
            catalog_generation,
            partitions,
        }
    }

    /// Validates and atomically activates a newer complete catalog snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error without changing the active catalog when validation
    /// fails or the generation does not advance.
    pub fn install_catalog(&self, head: &CatalogHead, pages: &[CatalogPage]) -> Result<(), CatalogError> {
        let candidate = Arc::new(CatalogSnapshot::from_catalog(head, pages)?);
        let current = self.catalog.load_full();
        if candidate.generation <= current.generation {
            return Err(CatalogError::GenerationConflict);
        }
        self.catalog.rcu(|installed| {
            if installed.generation >= candidate.generation {
                Arc::clone(installed)
            } else {
                Arc::clone(&candidate)
            }
        });
        if self.catalog.load().generation == candidate.generation {
            Ok(())
        } else {
            Err(CatalogError::GenerationConflict)
        }
    }

    /// Adds or replaces a prepared partition handle in the lifecycle snapshot.
    ///
    /// # Errors
    ///
    /// Returns an overload error when adding beyond the configured capacity.
    pub fn install_partition(&self, partition: &Partition) -> Result<(), ChunkKvError> {
        let snapshot = partition.snapshot();
        let id = Id128 {
            high: snapshot.partition_id.high,
            low: snapshot.partition_id.low,
        };
        if !self.partitions.load().contains_key(&id) && self.partitions.load().len() >= self.max_partitions {
            return Err(ChunkKvError::Overloaded);
        }
        self.partitions.rcu(|current| {
            let mut next = (**current).clone();
            next.insert(id, partition.clone());
            Arc::new(next)
        });
        Ok(())
    }

    pub fn remove_partition(&self, partition_id: Id128) {
        self.partitions.rcu(|current| {
            let mut next = (**current).clone();
            next.remove(&partition_id);
            Arc::new(next)
        });
    }

    /// Handles a point request directly; it never proxies to another owner.
    ///
    /// The wall-clock deadline is checked before sequencer admission. Once a
    /// mutation is admitted, this method awaits its single durable result even
    /// if the caller drops the surrounding transport future.
    pub async fn handle_point(
        &self,
        request: PointRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> ChunkKvResponse {
        self.metrics.request();
        let response = self
            .handle_point_inner(request, now_wall_ms, now_monotonic_ms)
            .await;
        self.metrics.response(&response);
        response
    }

    async fn handle_point_inner(
        &self,
        request: PointRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> ChunkKvResponse {
        let catalog = self.catalog.load_full();
        if !self.admitting.load(Ordering::Acquire) {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::Recovering,
                "server is draining".into(),
            );
        }
        if let Err(error) = request.routing.validate() {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::InvalidRequest,
                error.to_string(),
            );
        }
        if request
            .routing
            .deadline_ms
            .is_some_and(|deadline| now_wall_ms >= deadline)
        {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::RequestExpired,
                "request deadline elapsed before admission".into(),
            );
        }
        let key = request.operation.key().to_vec();
        let Some(entry) = catalog.entry_for_key(&key) else {
            return not_my_range(catalog.generation, None);
        };
        if !matches_routing(&request.routing, catalog.generation, entry, self.instance_id) {
            return not_my_range(catalog.generation, Some(entry));
        }
        if let Err(error) = self.authority.authorize(
            catalog.generation,
            request.routing.partition_id,
            request.routing.owner_epoch,
            now_monotonic_ms,
        ) {
            return authority_failure(catalog.generation, &error, Some(entry));
        }
        let partition = self.partitions.load().get(&request.routing.partition_id).cloned();
        let Some(partition) = partition else {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::Recovering,
                "assigned partition is not prepared locally".into(),
            );
        };

        match request.operation {
            PointOperation::Get { key } => {
                let minimum = request.routing.min_journal_position.map(journal_position);
                match partition.get(request.routing.owner_epoch, &key, minimum).await {
                    Ok(value) => success(
                        catalog.generation,
                        None,
                        OperationResult::Value(value.map(|value| rpc_value(key, value))),
                    ),
                    Err(error) => partition_failure(catalog.generation, &error, Some(entry)),
                }
            }
            operation => {
                let request_id = RequestId {
                    client_high: request.routing.request_id.client_instance_id.high,
                    client_low: request.routing.request_id.client_instance_id.low,
                    client_sequence: request.routing.request_id.client_sequence,
                };
                let mutation = mutation_operation(operation);
                match partition
                    .mutate(request.routing.owner_epoch, request_id, mutation)
                    .await
                {
                    Ok(response) => {
                        let position = RpcJournalPosition {
                            stream_name: Id128 {
                                high: response.journal_position.stream_name.high,
                                low: response.journal_position.stream_name.low,
                            },
                            offset: response.journal_position.offset,
                        };
                        let result = match response.result {
                            MutationResult::Applied { revision } => OperationResult::Mutation {
                                applied: true,
                                revision: Some(revision),
                                observed: None,
                            },
                            MutationResult::ConditionFailed { observed } => OperationResult::Mutation {
                                applied: false,
                                revision: None,
                                observed: observed.map(|value| rpc_value(key, value)),
                            },
                        };
                        success(catalog.generation, Some(position), result)
                    }
                    Err(error) => partition_failure(catalog.generation, &error, Some(entry)),
                }
            }
        }
    }
}

fn matches_routing(
    routing: &RequestRouting,
    generation: u64,
    entry: &CatalogEntry,
    instance_id: u64,
) -> bool {
    routing.map_revision == generation
        && routing.partition_id == entry.partition_id
        && routing.owner_epoch == entry.owner_epoch
        && entry.owner.instance_id == instance_id
}

fn journal_position(position: RpcJournalPosition) -> JournalPosition {
    JournalPosition {
        stream_name: crowdb_chunk_stream::StreamName {
            high: position.stream_name.high,
            low: position.stream_name.low,
        },
        offset: position.offset,
    }
}

fn mutation_operation(operation: PointOperation) -> MutationOperation {
    match operation {
        PointOperation::Get { .. } => unreachable!("get is handled before mutation conversion"),
        PointOperation::Put { key, value } => MutationOperation::Put { key, value },
        PointOperation::Delete { key } => MutationOperation::Delete { key },
        PointOperation::PutIfAbsent { key, value } => MutationOperation::PutIfAbsent { key, value },
        PointOperation::CompareExchange {
            key,
            condition,
            value,
        } => MutationOperation::CompareExchange {
            key,
            condition: compare_condition(condition),
            value,
        },
        PointOperation::ConditionalDelete { key, condition } => MutationOperation::ConditionalDelete {
            key,
            condition: compare_condition(condition),
        },
    }
}

fn compare_condition(condition: RpcCompareCondition) -> CompareCondition {
    match condition {
        RpcCompareCondition::Revision(revision) => CompareCondition::Revision(revision),
        RpcCompareCondition::Value(value) => CompareCondition::Value(value),
    }
}

fn rpc_value(key: Vec<u8>, value: ValueRevision) -> RpcValue {
    RpcValue {
        key,
        value: value.value,
        revision: value.revision,
    }
}

fn success(
    map_revision: u64,
    journal_position: Option<RpcJournalPosition>,
    result: OperationResult,
) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision,
        journal_position,
        result: Ok(result),
    }
}

fn failure(map_revision: u64, code: ChunkKvRpcErrorCode, message: String) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision,
        journal_position: None,
        result: Err(RpcFailure {
            code,
            message,
            retry_after_ms: None,
            latest_map_revision: None,
            owner_hint: None,
        }),
    }
}

fn not_my_range(map_revision: u64, entry: Option<&CatalogEntry>) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision,
        journal_position: None,
        result: Err(RpcFailure {
            code: ChunkKvRpcErrorCode::NotMyRange,
            message: "request routing does not match the active owner".into(),
            retry_after_ms: None,
            latest_map_revision: Some(map_revision),
            owner_hint: entry.map(|entry| OwnerHint {
                instance_id: entry.owner.instance_id,
                rpc_endpoint: entry.owner.rpc_endpoint.clone(),
                owner_epoch: entry.owner_epoch,
            }),
        }),
    }
}

fn authority_failure(
    map_revision: u64,
    error: &AuthorityError,
    entry: Option<&CatalogEntry>,
) -> ChunkKvResponse {
    match error {
        AuthorityError::LeaseExpired => {
            failure(map_revision, ChunkKvRpcErrorCode::LeaseExpired, error.to_string())
        }
        _ => not_my_range(map_revision, entry),
    }
}

fn partition_failure(
    map_revision: u64,
    error: &ChunkKvError,
    entry: Option<&CatalogEntry>,
) -> ChunkKvResponse {
    let code = match error {
        ChunkKvError::OutOfRange | ChunkKvError::StaleEpoch => {
            return not_my_range(map_revision, entry);
        }
        ChunkKvError::Overloaded => ChunkKvRpcErrorCode::Overloaded,
        ChunkKvError::Recovering | ChunkKvError::NotServing(_) => ChunkKvRpcErrorCode::Recovering,
        ChunkKvError::WriteStalled
        | ChunkKvError::ApplyStateUnknown
        | ChunkKvError::MaintenanceDegraded(_) => ChunkKvRpcErrorCode::WriteStalled,
        ChunkKvError::RequestExpired => ChunkKvRpcErrorCode::RequestExpired,
        ChunkKvError::RequestConflict => ChunkKvRpcErrorCode::RequestConflict,
        ChunkKvError::InvalidRequest(_) | ChunkKvError::IncompleteFrame => {
            ChunkKvRpcErrorCode::InvalidRequest
        }
        ChunkKvError::JournalCorruption(_)
        | ChunkKvError::TreeUnavailable(_)
        | ChunkKvError::TreeCorruption(_)
        | ChunkKvError::SplitRetry(_)
        | ChunkKvError::Faulted(_)
        | ChunkKvError::Internal(_) => ChunkKvRpcErrorCode::Internal,
    };
    failure(map_revision, code, error.to_string())
}
