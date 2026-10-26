// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use crowdb_chunk_kv::{
    ChunkKvError, CompareCondition, JournalPosition, MutationOperation, MutationResult, Partition,
    PreparedSplitWriterArtifact, RequestId, SplitArtifact, ValueRevision,
};
use crowdb_kv_client::StreamProcessAuthority;
use crowdb_protocol::chunk_kv::{
    BatchMutationRequest, BatchMutationResponse, BatchMutationResult, ChunkKvRangeCatalogEntry,
    ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPartitionState, ChunkKvResponse,
    ChunkKvRpcErrorCode, Id128, KeyRange, MultiGetRequest, MultiGetResponse, OperationResult, OwnerHint,
    PointOperation, PointRequest, RequestRouting, RpcCompareCondition, RpcFailure, RpcJournalPosition,
    RpcValue, ScanContinuation, ScanRequest, SeekKind, SeekRequest,
};
use crowdb_protocol::common::{ChunkKvExtra, ChunkKvPartitionLoad};
use thiserror::Error;
use tracing::info;

use crate::{
    validate_and_clip_scan, AuthorityError, ChunkKvRangeCatalogError, ScanValidationError, ServerMetrics,
    ServingAuthority,
};

mod activation;
mod assignments;
mod authority;
mod drain;
mod group;
use assignments::{local_partition_for_entry, partition_matches_entry};
mod load_sampling;
mod maintenance;
mod observation;
mod prepared_transfer;
mod readiness;
mod recovery;
pub use readiness::RangeReadiness;
mod reconcile;
mod routing;
mod scan;
pub(crate) use observation::PageQuery;

const DEFAULT_SCAN_RESPONSE_BYTES: usize = 17 * 1024 * 1024;
const DEFAULT_INITIALIZATION_WAITERS: usize = 1_024;
const DEFAULT_INITIALIZATION_WAIT_MS: u64 = 250;

#[derive(Clone, Debug, Default)]
struct CatalogSnapshot {
    generation: u64,
    entries: Vec<ChunkKvRangeCatalogEntry>,
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
    pub serving_partitions: usize,
    pub ready: bool,
    pub range_readiness: Vec<RangeReadiness>,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum ChunkKvRangeCatalogReconcileError {
    #[error(transparent)]
    Catalog(#[from] ChunkKvRangeCatalogError),
    #[error(transparent)]
    Partition(#[from] ChunkKvError),
}

impl CatalogSnapshot {
    fn from_catalog(
        head: &ChunkKvRangeCatalogHead,
        pages: &[ChunkKvRangeCatalogPage],
    ) -> Result<Self, ChunkKvRangeCatalogError> {
        head.validate_pages(pages)?;
        Ok(Self {
            generation: head.generation,
            entries: pages
                .iter()
                .flat_map(|page| page.entries.iter().cloned())
                .collect(),
        })
    }

    fn entry_for_key(&self, key: &[u8]) -> Option<&ChunkKvRangeCatalogEntry> {
        self.entries.iter().find(|entry| entry.range.contains(key))
    }

    fn entry_for_partition(&self, partition_id: Id128) -> Option<&ChunkKvRangeCatalogEntry> {
        self.entries
            .iter()
            .find(|entry| entry.partition_id == partition_id)
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
    prepared_transfer_targets: ArcSwap<HashMap<Id128, Partition>>,
    local_split_sessions: ArcSwap<HashMap<Id128, LocalSplitSession>>,
    independently_recoverable: ArcSwap<HashSet<(Id128, u64)>>,
    load_sampling: Arc<load_sampling::LoadSampling>,
    max_partitions: usize,
    max_scan_response_bytes: usize,
    max_initialization_waiters: usize,
    initialization_wait_ms: u64,
    initialization_waiters: AtomicUsize,
    admitting: AtomicBool,
    metrics: ServerMetrics,
}

/// One local split handoff, keyed by its old parent identity.
///
/// Its presence is the sole process-local readiness fact for old-topology
/// dispatch.  The catalog remains the durable cross-process fact.
#[derive(Clone)]
struct LocalSplitSession {
    artifact: SplitArtifact,
    dispatcher: Partition,
}

struct InitializationWaiter<'a> {
    count: &'a AtomicUsize,
}

impl Drop for InitializationWaiter<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ChunkKvService {
    /// Creates an empty, fenced service instance.
    ///
    /// # Errors
    ///
    /// Returns an invalid-request error for zero identity or capacity.
    pub fn new(process: Arc<StreamProcessAuthority>, max_partitions: usize) -> Result<Self, ChunkKvError> {
        Self::new_with_limits(
            process,
            max_partitions,
            DEFAULT_SCAN_RESPONSE_BYTES,
            DEFAULT_INITIALIZATION_WAITERS,
            DEFAULT_INITIALIZATION_WAIT_MS,
        )
    }

    /// Creates an empty service with explicit hosting and scan response bounds.
    ///
    /// # Errors
    ///
    /// Returns an invalid-request error when any identity or bound is zero.
    pub fn new_with_limits(
        process: Arc<StreamProcessAuthority>,
        max_partitions: usize,
        max_scan_response_bytes: usize,
        max_initialization_waiters: usize,
        initialization_wait_ms: u64,
    ) -> Result<Self, ChunkKvError> {
        let identity = process
            .capture(Instant::now())
            .map_err(|error| ChunkKvError::InvalidRequest(error.to_string()))?
            .identity();
        let instance_id = identity.instance_id;
        if identity.purpose != crowdb_protocol::chunk_stream::StreamPurpose::Wal
            || instance_id == 0
            || max_partitions == 0
            || max_scan_response_bytes == 0
            || max_initialization_waiters == 0
            || initialization_wait_ms == 0
        {
            return Err(ChunkKvError::InvalidRequest(
                "server instance identity and capacity bounds must be nonzero".into(),
            ));
        }
        Ok(Self {
            instance_id,
            authority: Arc::new(ServingAuthority::new(process)),
            catalog: ArcSwap::from_pointee(CatalogSnapshot::default()),
            partitions: ArcSwap::from_pointee(HashMap::new()),
            prepared_transfer_targets: ArcSwap::from_pointee(HashMap::new()),
            local_split_sessions: ArcSwap::from_pointee(HashMap::new()),
            independently_recoverable: ArcSwap::from_pointee(HashSet::new()),
            load_sampling: Arc::new(load_sampling::LoadSampling::default()),
            max_partitions,
            max_scan_response_bytes,
            max_initialization_waiters,
            initialization_wait_ms,
            initialization_waiters: AtomicUsize::new(0),
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

    #[must_use]
    pub fn metrics_snapshot(&self) -> crate::ServerMetricsSnapshot {
        let mut result = self.metrics.snapshot();
        for partition in self.partitions.load().values() {
            let metrics = partition.metrics().snapshot();
            result.admission_backpressure = result
                .admission_backpressure
                .saturating_add(metrics.admission_backpressure);
            result.recoveries = result.recoveries.saturating_add(metrics.recoveries);
            result.split_finalizations = result
                .split_finalizations
                .saturating_add(metrics.split_finalizations);
            result.split_commits = result.split_commits.saturating_add(metrics.split_commits);
            result.split_catchup_lag_records = result
                .split_catchup_lag_records
                .max(metrics.split_catchup_lag_records);
            result.split_finalization_duration_us = result
                .split_finalization_duration_us
                .max(metrics.split_finalization_duration_us);
            result.split_tail_records = result
                .split_tail_records
                .saturating_add(metrics.split_delta_records);
            result.split_tail_bytes = result.split_tail_bytes.saturating_add(metrics.split_tail_bytes);
            result.split_preparation_duration_us = result
                .split_preparation_duration_us
                .max(metrics.split_preparation_duration_us);
            result.split_base_checkpoint_duration_us = result
                .split_base_checkpoint_duration_us
                .max(metrics.split_base_checkpoint_duration_us);
            result.split_overlay_apply_records = result
                .split_overlay_apply_records
                .saturating_add(metrics.split_overlay_apply_records);
            result.split_overlay_apply_bytes = result
                .split_overlay_apply_bytes
                .saturating_add(metrics.split_overlay_apply_bytes);
            result.materialization_duration_us = result
                .materialization_duration_us
                .saturating_add(metrics.materialization_duration_us);
        }
        for session in self.local_split_sessions.load().values() {
            let metrics = session.dispatcher.metrics().snapshot();
            result.split_finalizations = result
                .split_finalizations
                .saturating_add(metrics.split_finalizations);
            result.split_commits = result.split_commits.saturating_add(metrics.split_commits);
            result.split_finalization_duration_us = result
                .split_finalization_duration_us
                .max(metrics.split_finalization_duration_us);
            result.split_tail_records = result
                .split_tail_records
                .saturating_add(metrics.split_delta_records);
            result.split_tail_bytes = result.split_tail_bytes.saturating_add(metrics.split_tail_bytes);
            result.split_preparation_duration_us = result
                .split_preparation_duration_us
                .max(metrics.split_preparation_duration_us);
        }
        result
    }

    /// Milliseconds elapsed on the process-local monotonic clock.
    #[must_use]
    pub fn monotonic_ms(&self) -> u64 {
        self.authority.monotonic_ms()
    }

    /// Stops new admission and revokes successful serving replies. Admitted
    /// operations retain their partition handle and can finish durable work.
    pub fn begin_drain(&self) {
        self.admitting.store(false, Ordering::Release);
        self.authority.clear();
    }

    #[must_use]
    pub fn health(&self, now_monotonic_ms: u64) -> ServerHealth {
        let catalog = self.catalog.load();
        let catalog_generation = catalog.generation;
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
        } else if self.authority.process().capture(Instant::now()).is_ok()
            && catalog_generation != 0
            && (!catalog.entries.iter().any(|entry| {
                entry.owner.instance_id == self.instance_id
                    && entry.state != ChunkKvRangeCatalogPartitionState::Retired
            }) || self
                .authority
                .has_live_grant(catalog_generation, now_monotonic_ms))
        {
            ServerLifecycle::Serving
        } else {
            ServerLifecycle::Prepared
        };
        let serving_partitions = catalog
            .entries
            .iter()
            .filter(|entry| {
                entry.owner.instance_id == self.instance_id
                    && entry.state == ChunkKvRangeCatalogPartitionState::Serving
                    && partitions.iter().any(|partition| {
                        partition.partition_id == entry.partition_id
                            && partition.owner_epoch == entry.owner_epoch
                            && partition.lifecycle == crowdb_chunk_kv::PartitionLifecycle::Serving
                    })
            })
            .count();
        let range_readiness = self.assignment_readiness(&catalog, now_monotonic_ms);
        let recovery_required = self.recovery_required();
        let ready = lifecycle == ServerLifecycle::Serving
            && range_readiness.iter().all(|range| range.ready)
            && recovery_required.is_empty();
        let detail = if !recovery_required.is_empty() {
            format!("Writer outcome requires durable replay before serving: {recovery_required:?}")
        } else if range_readiness.is_empty() && ready {
            format!("Idle stateless instance: catalog generation {catalog_generation} loaded, no tree-range ownership assigned")
        } else {
            format!("Instance lifecycle {lifecycle:?}; catalog generation {catalog_generation}; {} owned tree ranges, {} ready", range_readiness.len(), range_readiness.iter().filter(|range| range.ready).count())
        };
        ServerHealth {
            instance_id: self.instance_id,
            lifecycle,
            catalog_generation,
            partitions,
            serving_partitions,
            ready,
            range_readiness,
            detail,
        }
    }

    /// Builds the service-registry payload from immutable partition snapshots.
    #[must_use]
    pub fn registry_observation(&self, capacity_bytes: u64, request_rate: u64) -> ChunkKvExtra {
        let partitions = self.observed_partition_snapshot();
        let durable_bytes = partitions
            .values()
            .filter_map(|partition| partition.estimated_bytes().ok())
            .fold(0_u64, u64::saturating_add);
        let mut hosted: Vec<_> = partitions
            .values()
            .map(|partition| {
                let snapshot = partition.snapshot();
                crowdb_protocol::chunk_kv::HostedPartition {
                    partition_id: Id128 {
                        high: snapshot.partition_id.high,
                        low: snapshot.partition_id.low,
                    },
                    owner_epoch: snapshot.ownership_epoch,
                    recovering: !matches!(
                        snapshot.lifecycle,
                        crowdb_chunk_kv::PartitionLifecycle::Prepared
                            | crowdb_chunk_kv::PartitionLifecycle::Serving
                            | crowdb_chunk_kv::PartitionLifecycle::SplitPreparing
                            | crowdb_chunk_kv::PartitionLifecycle::SplitFinalizing
                    ),
                }
            })
            .collect();
        hosted.sort_unstable_by_key(|partition| partition.partition_id);
        ChunkKvExtra {
            process_epoch: self
                .authority
                .process()
                .status(Instant::now())
                .lease
                .identity
                .process_epoch,
            capacity_bytes,
            durable_bytes,
            request_rate,
            hosted,
            partition_loads: Vec::new(),
            ..ChunkKvExtra::default()
        }
    }

    /// Builds a heartbeat using cached bounded split samples. Starts at most
    /// one background observation job, without delaying heartbeat publication.
    ///
    /// # Errors
    ///
    /// Storage read errors leave the affected partition unsampled.
    pub fn registry_observation_with_load_samples(
        &self,
        capacity_bytes: u64,
        request_rate: u64,
        max_samples: usize,
    ) -> std::future::Ready<Result<ChunkKvExtra, ChunkKvError>> {
        let mut observation = self.registry_observation(capacity_bytes, request_rate);
        let partitions = self.partitions.load_full();
        if max_samples >= 2 {
            self.load_sampling.launch(Arc::clone(&partitions), max_samples);
        }
        for (id, partition) in partitions.iter() {
            let snapshot = partition.snapshot();
            let Ok(durable_bytes) = partition.estimated_bytes() else {
                continue;
            };
            let summary = partition
                .observe_tree()
                .ok()
                .and_then(|observation| observation.summary);
            let (logical_bytes, logical_metrics_exact) =
                summary.map_or((0, false), |summary| (summary.live_logical_bytes, summary.exact));
            let live_byte_samples =
                if max_samples >= 2 && snapshot.lifecycle == crowdb_chunk_kv::PartitionLifecycle::Serving {
                    self.load_sampling.samples(*id, snapshot.ownership_epoch)
                } else {
                    Vec::new()
                };
            observation.partition_loads.push(ChunkKvPartitionLoad {
                partition_id: Id128 {
                    high: snapshot.partition_id.high,
                    low: snapshot.partition_id.low,
                },
                durable_bytes,
                logical_bytes,
                logical_metrics_exact,
                live_byte_samples,
                independently_recoverable: self.independently_recoverable.load().contains(&(
                    Id128 {
                        high: snapshot.partition_id.high,
                        low: snapshot.partition_id.low,
                    },
                    snapshot.ownership_epoch,
                )) || self
                    .catalog
                    .load()
                    .entry_for_partition(Id128 {
                        high: snapshot.partition_id.high,
                        low: snapshot.partition_id.low,
                    })
                    .is_some_and(|entry| {
                        entry.artifact.tail_overlay.is_none() && partition_matches_entry(partition, entry)
                    }),
            });
        }
        observation
            .partition_loads
            .sort_unstable_by_key(|load| load.partition_id);
        std::future::ready(Ok(observation))
    }

    /// Validates and atomically activates a newer complete catalog snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error without changing the active catalog when validation
    /// fails or the generation does not advance.
    pub fn install_catalog(
        &self,
        head: &ChunkKvRangeCatalogHead,
        pages: &[ChunkKvRangeCatalogPage],
    ) -> Result<(), ChunkKvRangeCatalogError> {
        let candidate = Arc::new(CatalogSnapshot::from_catalog(head, pages)?);
        self.activate_catalog(&candidate)
    }

    fn activate_catalog(&self, candidate: &Arc<CatalogSnapshot>) -> Result<(), ChunkKvRangeCatalogError> {
        let current = self.catalog.load_full();
        if candidate.generation <= current.generation {
            return Err(ChunkKvRangeCatalogError::GenerationConflict);
        }
        self.catalog.rcu(|installed| {
            if installed.generation >= candidate.generation {
                Arc::clone(installed)
            } else {
                Arc::clone(candidate)
            }
        });
        if self.catalog.load().generation == candidate.generation {
            Ok(())
        } else {
            Err(ChunkKvRangeCatalogError::GenerationConflict)
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

    /// Returns whether the catalog assignment already has a locally prepared
    /// writer.  A split writer is identified by its durable split lineage,
    /// rather than by the catalog fields which advertise that lineage.
    #[must_use]
    pub fn hosts_catalog_assignment(&self, entry: &ChunkKvRangeCatalogEntry) -> bool {
        self.hosted_partition(entry.partition_id)
            .is_some_and(|partition| local_partition_for_entry(&partition, entry).is_some())
            || self.local_split_writer(entry).is_some()
    }

    /// Records the active local writer pair before group-0 advertises it.
    ///
    /// # Errors
    ///
    /// Returns an error unless the old parent dispatches to the exact durable
    /// retained-parent and child writers.
    pub async fn record_local_split_ready(&self, artifact: &SplitArtifact) -> Result<(), ChunkKvError> {
        let parent_id = Id128 {
            high: artifact.parent_id.high,
            low: artifact.parent_id.low,
        };
        if let Some(session) = self.local_split_sessions.load().get(&parent_id) {
            if session.artifact == *artifact {
                return Ok(());
            }
            if session.artifact.transition_id == artifact.transition_id
                || session.artifact.parent_next_epoch != artifact.parent_epoch
            {
                return Err(ChunkKvError::SplitRetry(
                    "local split session conflicts with durable artifact".into(),
                ));
            }
        }
        let parent = self.hosted_partition(parent_id).ok_or(ChunkKvError::OutOfRange)?;
        let ingress = parent
            .split_ingress()
            .ok_or_else(|| ChunkKvError::SplitRetry("prepared split ingress is not active".into()))?;
        if !split_writer_matches_artifact(&ingress.retained_parent(), &artifact.retained_parent)
            || !split_writer_matches_artifact(&ingress.child(), &artifact.child)
        {
            return Err(ChunkKvError::SplitRetry(
                "prepared split ingress does not match its durable artifact".into(),
            ));
        }
        let retained_parent = ingress.retained_parent();
        let child = ingress.child();
        parent.complete_local_split_handoff(artifact).await?;
        self.local_split_sessions.rcu(|current| {
            let mut next = (**current).clone();
            next.insert(
                parent_id,
                LocalSplitSession {
                    artifact: artifact.clone(),
                    dispatcher: parent.clone(),
                },
            );
            Arc::new(next)
        });
        self.partitions.rcu(|current| {
            let mut next = (**current).clone();
            next.insert(parent_id, retained_parent.clone());
            let child_snapshot = child.snapshot();
            next.insert(
                Id128 {
                    high: child_snapshot.partition_id.high,
                    low: child_snapshot.partition_id.low,
                },
                child.clone(),
            );
            Arc::new(next)
        });
        info!(
            transition_id_high = artifact.transition_id.high,
            transition_id_low = artifact.transition_id.low,
            parent_id_high = artifact.parent_id.high,
            parent_id_low = artifact.parent_id.low,
            parent_epoch = artifact.parent_epoch,
            parent_next_epoch = artifact.parent_next_epoch,
            child_id_high = artifact.child.partition_id.high,
            child_id_low = artifact.child.partition_id.low,
            retained_stream_high = artifact.retained_parent.stream_name.high,
            retained_stream_low = artifact.retained_parent.stream_name.low,
            child_stream_high = artifact.child.stream_name.high,
            child_stream_low = artifact.child.stream_name.low,
            cutover_seq = artifact.cutover_seq,
            retained_base_seq = artifact.retained_parent.base_applied_seq,
            retained_applied_seq = artifact.retained_parent.applied_seq,
            retained_tree_manifest = artifact.retained_parent.tree_manifest,
            retained_root_manifest_generation = artifact.retained_parent.root_manifest_generation,
            retained_parent_stream_generation = artifact.retained_parent.parent_stream_generation,
            child_base_seq = artifact.child.base_applied_seq,
            child_applied_seq = artifact.child.applied_seq,
            child_tree_manifest = artifact.child.tree_manifest,
            child_root_manifest_generation = artifact.child.root_manifest_generation,
            child_parent_stream_generation = artifact.child.parent_stream_generation,
            "local split writers installed"
        );
        Ok(())
    }

    /// Returns the immutable artifact of the active local handoff.
    #[must_use]
    pub(crate) fn local_split_artifact(
        &self,
        parent_id: Id128,
        transition_id: crowdb_chunk_kv::TransitionId,
    ) -> Option<SplitArtifact> {
        self.local_split_sessions
            .load()
            .get(&parent_id)
            .filter(|session| {
                session.artifact.transition_id.high == transition_id.high
                    && session.artifact.transition_id.low == transition_id.low
            })
            .map(|session| session.artifact.clone())
    }

    pub fn remove_partition(&self, partition_id: Id128) {
        self.partitions.rcu(|current| {
            let mut next = (**current).clone();
            next.remove(&partition_id);
            Arc::new(next)
        });
    }

    pub(crate) fn hosted_partition(&self, partition_id: Id128) -> Option<Partition> {
        self.partitions
            .load()
            .get(&partition_id)
            .cloned()
            .or_else(|| self.prepared_transfer_targets.load().get(&partition_id).cloned())
    }

    pub(crate) fn split_transition_parent(&self, partition_id: Id128) -> Option<Partition> {
        self.hosted_partition(partition_id).or_else(|| {
            self.local_split_sessions
                .load()
                .get(&partition_id)
                .map(|session| session.dispatcher.clone())
        })
    }

    /// Clears a locally prepared split after the catalog durably rejected it.
    /// The transition identity and child epoch fence every removal so a late
    /// abort cannot discard a newer assignment that reused the child handle.
    pub(crate) async fn abort_local_split(
        &self,
        transition: &crowdb_protocol::chunk_kv::SplitTransition,
        catalog_revision: u64,
    ) -> Result<(), ChunkKvError> {
        let parent_id = transition.parent_id;
        let parent_key = Id128 {
            high: parent_id.high,
            low: parent_id.low,
        };
        let parent = self.split_transition_parent(parent_key);
        let session = self.local_split_sessions.load().get(&parent_key).cloned();
        let proof = crowdb_chunk_kv::SplitAbortProof {
            catalog_revision,
            transition_id: crowdb_chunk_kv::TransitionId {
                high: transition.transition_id.high,
                low: transition.transition_id.low,
            },
            parent_id: crowdb_chunk_kv::PartitionId {
                high: parent_id.high,
                low: parent_id.low,
            },
            parent_epoch: transition.parent_epoch,
        };
        if let Some(parent) = parent {
            match parent.abort_split(&proof).await {
                Ok(()) | Err(ChunkKvError::SplitRetry(_)) => {}
                Err(error) => return Err(error),
            }
        }
        self.local_split_sessions.rcu(|current| {
            let mut next = (**current).clone();
            if next.get(&parent_key).is_some_and(|active| {
                active.artifact.transition_id.high == transition.transition_id.high
                    && active.artifact.transition_id.low == transition.transition_id.low
            }) {
                next.remove(&parent_key);
            }
            Arc::new(next)
        });
        self.partitions.rcu(|current| {
            let mut next = (**current).clone();
            if let Some(session) = &session {
                if session.artifact.transition_id.high == transition.transition_id.high
                    && session.artifact.transition_id.low == transition.transition_id.low
                {
                    next.insert(parent_key, session.dispatcher.clone());
                }
            }
            let child_key = Id128 {
                high: transition.child.partition_id.high,
                low: transition.child.partition_id.low,
            };
            if next
                .get(&child_key)
                .is_some_and(|child| child.snapshot().ownership_epoch == transition.child.owner_epoch)
            {
                next.remove(&child_key);
            }
            Arc::new(next)
        });
        Ok(())
    }

    /// Handles a point request directly; it never proxies to another owner.
    ///
    /// The wall-clock deadline is checked before sequencer admission. Once a
    /// mutation is admitted, this method awaits its durable result and checks
    /// serving authority again before returning success.
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
        let mut transfer_append_artifact = None;
        let partition = if entry.state == ChunkKvRangeCatalogPartitionState::TargetCatchingUp {
            if !matches_routing(&request.routing, catalog.generation, entry, self.instance_id) {
                return not_my_range(catalog.generation, Some(entry));
            }
            let Some(partition) = self
                .hosted_partition(entry.partition_id)
                .filter(|partition| partition_matches_entry(partition, entry))
            else {
                return target_not_ready(catalog.generation);
            };
            if operation_requires_initialized_view(&request.operation) {
                if let Err(response) = self
                    .await_target_initialization(
                        &partition,
                        request.routing.deadline_ms,
                        now_wall_ms,
                        catalog.generation,
                        entry,
                    )
                    .await
                {
                    return *response;
                }
            } else if partition.lifecycle() != crowdb_chunk_kv::PartitionLifecycle::Serving {
                let Some(artifact) = final_transfer_artifact(&partition, entry) else {
                    return target_not_ready(catalog.generation);
                };
                transfer_append_artifact = Some(artifact);
            }
            partition
        } else {
            match self.resolve_point_partition(
                &request.routing,
                &key,
                catalog.generation,
                entry,
                now_monotonic_ms,
            ) {
                Ok(partition) => partition,
                Err(response) => return *response,
            }
        };

        self.execute_authorized_point(
            &partition,
            entry,
            catalog.generation,
            request,
            transfer_append_artifact,
            now_monotonic_ms,
        )
        .await
    }

    async fn execute_point_operation(
        &self,
        partition: &Partition,
        entry: &ChunkKvRangeCatalogEntry,
        map_revision: u64,
        request: PointRequest,
        transfer_append_artifact: Option<PreparedSplitWriterArtifact>,
    ) -> ChunkKvResponse {
        let key = request.operation.key().to_vec();
        match request.operation {
            PointOperation::Get { key } => {
                let minimum = request.routing.min_journal_position.map(journal_position);
                match partition
                    .get(partition.snapshot().ownership_epoch, &key, minimum)
                    .await
                {
                    Ok(value) => success(
                        map_revision,
                        None,
                        OperationResult::Value(value.map(|value| rpc_value(key, value))),
                    ),
                    Err(error) => partition_failure(map_revision, &error, Some(entry)),
                }
            }
            operation => {
                let request_id = RequestId {
                    client_high: request.routing.request_id.client_instance_id.high,
                    client_low: request.routing.request_id.client_instance_id.low,
                    client_sequence: request.routing.request_id.client_sequence,
                };
                let mutation = mutation_operation(operation);
                let ownership_epoch = partition.snapshot().ownership_epoch;
                let mutation = if let Some(artifact) = transfer_append_artifact {
                    partition
                        .append_prepared_transfer_mutation(artifact, ownership_epoch, request_id, mutation)
                        .await
                } else {
                    partition.mutate(ownership_epoch, request_id, mutation).await
                };
                match mutation {
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
                        success(map_revision, Some(position), result)
                    }
                    Err(error) => partition_failure(map_revision, &error, Some(entry)),
                }
            }
        }
    }

    async fn await_target_initialization(
        &self,
        partition: &Partition,
        deadline_ms: Option<u64>,
        now_wall_ms: u64,
        map_revision: u64,
        entry: &ChunkKvRangeCatalogEntry,
    ) -> Result<(), Box<ChunkKvResponse>> {
        if self
            .initialization_waiters
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.max_initialization_waiters).then_some(current + 1)
            })
            .is_err()
        {
            return Err(Box::new(target_not_ready(map_revision)));
        }
        let _waiter = InitializationWaiter {
            count: &self.initialization_waiters,
        };
        let deadline_remaining = deadline_ms.map(|deadline| deadline.saturating_sub(now_wall_ms));
        let wait_ms = deadline_remaining.map_or(self.initialization_wait_ms, |remaining| {
            remaining.min(self.initialization_wait_ms)
        });
        match tokio::time::timeout(
            Duration::from_millis(wait_ms),
            partition.await_transfer_initialization(),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(Box::new(partition_failure(map_revision, &error, Some(entry)))),
            Err(_)
                if deadline_remaining.is_some_and(|remaining| remaining <= self.initialization_wait_ms) =>
            {
                Err(Box::new(failure(
                    map_revision,
                    ChunkKvRpcErrorCode::RequestExpired,
                    "request deadline elapsed while target initialized".into(),
                )))
            }
            Err(_) => Err(Box::new(target_not_ready(map_revision))),
        }
    }

    /// Handles one ordered seek directly against a partition view.
    pub async fn handle_seek(
        &self,
        request: SeekRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> ChunkKvResponse {
        self.metrics.request();
        let response = self
            .handle_seek_inner(request, now_wall_ms, now_monotonic_ms)
            .await;
        self.metrics.response(&response);
        response
    }

    async fn handle_seek_inner(
        &self,
        request: SeekRequest,
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
        if request.routing.validate().is_err() {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::InvalidRequest,
                "seek routing is invalid".into(),
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
        let Some(entry) = catalog.entry_for_key(&request.key) else {
            return not_my_range(catalog.generation, None);
        };
        if let Err(error) = self.authority.authorize(
            catalog.generation,
            entry.partition_id,
            entry.owner_epoch,
            now_monotonic_ms,
        ) {
            return authority_failure(catalog.generation, &error, Some(entry));
        }
        let Some(partition) = self.partition_for_request(&request.routing, &request.key, entry) else {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::Recovering,
                "assigned partition is not prepared locally".into(),
            );
        };
        let ingress = partition.split_ingress();
        let primary = ingress
            .as_ref()
            .map_or_else(|| partition.clone(), |route| route.writer_for_key(&request.key));
        if let Some(response) = self.authorize_resolved_writers(
            &catalog,
            now_monotonic_ms,
            entry.partition_id,
            std::slice::from_ref(&primary),
        ) {
            return response;
        }
        let minimum = request.routing.min_journal_position.map(journal_position);
        let mut result =
            execute_writer_seek(&primary, &request, primary.snapshot().ownership_epoch, minimum).await;
        if matches!(result, Ok(None)) {
            if let Some(route) = ingress.as_ref() {
                if let Some(secondary) = secondary_writer_for_seek(route, request.kind, &request.key) {
                    if let Some(response) = self.authorize_resolved_writers(
                        &catalog,
                        now_monotonic_ms,
                        entry.partition_id,
                        std::slice::from_ref(&secondary),
                    ) {
                        return response;
                    }
                    result = execute_boundary_seek(
                        route,
                        &secondary,
                        request.kind,
                        minimum,
                        self.max_scan_response_bytes,
                    )
                    .await;
                }
            }
        }
        match result {
            Ok(value) => success(
                catalog.generation,
                None,
                OperationResult::Value(value.map(scan_entry_value)),
            ),
            Err(error) => partition_failure(catalog.generation, &error, Some(entry)),
        }
    }

    /// Handles one bounded directional scan directly against a partition view.
    pub async fn handle_scan(
        &self,
        request: ScanRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> ChunkKvResponse {
        self.metrics.request();
        let response = self
            .handle_scan_inner(request, now_wall_ms, now_monotonic_ms, None)
            .await;
        self.metrics.response(&response);
        response
    }

    async fn handle_scan_inner(
        &self,
        request: ScanRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
        strict_budget: Option<usize>,
    ) -> ChunkKvResponse {
        let catalog = self.catalog.load_full();
        if !self.admitting.load(Ordering::Acquire) {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::Recovering,
                "server is draining".into(),
            );
        }
        if request.routing.validate().is_err() {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::InvalidRequest,
                "scan routing is invalid".into(),
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
        let Some(entry) = catalog.entry_for_partition(request.routing.partition_id) else {
            return not_my_range(catalog.generation, None);
        };
        // Select the routed assignment before clipping. A historical parent
        // scan can start wholly inside the child and still needs both writers.
        let partition = self.partition_for_request(&request.routing, &entry.range.start, entry);
        let logical_range = partition.as_ref().map_or_else(
            || entry.range.clone(),
            |partition| {
                let range = partition.snapshot().range;
                KeyRange {
                    start: range.start.unwrap_or_default(),
                    end: range.end,
                }
            },
        );
        let clipped = match validate_and_clip_scan(&request, &logical_range) {
            Ok(clipped) => clipped,
            Err(ScanValidationError::RefreshRequired) => {
                return failure(
                    catalog.generation,
                    ChunkKvRpcErrorCode::RefreshRequired,
                    "scan continuation topology is stale".into(),
                );
            }
            Err(ScanValidationError::NotMyRange) => {
                return not_my_range(catalog.generation, Some(entry));
            }
            Err(ScanValidationError::InvalidRequest) => {
                return failure(
                    catalog.generation,
                    ChunkKvRpcErrorCode::InvalidRequest,
                    "scan interval is invalid".into(),
                );
            }
        };
        if let Err(error) = self.authority.authorize(
            catalog.generation,
            entry.partition_id,
            entry.owner_epoch,
            now_monotonic_ms,
        ) {
            return authority_failure(catalog.generation, &error, Some(entry));
        }
        let Some(partition) = partition else {
            return failure(
                catalog.generation,
                ChunkKvRpcErrorCode::Recovering,
                "assigned partition is not prepared locally".into(),
            );
        };
        let scan_writers = partition
            .split_ingress()
            .map_or_else(|| vec![partition.clone()], |ingress| split_writers(&ingress));
        if let Some(response) =
            self.authorize_resolved_writers(&catalog, now_monotonic_ms, entry.partition_id, &scan_writers)
        {
            return response;
        }
        match self
            .execute_scan(&partition, &request, &clipped, strict_budget)
            .await
        {
            Ok(page) if strict_budget.is_some() && page.entries.is_empty() && page.truncated => failure(
                catalog.generation,
                ChunkKvRpcErrorCode::ScanLimitExceeded,
                "strict scan made no progress within byte or traversal budget".into(),
            ),
            Ok(page) => scan_success(catalog.generation, &request, page),
            Err(error) => partition_failure(catalog.generation, &error, Some(entry)),
        }
    }

    fn resolve_point_partition(
        &self,
        routing: &RequestRouting,
        key: &[u8],
        generation: u64,
        entry: &ChunkKvRangeCatalogEntry,
        now_monotonic_ms: u64,
    ) -> Result<Partition, Box<ChunkKvResponse>> {
        if !matches_routing(routing, generation, entry, self.instance_id) {
            self.metrics.split_stale_route_forward();
        }
        if let Err(error) = self.authority.authorize(
            generation,
            entry.partition_id,
            entry.owner_epoch,
            now_monotonic_ms,
        ) {
            return Err(Box::new(authority_failure(generation, &error, Some(entry))));
        }
        self.partition_for_request(routing, key, entry)
            .map(|partition| {
                partition
                    .split_ingress()
                    .map_or(partition.clone(), |ingress| ingress.writer_for_key(key))
            })
            .ok_or_else(|| {
                Box::new(failure(
                    generation,
                    ChunkKvRpcErrorCode::Recovering,
                    "assigned partition is not prepared locally".into(),
                ))
            })
    }

    fn authorize_resolved_writers(
        &self,
        catalog: &CatalogSnapshot,
        now_monotonic_ms: u64,
        already_authorized: Id128,
        writers: &[Partition],
    ) -> Option<ChunkKvResponse> {
        for writer in writers {
            let snapshot = writer.snapshot();
            let partition_id = Id128 {
                high: snapshot.partition_id.high,
                low: snapshot.partition_id.low,
            };
            if partition_id == already_authorized {
                continue;
            }
            let Some(entry) = catalog.entry_for_partition(partition_id) else {
                continue;
            };
            if let Err(error) = self.authority.authorize(
                catalog.generation,
                entry.partition_id,
                entry.owner_epoch,
                now_monotonic_ms,
            ) {
                return Some(authority_failure(catalog.generation, &error, Some(entry)));
            }
        }
        None
    }

    fn partition_for_catalog_entry(&self, entry: &ChunkKvRangeCatalogEntry) -> Option<Partition> {
        self.local_split_sessions
            .load()
            .get(&entry.partition_id)
            .map(|session| &session.dispatcher)
            .filter(|partition| partition_matches_entry(partition, entry))
            .cloned()
            .or_else(|| self.partitions.load().get(&entry.partition_id).cloned())
    }

    fn logical_range_for_request(&self, partition_id: Id128, catalog: &CatalogSnapshot) -> Option<KeyRange> {
        self.local_split_sessions
            .load()
            .get(&partition_id)
            .map(|session| {
                let dispatcher = &session.dispatcher;
                let range = dispatcher.snapshot().range;
                KeyRange {
                    start: range.start.unwrap_or_default(),
                    end: range.end,
                }
            })
            .or_else(|| {
                catalog
                    .entry_for_partition(partition_id)
                    .map(|entry| entry.range.clone())
            })
    }

    fn local_split_writer(&self, entry: &ChunkKvRangeCatalogEntry) -> Option<Partition> {
        self.local_split_sessions.load().values().find_map(|session| {
            let dispatcher = &session.dispatcher;
            // Until catalog publication the full-range parent assignment is
            // still served by this dispatcher. Reopening it would create a
            // second publisher for the retained original tree.
            if partition_matches_entry(dispatcher, entry) {
                return Some(dispatcher.clone());
            }
            let ingress = dispatcher.split_ingress()?;
            [ingress.retained_parent(), ingress.child()]
                .into_iter()
                .find(|writer| partition_matches_entry(writer, entry))
        })
    }
}

fn split_writers(ingress: &crowdb_chunk_kv::SplitIngress) -> Vec<Partition> {
    vec![ingress.retained_parent(), ingress.child()]
}

fn secondary_writer_for_seek(
    ingress: &crowdb_chunk_kv::SplitIngress,
    kind: SeekKind,
    key: &[u8],
) -> Option<Partition> {
    match kind {
        SeekKind::Ceiling | SeekKind::Higher if key < ingress.split_key() => Some(ingress.child()),
        SeekKind::Floor | SeekKind::Lower if key >= ingress.split_key() => Some(ingress.retained_parent()),
        _ => None,
    }
}

async fn execute_boundary_seek(
    ingress: &crowdb_chunk_kv::SplitIngress,
    writer: &Partition,
    kind: SeekKind,
    minimum: Option<JournalPosition>,
    max_scan_response_bytes: usize,
) -> Result<Option<crowdb_chunk_kv::ScanEntry>, ChunkKvError> {
    match kind {
        SeekKind::Ceiling | SeekKind::Higher => {
            writer
                .ceiling(writer.snapshot().ownership_epoch, ingress.split_key(), minimum)
                .await
        }
        SeekKind::Floor | SeekKind::Lower => {
            let mut page = writer
                .scan_reverse(
                    writer.snapshot().ownership_epoch,
                    Some(ingress.split_key()),
                    None,
                    1,
                    max_scan_response_bytes,
                    minimum,
                )
                .await?;
            Ok(page.entries.pop())
        }
    }
}

async fn execute_writer_seek(
    partition: &Partition,
    request: &SeekRequest,
    writer_epoch: u64,
    minimum: Option<JournalPosition>,
) -> Result<Option<crowdb_chunk_kv::ScanEntry>, ChunkKvError> {
    match request.kind {
        SeekKind::Ceiling => partition.ceiling(writer_epoch, &request.key, minimum).await,
        SeekKind::Higher => partition.higher(writer_epoch, &request.key, minimum).await,
        SeekKind::Floor => partition.floor(writer_epoch, &request.key, minimum).await,
        SeekKind::Lower => partition.lower(writer_epoch, &request.key, minimum).await,
    }
}

fn scan_page_bytes(page: &crowdb_chunk_kv::ScanPage) -> usize {
    page.entries.iter().fold(0usize, |total, entry| {
        total.saturating_add(entry.key.len().saturating_add(entry.value.value.len()))
    })
}

fn recoverable_local_entry(entry: &ChunkKvRangeCatalogEntry, instance_id: u64) -> bool {
    entry.owner.instance_id == instance_id
        && !matches!(
            entry.state,
            ChunkKvRangeCatalogPartitionState::Retired | ChunkKvRangeCatalogPartitionState::Faulted
        )
}

fn final_transfer_artifact(
    partition: &Partition,
    entry: &ChunkKvRangeCatalogEntry,
) -> Option<PreparedSplitWriterArtifact> {
    let overlay = entry.artifact.tail_overlay.as_ref()?;
    let mut artifact = partition.prepared_transfer_artifact()?;
    let exact_base = artifact.partition_id.high == entry.partition_id.high
        && artifact.partition_id.low == entry.partition_id.low
        && artifact.ownership_epoch == entry.owner_epoch
        && artifact.tree_id == entry.artifact.tree_id
        && artifact.tree_manifest == overlay.base_tree_manifest
        && artifact.root_manifest_generation == overlay.base_root_manifest_generation
        && artifact.base_applied_seq == overlay.base_applied_seq
        && artifact.parent_id.high == overlay.source_partition_id.high
        && artifact.parent_id.low == overlay.source_partition_id.low
        && artifact.parent_epoch == overlay.source_epoch
        && artifact.parent_stream_name == overlay.source_stream_name
        && artifact.parent_stream_generation == overlay.source_stream_generation
        && artifact.parent_replay_offset == overlay.replay_offset
        && overlay.target_stream_start_seq == overlay.cutover_seq.checked_add(1)?;
    if !exact_base {
        return None;
    }
    artifact.parent_cutover_offset = overlay.cutover_offset;
    artifact.applied_seq = overlay.cutover_seq;
    artifact.child_stream_start_seq = overlay.target_stream_start_seq;
    Some(artifact)
}

fn split_writer_matches_artifact(
    partition: &Partition,
    artifact: &crowdb_chunk_kv::PreparedSplitWriterArtifact,
) -> bool {
    let snapshot = partition.snapshot();
    snapshot.partition_id == artifact.partition_id
        && snapshot.ownership_epoch == artifact.ownership_epoch
        && snapshot.range == artifact.range
        && snapshot.stream_name == artifact.stream_name
}

fn matches_routing(
    routing: &RequestRouting,
    generation: u64,
    entry: &ChunkKvRangeCatalogEntry,
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

fn operation_requires_initialized_view(operation: &PointOperation) -> bool {
    matches!(
        operation,
        PointOperation::Get { .. }
            | PointOperation::PutIfAbsent { .. }
            | PointOperation::CompareExchange { .. }
            | PointOperation::ConditionalDelete { .. }
    )
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

fn scan_entry_value(entry: crowdb_chunk_kv::ScanEntry) -> RpcValue {
    RpcValue {
        key: entry.key.to_vec(),
        value: entry.value.value,
        revision: entry.value.revision,
    }
}

fn scan_success(
    map_revision: u64,
    request: &ScanRequest,
    page: crowdb_chunk_kv::ScanPage,
) -> ChunkKvResponse {
    let continuation = page
        .truncated
        .then(|| page.entries.last())
        .flatten()
        .map(|entry| ScanContinuation {
            direction: request.direction,
            last_key: entry.key.to_vec(),
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            map_revision: request.routing.map_revision,
        });
    success(
        map_revision,
        None,
        OperationResult::Scan {
            items: page.entries.into_iter().map(scan_entry_value).collect(),
            continuation,
        },
    )
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

fn target_not_ready(map_revision: u64) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision,
        journal_position: None,
        result: Err(RpcFailure {
            code: ChunkKvRpcErrorCode::TargetNotReady,
            message: "transfer target is replaying the sealed source suffix".into(),
            retry_after_ms: Some(10),
            latest_map_revision: Some(map_revision),
            owner_hint: None,
        }),
    }
}

fn not_my_range(map_revision: u64, entry: Option<&ChunkKvRangeCatalogEntry>) -> ChunkKvResponse {
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

fn not_my_range_failure(map_revision: u64, entry: Option<&ChunkKvRangeCatalogEntry>) -> RpcFailure {
    RpcFailure {
        code: ChunkKvRpcErrorCode::NotMyRange,
        message: "request routing does not match the active owner".into(),
        retry_after_ms: None,
        latest_map_revision: Some(map_revision),
        owner_hint: entry.map(|entry| OwnerHint {
            instance_id: entry.owner.instance_id,
            rpc_endpoint: entry.owner.rpc_endpoint.clone(),
            owner_epoch: entry.owner_epoch,
        }),
    }
}

fn rpc_failure(code: ChunkKvRpcErrorCode, message: &str) -> RpcFailure {
    RpcFailure {
        code,
        message: message.into(),
        retry_after_ms: None,
        latest_map_revision: None,
        owner_hint: None,
    }
}

fn authority_failure(
    map_revision: u64,
    error: &AuthorityError,
    entry: Option<&ChunkKvRangeCatalogEntry>,
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
    entry: Option<&ChunkKvRangeCatalogEntry>,
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
