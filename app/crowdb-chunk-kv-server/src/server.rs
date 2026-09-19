// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use crowdb_chunk_kv::{
    ChunkKvError, CompareCondition, JournalPosition, MutationOperation, MutationResult, Partition, RequestId,
    SplitArtifact, ValueRevision,
};
use crowdb_protocol::chunk_kv::{
    BatchMutationRequest, BatchMutationResponse, BatchMutationResult, ChunkKvRangeCatalogEntry,
    ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPartitionState, ChunkKvResponse,
    ChunkKvRpcErrorCode, Id128, KeyRange, MultiGetRequest, MultiGetResponse, OperationResult, OwnerHint,
    PointOperation, PointRequest, RequestRouting, RpcCompareCondition, RpcFailure, RpcJournalPosition,
    RpcValue, ScanContinuation, ScanDirection, ScanRequest, SeekKind, SeekRequest,
};
use crowdb_protocol::common::{ChunkKvExtra, ChunkKvPartitionLoad};
use thiserror::Error;
use tracing::info;

use crate::{
    validate_and_clip_scan, AuthorityError, ChunkKvRangeCatalogError, ClippedScan, ScanValidationError,
    ServerMetrics, ServingAuthority,
};

const DEFAULT_SCAN_RESPONSE_BYTES: usize = 17 * 1024 * 1024;

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
    started: Instant,
    instance_id: u64,
    authority: Arc<ServingAuthority>,
    catalog: ArcSwap<CatalogSnapshot>,
    partitions: ArcSwap<HashMap<Id128, Partition>>,
    local_split_sessions: ArcSwap<HashMap<Id128, LocalSplitSession>>,
    independently_recoverable: ArcSwap<HashSet<Id128>>,
    max_partitions: usize,
    max_scan_response_bytes: usize,
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

impl ChunkKvService {
    /// Creates an empty, fenced service instance.
    ///
    /// # Errors
    ///
    /// Returns an invalid-request error for zero identity or capacity.
    pub fn new(instance_id: u64, max_partitions: usize) -> Result<Self, ChunkKvError> {
        Self::new_with_limits(instance_id, max_partitions, DEFAULT_SCAN_RESPONSE_BYTES)
    }

    /// Creates an empty service with explicit hosting and scan response bounds.
    ///
    /// # Errors
    ///
    /// Returns an invalid-request error when any identity or bound is zero.
    pub fn new_with_limits(
        instance_id: u64,
        max_partitions: usize,
        max_scan_response_bytes: usize,
    ) -> Result<Self, ChunkKvError> {
        if instance_id == 0 || max_partitions == 0 || max_scan_response_bytes == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "server instance identity and capacity bounds must be nonzero".into(),
            ));
        }
        Ok(Self {
            started: Instant::now(),
            instance_id,
            authority: Arc::new(ServingAuthority::new(instance_id)),
            catalog: ArcSwap::from_pointee(CatalogSnapshot::default()),
            partitions: ArcSwap::from_pointee(HashMap::new()),
            local_split_sessions: ArcSwap::from_pointee(HashMap::new()),
            independently_recoverable: ArcSwap::from_pointee(HashSet::new()),
            max_partitions,
            max_scan_response_bytes,
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
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
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
        } else if catalog_generation != 0
            && (partitions.is_empty()
                || self
                    .authority
                    .has_live_grant(catalog_generation, now_monotonic_ms))
        {
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

    /// Builds the service-registry payload from immutable partition snapshots.
    #[must_use]
    pub fn registry_observation(&self, capacity_bytes: u64, request_rate: u64) -> ChunkKvExtra {
        let partitions = self.partitions.load_full();
        let durable_bytes = partitions
            .values()
            .filter_map(|partition| partition.chunk_storage_stats().ok().flatten())
            .map(|stats| stats.pack_bytes_written.saturating_sub(stats.orphan_bytes))
            .sum();
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
            capacity_bytes,
            durable_bytes,
            request_rate,
            hosted,
            partition_loads: Vec::new(),
        }
    }

    /// Builds a heartbeat observation and samples the largest serving
    /// partition with bounded memory for median split planning.
    ///
    /// # Errors
    ///
    /// Returns a partition read error when the sampled view cannot be scanned.
    pub async fn registry_observation_with_load_samples(
        &self,
        capacity_bytes: u64,
        request_rate: u64,
        max_samples: usize,
    ) -> Result<ChunkKvExtra, ChunkKvError> {
        let mut observation = self.registry_observation(capacity_bytes, request_rate);
        let partitions = self.partitions.load_full();
        let mut loads: Vec<_> = partitions
            .values()
            .map(|partition| {
                let snapshot = partition.snapshot();
                let durable_bytes = partition.chunk_storage_stats().ok().flatten().map_or(0, |stats| {
                    stats.pack_bytes_written.saturating_sub(stats.orphan_bytes)
                });
                (partition.clone(), snapshot, durable_bytes)
            })
            .collect();
        let largest = loads
            .iter()
            .enumerate()
            .filter(|(_, (_, snapshot, _))| {
                snapshot.lifecycle == crowdb_chunk_kv::PartitionLifecycle::Serving
            })
            .max_by_key(|(_, (_, _, durable_bytes))| *durable_bytes)
            .map(|(index, _)| index);
        for (index, (partition, snapshot, durable_bytes)) in loads.drain(..).enumerate() {
            let live_byte_samples = if Some(index) == largest && max_samples > 0 {
                sample_live_bytes(&partition, snapshot.ownership_epoch, max_samples).await?
            } else {
                Vec::new()
            };
            observation.partition_loads.push(ChunkKvPartitionLoad {
                partition_id: Id128 {
                    high: snapshot.partition_id.high,
                    low: snapshot.partition_id.low,
                },
                durable_bytes,
                live_byte_samples,
                independently_recoverable: self.independently_recoverable.load().contains(&Id128 {
                    high: snapshot.partition_id.high,
                    low: snapshot.partition_id.low,
                }) || self
                    .catalog
                    .load()
                    .entry_for_partition(Id128 {
                        high: snapshot.partition_id.high,
                        low: snapshot.partition_id.low,
                    })
                    .is_some_and(|entry| entry.artifact.tail_overlay.is_none()),
            });
        }
        observation
            .partition_loads
            .sort_unstable_by_key(|load| load.partition_id);
        Ok(observation)
    }

    /// Runs one bounded ownership-materialization pass for every local split child.
    ///
    /// Returns child identities whose own checkpoint no longer depends on the
    /// split-parent suffix.
    ///
    /// # Errors
    ///
    /// Returns the first materialization or checkpoint error.
    pub async fn materialize_split_overlays(&self) -> Result<Vec<Id128>, ChunkKvError> {
        let catalog = self.catalog.load_full();
        let partitions = self.partitions.load_full();
        let mut completed = Vec::new();
        for entry in &catalog.entries {
            if entry.owner.instance_id != self.instance_id
                || entry.state != ChunkKvRangeCatalogPartitionState::Serving
                || entry.artifact.tail_overlay.is_none()
                || self
                    .independently_recoverable
                    .load()
                    .contains(&entry.partition_id)
            {
                continue;
            }
            let Some(partition) = partitions.get(&entry.partition_id) else {
                continue;
            };
            if !partition
                .materialize_split_ownership(entry.owner_epoch)
                .await?
                .complete
            {
                continue;
            }
            let checkpoint = partition.checkpoint(entry.owner_epoch).await?;
            let Some(overlay) = entry.artifact.tail_overlay.as_ref() else {
                continue;
            };
            if checkpoint.applied_seq < overlay.cutover_seq {
                return Err(ChunkKvError::ApplyStateUnknown);
            }
            self.independently_recoverable.rcu(|current| {
                let mut next = (**current).clone();
                next.insert(entry.partition_id);
                Arc::new(next)
            });
            completed.push(entry.partition_id);
        }
        Ok(completed)
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
        self.partitions
            .load()
            .get(&entry.partition_id)
            .is_some_and(|partition| local_partition_for_entry(partition, entry).is_some())
            || self.local_split_writer(entry.partition_id).is_some()
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
            cutover_seq = artifact.cutover_seq,
            retained_base_seq = artifact.retained_parent.base_applied_seq,
            retained_applied_seq = artifact.retained_parent.applied_seq,
            retained_tree_manifest = artifact.retained_parent.tree_manifest,
            retained_root_manifest_generation = artifact.retained_parent.root_manifest_generation,
            retained_parent_stream_manifest_generation =
                artifact.retained_parent.parent_stream_manifest_generation,
            child_base_seq = artifact.child.base_applied_seq,
            child_applied_seq = artifact.child.applied_seq,
            child_tree_manifest = artifact.child.tree_manifest,
            child_root_manifest_generation = artifact.child.root_manifest_generation,
            child_parent_stream_manifest_generation = artifact.child.parent_stream_manifest_generation,
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

    /// Replaces the hosted snapshot with exactly the local, recoverable
    /// assignments from a validated catalog.
    ///
    /// Existing exact assignments retain their live handles. Every new or
    /// changed assignment must be supplied after replay in `recovered`.
    ///
    /// # Errors
    ///
    /// Returns an overload or invalid-assignment error without changing the
    /// hosted snapshot.
    pub fn reconcile_partitions(
        &self,
        pages: &[ChunkKvRangeCatalogPage],
        recovered: &[Partition],
    ) -> Result<(), ChunkKvError> {
        let next = self.reconciled_partition_snapshot(pages, recovered)?;
        self.partitions.store(next);
        Ok(())
    }

    /// Validates and installs one catalog together with its exact local
    /// partition snapshot.
    ///
    /// # Errors
    ///
    /// Returns a catalog or partition reconciliation error without changing
    /// either active snapshot.
    pub fn install_catalog_and_reconcile(
        &self,
        head: &ChunkKvRangeCatalogHead,
        pages: &[ChunkKvRangeCatalogPage],
        recovered: &[Partition],
    ) -> Result<(), ChunkKvRangeCatalogReconcileError> {
        let candidate = Arc::new(CatalogSnapshot::from_catalog(head, pages)?);
        let previous = self.catalog.load_full();
        if candidate.generation <= previous.generation {
            return Err(ChunkKvRangeCatalogError::GenerationConflict.into());
        }
        let next = self.reconciled_partition_snapshot(pages, recovered)?;
        let mut released_pins = Vec::new();
        for entry in &candidate.entries {
            if entry.artifact.tail_overlay.is_some() {
                continue;
            }
            let Some(prior) = previous.entry_for_partition(entry.partition_id) else {
                continue;
            };
            let (Some(_), Some(transition_id)) = (&prior.artifact.tail_overlay, prior.transition_id) else {
                continue;
            };
            if let Some(partition) = next.get(&entry.partition_id) {
                released_pins.push((partition.clone(), transition_id));
            }
        }
        self.activate_catalog(&candidate)?;
        let current = self.partitions.load_full();
        for (partition_id, partition) in current.iter() {
            if !next.contains_key(partition_id) {
                self.metrics.retire_partition(&partition.metrics().snapshot());
            }
        }
        self.partitions.store(next);
        for (partition, transition_id) in released_pins {
            partition.release_generation_pin(crowdb_chunk_kv::TransitionId {
                high: transition_id.high,
                low: transition_id.low,
            })?;
        }
        Ok(())
    }

    fn reconciled_partition_snapshot(
        &self,
        pages: &[ChunkKvRangeCatalogPage],
        recovered: &[Partition],
    ) -> Result<Arc<HashMap<Id128, Partition>>, ChunkKvError> {
        let current = self.partitions.load_full();
        let recovered: HashMap<_, _> = recovered
            .iter()
            .map(|partition| {
                let snapshot = partition.snapshot();
                (
                    Id128 {
                        high: snapshot.partition_id.high,
                        low: snapshot.partition_id.low,
                    },
                    partition.clone(),
                )
            })
            .collect();
        let desired: Vec<_> = pages
            .iter()
            .flat_map(|page| &page.entries)
            .filter(|entry| recoverable_local_entry(entry, self.instance_id))
            .collect();
        if desired.len() > self.max_partitions {
            return Err(ChunkKvError::Overloaded);
        }
        let mut next = HashMap::with_capacity(desired.len());
        for entry in desired {
            let partition = current
                .get(&entry.partition_id)
                .and_then(|partition| local_partition_for_entry(partition, entry))
                .or_else(|| self.local_split_writer(entry.partition_id))
                .or_else(|| {
                    recovered
                        .get(&entry.partition_id)
                        .filter(|partition| partition_matches_entry(partition, entry))
                        .cloned()
                })
                .ok_or_else(|| {
                    ChunkKvError::InvalidRequest(
                        "catalog assignment was not recovered before reconciliation".into(),
                    )
                })?;
            next.insert(entry.partition_id, partition.clone());
        }
        Ok(Arc::new(next))
    }

    pub fn remove_partition(&self, partition_id: Id128) {
        self.partitions.rcu(|current| {
            let mut next = (**current).clone();
            next.remove(&partition_id);
            Arc::new(next)
        });
    }

    pub(crate) fn hosted_partition(&self, partition_id: Id128) -> Option<Partition> {
        self.partitions.load().get(&partition_id).cloned()
    }

    pub(crate) fn split_transition_parent(&self, partition_id: Id128) -> Option<Partition> {
        self.hosted_partition(partition_id).or_else(|| {
            self.local_split_sessions
                .load()
                .get(&partition_id)
                .map(|session| session.dispatcher.clone())
        })
    }

    /// Activates one replayed assignment after a matching catalog and serving
    /// grant have been installed by the process lifecycle.
    ///
    /// # Errors
    ///
    /// Returns `OutOfRange` when the partition is not hosted, or the precise
    /// partition epoch/lifecycle error when activation is unsafe.
    pub fn activate_recovered_partition(
        &self,
        partition_id: Id128,
        owner_epoch: u64,
    ) -> Result<(), ChunkKvError> {
        let catalog = self.catalog.load();
        let entry = catalog
            .entry_for_partition(partition_id)
            .ok_or(ChunkKvError::OutOfRange)?;
        if entry.owner.instance_id != self.instance_id
            || entry.owner_epoch != owner_epoch
            || entry.state != ChunkKvRangeCatalogPartitionState::Serving
        {
            return Err(ChunkKvError::NotServing(
                "catalog does not publish this serving assignment".into(),
            ));
        }
        // A local split dispatcher owns both replacement writers.  Its old
        // parent identity remains a compatibility route for g1 requests, not
        // a partition that a later grant may reactivate at its obsolete epoch.
        if self.local_split_sessions.load().contains_key(&partition_id) {
            return Ok(());
        }
        self.partitions
            .load()
            .get(&partition_id)
            .ok_or(ChunkKvError::OutOfRange)?
            .activate_recovered(owner_epoch)
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
        if entry.state == ChunkKvRangeCatalogPartitionState::TargetCatchingUp {
            return target_not_ready(catalog.generation);
        }
        let partition = match self.resolve_point_partition(
            &request.routing,
            &key,
            catalog.generation,
            entry,
            now_monotonic_ms,
        ) {
            Ok(partition) => partition,
            Err(response) => return *response,
        };

        match request.operation {
            PointOperation::Get { key } => {
                let minimum = request.routing.min_journal_position.map(journal_position);
                match partition
                    .get(partition.snapshot().ownership_epoch, &key, minimum)
                    .await
                {
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
                    .mutate(partition.snapshot().ownership_epoch, request_id, mutation)
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

    /// Executes a fully range-validated partition-local multi-get.
    pub async fn handle_multi_get(
        &self,
        request: MultiGetRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> MultiGetResponse {
        let catalog = self.catalog.load_full();
        let Some(range) = self.logical_range_for_request(request.routing.partition_id, &catalog) else {
            return MultiGetResponse {
                map_revision: catalog.generation,
                result: Err(not_my_range_failure(catalog.generation, None)),
            };
        };
        if request.validate_for_range(&range).is_err() {
            return MultiGetResponse {
                map_revision: catalog.generation,
                result: Err(rpc_failure(
                    ChunkKvRpcErrorCode::InvalidRequest,
                    "multi-get group contains an out-of-range key",
                )),
            };
        }
        let mut values = Vec::with_capacity(request.keys.len());
        for key in request.keys {
            let response = self
                .handle_point(
                    PointRequest {
                        routing: request.routing.clone(),
                        operation: PointOperation::Get { key },
                    },
                    now_wall_ms,
                    now_monotonic_ms,
                )
                .await;
            match response.result {
                Ok(OperationResult::Value(value)) => values.push(value),
                Ok(_) => {
                    return MultiGetResponse {
                        map_revision: response.map_revision,
                        result: Err(rpc_failure(
                            ChunkKvRpcErrorCode::Internal,
                            "multi-get produced a non-value result",
                        )),
                    };
                }
                Err(error) => {
                    return MultiGetResponse {
                        map_revision: response.map_revision,
                        result: Err(error),
                    };
                }
            }
        }
        MultiGetResponse {
            map_revision: catalog.generation,
            result: Ok(values),
        }
    }

    /// Executes a fully range-validated mutation group in input order.
    pub async fn handle_batch_mutation(
        &self,
        request: BatchMutationRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> BatchMutationResponse {
        let catalog = self.catalog.load_full();
        let Some(range) = self.logical_range_for_request(request.routing.partition_id, &catalog) else {
            return BatchMutationResponse {
                map_revision: catalog.generation,
                result: Err(not_my_range_failure(catalog.generation, None)),
            };
        };
        if request.validate_for_range(&range).is_err() {
            return BatchMutationResponse {
                map_revision: catalog.generation,
                result: Err(rpc_failure(
                    ChunkKvRpcErrorCode::InvalidRequest,
                    "batch group contains an invalid or out-of-range mutation",
                )),
            };
        }
        let first_id = request.operations[0].request_id;
        let routing = RequestRouting {
            request_id: first_id,
            map_revision: request.routing.map_revision,
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            min_journal_position: None,
            deadline_ms: request.routing.deadline_ms,
        };
        let mut results = Vec::with_capacity(request.operations.len());
        for item in request.operations {
            let response = self
                .handle_point(
                    PointRequest {
                        routing: RequestRouting {
                            request_id: item.request_id,
                            ..routing.clone()
                        },
                        operation: item.operation,
                    },
                    now_wall_ms,
                    now_monotonic_ms,
                )
                .await;
            results.push(BatchMutationResult {
                request_id: item.request_id,
                journal_position: response.journal_position,
                result: response.result,
            });
        }
        BatchMutationResponse {
            map_revision: catalog.generation,
            result: Ok(results),
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
            .handle_scan_inner(request, now_wall_ms, now_monotonic_ms)
            .await;
        self.metrics.response(&response);
        response
    }

    async fn handle_scan_inner(
        &self,
        request: ScanRequest,
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
        let logical_range = self
            .logical_range_for_request(request.routing.partition_id, &catalog)
            .unwrap_or_else(|| entry.range.clone());
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
        let Some(partition) = self.partition_for_request(&request.routing, &clipped.start, entry) else {
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
        match self.execute_scan(&partition, &request, &clipped).await {
            Ok(page) => scan_success(catalog.generation, &request, page),
            Err(error) => partition_failure(catalog.generation, &error, Some(entry)),
        }
    }

    async fn execute_scan(
        &self,
        partition: &Partition,
        request: &ScanRequest,
        clipped: &ClippedScan,
    ) -> Result<crowdb_chunk_kv::ScanPage, ChunkKvError> {
        if let Some(ingress) = partition.split_ingress() {
            return self.execute_split_scan(&ingress, request, clipped).await;
        }
        self.execute_partition_scan(
            partition,
            request,
            clipped,
            clipped.limit as usize,
            self.max_scan_response_bytes,
        )
        .await
    }

    async fn execute_split_scan(
        &self,
        ingress: &crowdb_chunk_kv::SplitIngress,
        request: &ScanRequest,
        clipped: &ClippedScan,
    ) -> Result<crowdb_chunk_kv::ScanPage, ChunkKvError> {
        let writers = match clipped.direction {
            ScanDirection::Forward => [ingress.retained_parent(), ingress.child()],
            ScanDirection::Reverse => [ingress.child(), ingress.retained_parent()],
        };
        let mut entries = Vec::new();
        let mut remaining_bytes = self.max_scan_response_bytes;
        for (index, writer) in writers.iter().enumerate() {
            let remaining = (clipped.limit as usize).saturating_sub(entries.len());
            if remaining == 0 || remaining_bytes == 0 {
                return Ok(crowdb_chunk_kv::ScanPage {
                    entries,
                    truncated: true,
                });
            }
            let page = self
                .execute_partition_scan(writer, request, clipped, remaining, remaining_bytes)
                .await?;
            remaining_bytes = remaining_bytes.saturating_sub(scan_page_bytes(&page));
            entries.extend(page.entries);
            if page.truncated {
                return Ok(crowdb_chunk_kv::ScanPage {
                    entries,
                    truncated: true,
                });
            }
            if entries.len() == clipped.limit as usize && index + 1 < writers.len() {
                return Ok(crowdb_chunk_kv::ScanPage {
                    entries,
                    truncated: true,
                });
            }
        }
        Ok(crowdb_chunk_kv::ScanPage {
            entries,
            truncated: false,
        })
    }

    async fn execute_partition_scan(
        &self,
        partition: &Partition,
        request: &ScanRequest,
        clipped: &ClippedScan,
        limit: usize,
        byte_budget: usize,
    ) -> Result<crowdb_chunk_kv::ScanPage, ChunkKvError> {
        let minimum = request.routing.min_journal_position.map(journal_position);
        let writer_epoch = partition.snapshot().ownership_epoch;
        match clipped.direction {
            ScanDirection::Forward => {
                if let Some(resume_after) = clipped.resume_after.as_deref() {
                    partition
                        .scan_forward_after(
                            writer_epoch,
                            resume_after,
                            clipped.end.as_deref(),
                            limit,
                            byte_budget,
                            minimum,
                        )
                        .await
                } else {
                    partition
                        .scan_forward(
                            writer_epoch,
                            Some(&clipped.start),
                            clipped.end.as_deref(),
                            limit,
                            byte_budget,
                            minimum,
                        )
                        .await
                }
            }
            ScanDirection::Reverse => {
                let start_before = clipped.resume_after.as_deref().or(clipped.end.as_deref());
                partition
                    .scan_reverse(
                        writer_epoch,
                        start_before,
                        Some(&clipped.start),
                        limit,
                        byte_budget,
                        minimum,
                    )
                    .await
            }
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

    fn partition_for_request(
        &self,
        routing: &RequestRouting,
        key: &[u8],
        entry: &ChunkKvRangeCatalogEntry,
    ) -> Option<Partition> {
        self.local_split_sessions
            .load()
            .get(&routing.partition_id)
            .map(|session| session.dispatcher.clone())
            .filter(|dispatcher| dispatcher.snapshot().range.contains(key))
            .or_else(|| self.partition_for_catalog_entry(entry))
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

    fn local_split_writer(&self, partition_id: Id128) -> Option<Partition> {
        self.local_split_sessions.load().values().find_map(|session| {
            let dispatcher = &session.dispatcher;
            let ingress = dispatcher.split_ingress()?;
            [ingress.retained_parent(), ingress.child()]
                .into_iter()
                .find(|writer| {
                    let snapshot = writer.snapshot();
                    snapshot.partition_id.high == partition_id.high
                        && snapshot.partition_id.low == partition_id.low
                })
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

async fn sample_live_bytes(
    partition: &Partition,
    ownership_epoch: u64,
    max_samples: usize,
) -> Result<Vec<(Vec<u8>, u64)>, ChunkKvError> {
    const PAGE_ENTRIES: usize = 256;
    const PAGE_BYTES: usize = 4 * 1024 * 1024;

    let mut samples = Vec::with_capacity(max_samples.saturating_add(1));
    let mut start_after: Option<Vec<u8>> = None;
    loop {
        let page = match start_after.as_deref() {
            Some(key) => {
                partition
                    .scan_forward_after(ownership_epoch, key, None, PAGE_ENTRIES, PAGE_BYTES, None)
                    .await?
            }
            None => {
                partition
                    .scan_forward(ownership_epoch, None, None, PAGE_ENTRIES, PAGE_BYTES, None)
                    .await?
            }
        };
        if page.entries.is_empty() {
            break;
        }
        for entry in &page.entries {
            let bytes =
                u64::try_from(entry.key.len().saturating_add(entry.value.value.len())).unwrap_or(u64::MAX);
            samples.push((entry.key.to_vec(), bytes));
        }
        compact_live_byte_samples(&mut samples, max_samples);
        start_after = page.entries.last().map(|entry| entry.key.to_vec());
        if !page.truncated {
            break;
        }
    }
    Ok(samples)
}

fn compact_live_byte_samples(samples: &mut Vec<(Vec<u8>, u64)>, max_samples: usize) {
    while samples.len() > max_samples {
        let mut compacted = Vec::with_capacity(samples.len().div_ceil(2));
        for pair in samples.chunks(2) {
            if let [left, right] = pair {
                let total = left.1.saturating_add(right.1);
                let key = if left.1.saturating_mul(2) >= total {
                    left.0.clone()
                } else {
                    right.0.clone()
                };
                compacted.push((key, total));
            } else {
                compacted.push(pair[0].clone());
            }
        }
        *samples = compacted;
    }
}

fn recoverable_local_entry(entry: &ChunkKvRangeCatalogEntry, instance_id: u64) -> bool {
    entry.owner.instance_id == instance_id
        && !matches!(
            entry.state,
            ChunkKvRangeCatalogPartitionState::Retired | ChunkKvRangeCatalogPartitionState::Faulted
        )
}

fn partition_matches_entry(partition: &Partition, entry: &ChunkKvRangeCatalogEntry) -> bool {
    let snapshot = partition.snapshot();
    snapshot.partition_id.high == entry.partition_id.high
        && snapshot.partition_id.low == entry.partition_id.low
        && snapshot.ownership_epoch == entry.owner_epoch
        && snapshot.range.start.as_deref() == Some(entry.range.start.as_slice())
        && snapshot.range.end == entry.range.end
        && snapshot.stream_name == entry.artifact.stream_name
}

fn local_partition_for_entry(partition: &Partition, entry: &ChunkKvRangeCatalogEntry) -> Option<Partition> {
    if partition_matches_entry(partition, entry) {
        return Some(partition.clone());
    }
    let ingress = partition.split_ingress()?;
    [ingress.retained_parent(), ingress.child()]
        .into_iter()
        .find(|writer| partition_matches_entry(writer, entry))
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
