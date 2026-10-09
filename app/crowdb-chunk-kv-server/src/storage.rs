// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production chunk-stream dependency assembly.

use std::sync::Arc;

use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, SmallWritePolicy};
use crowdb_chunk_kv::{
    MutationOperation, Partition, PartitionConfig, PartitionId, PartitionJournal, PartitionRange,
    PartitionTree, PreparedSplit, PreparedSplitWriterArtifact, RequestId, SplitArtifact, SplitChild,
    SplitPlan, SplitWriterTarget, StreamPartitionJournal, TransitionId,
};
use crowdb_chunk_stream::{ChunkStream, ProductionStreamRuntime, StreamConfig, StreamName, StreamRegistry};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, PartitionArtifact, SplitChildAssignment, SplitTransition, TailOverlayArtifact,
    TransferTransition,
};
use crowdb_protocol::chunk_stream::{StreamBinding, StreamBindingState};
use crowdb_tree_ffi::{
    ChunkPageStoreOptions, ChunkRootCatalog, ChunkTransport, OwnedChunkRpcTransportOptions, PageStore,
};
use thiserror::Error;

use crate::{BootstrapPartitionConfig, ChunkKvServerConfig};

mod root_catalog;
mod split_handoff;
mod split_recovery;
use root_catalog::KvRootCatalogStore;

#[derive(Debug, Error)]
pub enum StorageRuntimeError {
    #[error("failed to connect chunk IO: {0}")]
    ChunkIo(String),
    #[error("failed to configure chunk stream: {0}")]
    Stream(String),
    #[error("failed to configure native tree storage: {0}")]
    Tree(String),
    #[error("failed to recover chunk KV partition: {0}")]
    Partition(String),
}

/// Process-wide production clients shared by every hosted partition stream.
pub struct ChunkKvStorage {
    kv: Arc<CrowdbKvClient>,
    chunk_io: ChunkIoClient,
    streams: Arc<ProductionStreamRuntime>,
    tree_transport: Arc<ChunkTransport>,
    tree_mirror_copies: u32,
    tree_chunk_capacity_bytes: u64,
    metadata_store_id: u64,
}

impl ChunkKvStorage {
    /// Discovers KV, `ChunkDB`, and `DiskIO` endpoints and creates the shared
    /// stream runtime used by hosted partitions.
    ///
    /// # Errors
    ///
    /// Returns an endpoint discovery or storage configuration error.
    pub async fn connect(config: &ChunkKvServerConfig) -> Result<Self, StorageRuntimeError> {
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
            config.group0_mgmt_seeds.clone(),
        )));
        let chunk_io = ChunkIoClient::connect_with_kv(
            ChunkIoClientConfig {
                management_seeds: config.group0_mgmt_seeds.clone(),
                diskio_connections_per_endpoint: config.storage.diskio_connections_per_endpoint,
                diskio_rpc_workers: config.storage.diskio_rpc_workers,
                small_write: SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::Wal),
            },
            Arc::clone(&kv),
        )
        .await
        .map_err(|error| StorageRuntimeError::ChunkIo(error.to_string()))?;
        Self::from_parts_with_mirror_copies(
            kv,
            chunk_io,
            config.storage.metadata_store_id,
            config.storage.stream_writer_lease_ms,
            config.storage.stream_mirror_copies,
            config.storage.tree_chunk_capacity_bytes,
            StreamConfig {
                chunk_capacity_bytes: config.storage.stream_chunk_capacity_bytes,
                extent_page_entries: config.storage.stream_extent_page_entries,
                ..StreamConfig::default()
            },
        )
        .await
    }

    async fn from_parts_with_mirror_copies(
        kv: Arc<CrowdbKvClient>,
        chunk_io: ChunkIoClient,
        metadata_store_id: u64,
        writer_lease_ms: u64,
        stream_mirror_copies: u32,
        tree_chunk_capacity_bytes: u64,
        stream_config: StreamConfig,
    ) -> Result<Self, StorageRuntimeError> {
        let streams = Arc::new(
            ProductionStreamRuntime::new_with_mirror_copies(
                Arc::clone(&kv),
                &chunk_io,
                writer_lease_ms,
                ChunkReadPolicy::default(),
                stream_config,
                stream_mirror_copies,
            )
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))?,
        );
        Self::assemble(
            kv,
            chunk_io,
            streams,
            metadata_store_id,
            writer_lease_ms,
            stream_mirror_copies,
            tree_chunk_capacity_bytes,
        )
        .await
    }

    /// Assembles production adapters from already connected process clients.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid stream lease or read policy.
    pub async fn from_parts(
        kv: Arc<CrowdbKvClient>,
        chunk_io: ChunkIoClient,
        metadata_store_id: u64,
        writer_lease_ms: u64,
        stream_mirror_copies: u32,
    ) -> Result<Self, StorageRuntimeError> {
        Self::from_parts_with_mirror_copies(
            kv,
            chunk_io,
            metadata_store_id,
            writer_lease_ms,
            stream_mirror_copies,
            256 * 1024 * 1024,
            StreamConfig::default(),
        )
        .await
    }

    async fn assemble(
        kv: Arc<CrowdbKvClient>,
        chunk_io: ChunkIoClient,
        streams: Arc<ProductionStreamRuntime>,
        metadata_store_id: u64,
        writer_lease_ms: u64,
        mirror_copies: u32,
        tree_chunk_capacity_bytes: u64,
    ) -> Result<Self, StorageRuntimeError> {
        let (chunkdb, disks) = chunk_io
            .native_storage_routes(writer_lease_ms)
            .await
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?;
        let tree_transport = Arc::new(
            ChunkTransport::open_owned_rpc(OwnedChunkRpcTransportOptions {
                chunkdb: Arc::new(move |id, refresh| {
                    chunkdb.resolve(
                        id.map(|(high, low)| crowdb_protocol::common::ChunkId { high, low }),
                        refresh,
                    )
                }),
                disks: Some(Arc::new(move |id, _| {
                    id.and_then(|(high, low)| disks.resolve(high, low))
                })),
                disk_routes: Vec::new(),
                writer_lease_ms,
                rpc_timeout_ms: writer_lease_ms,
                completion_capacity: 1_024,
                mirror_copies,
            })
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?,
        );
        Ok(Self {
            kv,
            chunk_io,
            streams,
            tree_transport,
            tree_mirror_copies: mirror_copies,
            tree_chunk_capacity_bytes,
            metadata_store_id,
        })
    }

    #[must_use]
    pub fn kv(&self) -> &Arc<CrowdbKvClient> {
        &self.kv
    }

    #[must_use]
    pub fn chunk_io(&self) -> &ChunkIoClient {
        &self.chunk_io
    }

    #[must_use]
    pub fn streams(&self) -> &Arc<ProductionStreamRuntime> {
        &self.streams
    }

    /// Open the R140 page store with the process-owned native RPC transport.
    /// The supplied catalog determines durable generation publication and
    /// ownership fencing for this tree identity.
    ///
    /// # Errors
    ///
    /// Returns an invalid native page-store or transport error.
    pub fn open_tree_page_store(
        &self,
        mut options: ChunkPageStoreOptions,
        catalog: Arc<ChunkRootCatalog>,
    ) -> Result<Arc<PageStore>, StorageRuntimeError> {
        options.mirror_copies = self.tree_mirror_copies;
        options.max_chunk_bytes = self.tree_chunk_capacity_bytes;
        PageStore::open_chunk(options, catalog, Some(&self.tree_transport))
            .map(Arc::new)
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))
    }

    /// Claims one tree root at `owner_epoch` and opens its durable,
    /// generation-CAS page store in the supplied metadata group.
    ///
    /// # Errors
    ///
    /// Returns a stale authority, KV availability, or native page-store error.
    pub async fn open_durable_tree_page_store(
        &self,
        options: ChunkPageStoreOptions,
        metadata_group_id: u64,
    ) -> Result<Arc<PageStore>, StorageRuntimeError> {
        let catalog = KvRootCatalogStore::claim(
            Arc::clone(&self.kv),
            self.metadata_store_id,
            metadata_group_id,
            options.tree_id,
            options.owner_epoch,
        )
        .await?;
        self.open_tree_page_store(
            options,
            Arc::new(
                ChunkRootCatalog::open_callback(Arc::new(catalog))
                    .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?,
            ),
        )
    }

    /// Initializes the stream selected by an Active catalog binding.
    ///
    /// # Errors
    ///
    /// Returns a registry, metadata, chunk IO, or fencing error.
    pub async fn create_registered_stream(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
    ) -> Result<ChunkStream, StorageRuntimeError> {
        self.streams
            .create_registered(stream_name, self.metadata_store_id, writer_epoch)
            .await
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))
    }

    /// Reopens one assigned stream under the supplied ownership epoch.
    ///
    /// # Errors
    ///
    /// Returns a registry, metadata, chunk IO, or fencing error.
    pub async fn open_stream(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
    ) -> Result<ChunkStream, StorageRuntimeError> {
        self.streams
            .open(stream_name, self.metadata_store_id, writer_epoch)
            .await
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))
    }

    /// Reopens an assigned tree root and WAL, replays through the durable tail,
    /// and returns a partition in `Prepared` state.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream binding is absent, either durable
    /// artifact cannot be reopened exactly, or WAL replay fails.
    pub async fn recover_partition(
        &self,
        entry: &ChunkKvRangeCatalogEntry,
    ) -> Result<Partition, StorageRuntimeError> {
        let stream_name = entry.artifact.stream_name;
        let binding = self
            .streams
            .registry()
            .load(stream_name)
            .await
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))?
            .ok_or_else(|| StorageRuntimeError::Stream("assigned stream binding does not exist".into()))?;
        let stream = self
            .open_stream(stream_name, entry.owner_epoch)
            .await
            .map_err(|error| {
                StorageRuntimeError::Stream(format!(
                    "target stream {stream_name:?} at owner epoch {}: {error}",
                    entry.owner_epoch
                ))
            })?;
        let open_generation = entry
            .artifact
            .tail_overlay
            .as_ref()
            .map_or(0, |overlay| overlay.base_root_manifest_generation);
        let options = ChunkPageStoreOptions {
            tree_id: entry.artifact.tree_id,
            owner_epoch: entry.owner_epoch,
            open_generation,
            pack_bytes: 0,
            iu_size: 0,
            max_concurrent_packs: 0,
            materialization_bytes_per_pass: 0,
            mirror_copies: 0,
            max_chunk_bytes: 0,
        };
        let catalog = KvRootCatalogStore::for_assignment(
            self.kv.clone(),
            self.metadata_store_id,
            binding.metadata_group_id,
            entry,
        )
        .await?;
        let page_store = self.open_tree_page_store(
            options,
            Arc::new(
                ChunkRootCatalog::open_callback(Arc::new(catalog))
                    .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?,
            ),
        )?;
        if let Some(overlay) = &entry.artifact.tail_overlay {
            let parent_stream = self
                .streams
                .open_read_only_current(overlay.source_stream_name, self.metadata_store_id)
                .await
                .map_err(|error| {
                    StorageRuntimeError::Stream(format!(
                        "overlay source stream {:?} at source epoch {}: {error}",
                        overlay.source_stream_name, overlay.source_epoch
                    ))
                })?;
            let artifact = prepared_overlay_artifact(entry, overlay);
            return Partition::recover_native_prepared_overlay(
                artifact,
                PartitionConfig::default(),
                crowdb_tree_ffi::Config::default(),
                page_store,
                stream,
                parent_stream,
            )
            .await
            .map_err(|error| overlay_recovery_error(entry, overlay, &error));
        }
        Partition::recover_native_latest_prepared_assignment(
            PartitionId {
                high: entry.partition_id.high,
                low: entry.partition_id.low,
            },
            PartitionRange {
                start: Some(entry.range.start.clone()),
                end: entry.range.end.clone(),
            },
            entry.owner_epoch,
            entry.artifact.tree_id,
            PartitionConfig::default(),
            crowdb_tree_ffi::Config::default(),
            page_store,
            stream,
        )
        .await
        .map_err(|error| assignment_recovery_error(entry, &error))
    }

    /// Advances one already-open transfer target from its preparation cursor
    /// to the final source release cursor without reopening the base tree.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing overlay/source stream or incremental
    /// replay failure.
    pub async fn catch_up_transfer_target(
        &self,
        target: &Partition,
        entry: &ChunkKvRangeCatalogEntry,
    ) -> Result<(), crate::MonitorError> {
        KvRootCatalogStore::prepare(
            self.kv.clone(),
            self.metadata_store_id,
            self.streams
                .registry()
                .load(entry.artifact.stream_name)
                .await
                .map_err(|error| storage_plan_error(&error.to_string()))?
                .ok_or_else(|| storage_plan_error("target stream binding is absent"))?
                .metadata_group_id,
            entry,
        )
        .map_err(|error| storage_plan_error(&error.to_string()))?
        .claim_published(true)
        .await
        .map_err(|error| storage_plan_error(&error.to_string()))?;
        let overlay = entry
            .artifact
            .tail_overlay
            .as_ref()
            .ok_or_else(|| storage_plan_error("transfer target overlay is absent"))?;
        let parent_stream = self
            .streams
            .open_read_only_current(overlay.source_stream_name, self.metadata_store_id)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        let parent_journal: Arc<dyn PartitionJournal> = Arc::new(
            StreamPartitionJournal::new(parent_stream, overlay.source_stream_name)
                .map_err(|error| storage_plan_error(&error.to_string()))?,
        );
        target
            .catch_up_prepared_transfer(prepared_overlay_artifact(entry, overlay), parent_journal)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))
    }

    /// Creates the explicitly configured initial full-range partition and
    /// publishes its first tree checkpoint before catalog visibility.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting durable identities or any stream,
    /// tree, checkpoint, or replay failure.
    pub async fn bootstrap_partition(
        &self,
        config: &BootstrapPartitionConfig,
    ) -> Result<Partition, StorageRuntimeError> {
        let binding = StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Wal,
            stream_name: config.stream_name,
            metadata_group_id: config.metadata_group_id,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        };
        let existing = self
            .streams
            .registry()
            .load(config.stream_name)
            .await
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))?;
        let fresh = match existing {
            Some(observed) if observed != binding => {
                return Err(StorageRuntimeError::Stream(
                    "bootstrap stream binding conflicts".into(),
                ));
            }
            Some(_) => false,
            None => {
                self.streams
                    .registry()
                    .create(binding)
                    .await
                    .map_err(|error| StorageRuntimeError::Stream(error.to_string()))?;
                true
            }
        };
        let stream = if fresh {
            self.create_registered_stream(config.stream_name, config.owner_epoch)
                .await?
        } else {
            self.open_stream(config.stream_name, config.owner_epoch).await?
        };
        let page_store = self
            .open_durable_tree_page_store(
                ChunkPageStoreOptions {
                    tree_id: config.tree_id,
                    owner_epoch: config.owner_epoch,
                    open_generation: 0,
                    pack_bytes: 0,
                    iu_size: 0,
                    max_concurrent_packs: 0,
                    materialization_bytes_per_pass: 0,
                    mirror_copies: 0,
                    max_chunk_bytes: 0,
                },
                config.metadata_group_id,
            )
            .await?;
        let partition_id = PartitionId {
            high: config.partition_id.high,
            low: config.partition_id.low,
        };
        let range = PartitionRange {
            start: Some(Vec::new()),
            end: None,
        };
        if !fresh {
            return Partition::recover_native_latest_prepared_assignment(
                partition_id,
                range,
                config.owner_epoch,
                config.tree_id,
                PartitionConfig::default(),
                crowdb_tree_ffi::Config::default(),
                page_store,
                stream,
            )
            .await
            .map_err(|error| StorageRuntimeError::Partition(error.to_string()));
        }
        let partition = Partition::open_native(
            partition_id,
            range,
            config.owner_epoch,
            PartitionConfig::default(),
            config.tree_id,
            crowdb_tree_ffi::Config::default(),
            page_store,
            stream,
        )
        .map_err(|error| StorageRuntimeError::Partition(format!("bootstrap tree open: {error}")))?;
        initialize_bootstrap_root(&partition, config).await?;
        Ok(partition)
    }

    /// Rebuilds one durable split child from the retained authoritative parent.
    ///
    /// A live retry reuses its exact pending base and WAL, including when its
    /// handoff is now confirmed. Cold recovery reopens the recorded artifacts.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting stream bindings, stale tree authority,
    /// invalid transition identity, or an incomplete R142 split preparation.
    pub async fn prepare_split(
        &self,
        parent: &Partition,
        transition: &SplitTransition,
        max_catchup_lag_records: u64,
    ) -> Result<PreparedSplit, crate::MonitorError> {
        let cached_child = parent
            .pending_split_child_target(&split_plan(transition))
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        if transition.handoff_proof.is_some() && cached_child.is_none() {
            return self.resume_split_handoff(parent, transition).await;
        }
        let store = Arc::new(crate::Group0ControlStore::from_client(self.kv.clone()));
        let (current, _) = store
            .load_split_transition(transition.transition_id)
            .await?
            .ok_or_else(|| storage_plan_error("split transition disappeared before preparation"))?;
        if &current != transition {
            return Err(storage_plan_error("split changed before preparation retry"));
        }
        let child = if let Some(child) = cached_child {
            child
        } else {
            parent
                .release_generation_pin(split_plan(transition).transition_id)
                .map_err(|error| storage_plan_error(&error.to_string()))?;
            let parent_binding = self
                .streams
                .registry()
                .load(transition.parent_artifact.stream_name)
                .await
                .map_err(|error| storage_plan_error(&error.to_string()))?
                .ok_or_else(|| storage_plan_error("split parent stream binding does not exist"))?;
            let child = self
                .split_target(&transition.child, parent_binding.metadata_group_id)
                .await?;
            if child.journal.tail() != 0 {
                return Err(storage_plan_error(
                    "nonempty child WAL has no committed handoff proof",
                ));
            }
            crowdb_chunk_kv::CrowdbPartitionTree::open(child.tree_id, &child.tree_config)
                .map_err(|error| storage_plan_error(&error.to_string()))?
                .unpin_generation(split_plan(transition).transition_id)
                .map_err(|error| storage_plan_error(&error.to_string()))?;
            child
        };
        let handoff = Arc::new(split_handoff::SplitHandoffCommit {
            store,
            expected: transition.clone(),
        });
        let prepared = parent
            .prepare_split_child_session(split_plan(transition), child, max_catchup_lag_records, handoff)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        validate_prepared_split(transition, &prepared.artifact)?;
        Ok(prepared)
    }

    /// Pins a live source checkpoint and creates the target-owned empty WAL.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing source binding, checkpoint failure, or target stream conflict.
    pub async fn prepare_transfer_source(
        &self,
        source: &Partition,
        transition: &TransferTransition,
    ) -> Result<PartitionArtifact, crate::MonitorError> {
        source
            .release_generation_pin(crowdb_chunk_kv::TransitionId {
                high: transition.transition_id.high,
                low: transition.transition_id.low,
            })
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        let binding = self
            .streams
            .registry()
            .load(transition.artifact.stream_name)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?
            .ok_or_else(|| storage_plan_error("transfer source stream binding does not exist"))?;
        self.open_or_create_stream(
            transition.target_artifact.stream_name,
            transition.target_epoch,
            binding.metadata_group_id,
            true,
        )
        .await?;
        let checkpoint = source
            .checkpoint(transition.source_epoch)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        source
            .retain_generation_pin(
                crowdb_chunk_kv::TransitionId {
                    high: transition.transition_id.high,
                    low: transition.transition_id.low,
                },
                checkpoint.root_manifest_generation,
            )
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        let snapshot = source.snapshot();
        let mut artifact = transition.target_artifact.clone();
        artifact.tail_overlay = Some(TailOverlayArtifact {
            source_partition_id: transition.partition_id,
            source_epoch: transition.source_epoch,
            source_stream_name: transition.artifact.stream_name,
            source_stream_manifest_generation: checkpoint.stream_manifest_generation,
            replay_offset: checkpoint.replay_offset,
            cutover_offset: snapshot.journal_durable_offset,
            base_tree_manifest: checkpoint.tree_manifest,
            base_root_manifest_generation: checkpoint.root_manifest_generation,
            base_applied_seq: checkpoint.applied_seq,
            cutover_seq: snapshot.journal_durable_seq,
            target_stream_start_seq: snapshot.journal_durable_seq.saturating_add(1),
        });
        Ok(artifact)
    }

    async fn split_target(
        &self,
        child: &SplitChildAssignment,
        metadata_group_id: u64,
    ) -> Result<SplitWriterTarget, crate::MonitorError> {
        self.split_target_artifact(&child.artifact, child.owner_epoch, metadata_group_id)
            .await
    }

    async fn split_target_artifact(
        &self,
        artifact: &PartitionArtifact,
        owner_epoch: u64,
        metadata_group_id: u64,
    ) -> Result<SplitWriterTarget, crate::MonitorError> {
        let stream = self
            .open_or_create_stream(artifact.stream_name, owner_epoch, metadata_group_id, false)
            .await?;
        let page_store = self
            .open_durable_tree_page_store(
                ChunkPageStoreOptions {
                    tree_id: artifact.tree_id,
                    owner_epoch,
                    open_generation: 0,
                    pack_bytes: 0,
                    iu_size: 0,
                    max_concurrent_packs: 0,
                    materialization_bytes_per_pass: 0,
                    mirror_copies: 0,
                    max_chunk_bytes: 0,
                },
                metadata_group_id,
            )
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        Ok(SplitWriterTarget {
            tree_id: artifact.tree_id,
            tree_config: crowdb_tree_ffi::Config {
                page_store: Some(page_store),
                ..crowdb_tree_ffi::Config::default()
            },
            journal: Arc::new(
                StreamPartitionJournal::new(stream, artifact.stream_name)
                    .map_err(|error| storage_plan_error(&error.to_string()))?,
            ),
        })
    }

    async fn open_or_create_stream(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        metadata_group_id: u64,
        require_empty: bool,
    ) -> Result<ChunkStream, crate::MonitorError> {
        let registry = self.streams.registry();
        let expected = StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Wal,
            stream_name,
            metadata_group_id,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        };
        match registry
            .load(stream_name)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?
        {
            Some(observed) if observed != expected => {
                return Err(storage_plan_error("split child stream binding conflicts"));
            }
            Some(_) => {}
            None => {
                if let Err(error) = registry.create(expected.clone()).await {
                    let reconciled = registry
                        .load(stream_name)
                        .await
                        .map_err(|error| storage_plan_error(&error.to_string()))?;
                    if reconciled.as_ref() != Some(&expected) {
                        return Err(storage_plan_error(&error.to_string()));
                    }
                }
            }
        }
        let stream = match self.create_registered_stream(stream_name, writer_epoch).await {
            Ok(stream) => stream,
            Err(_) => self
                .open_stream(stream_name, writer_epoch)
                .await
                .map_err(|error| storage_plan_error(&error.to_string()))?,
        };
        if require_empty && stream.tail() != 0 {
            return Err(storage_plan_error("target WAL is not empty"));
        }
        Ok(stream)
    }
}

fn prepared_overlay_artifact(
    entry: &ChunkKvRangeCatalogEntry,
    overlay: &TailOverlayArtifact,
) -> PreparedSplitWriterArtifact {
    PreparedSplitWriterArtifact {
        partition_id: PartitionId {
            high: entry.partition_id.high,
            low: entry.partition_id.low,
        },
        range: PartitionRange {
            start: Some(entry.range.start.clone()),
            end: entry.range.end.clone(),
        },
        ownership_epoch: entry.owner_epoch,
        tree_id: entry.artifact.tree_id,
        tree_manifest: overlay.base_tree_manifest,
        root_manifest_generation: overlay.base_root_manifest_generation,
        stream_name: entry.artifact.stream_name,
        base_applied_seq: overlay.base_applied_seq,
        parent_id: PartitionId {
            high: overlay.source_partition_id.high,
            low: overlay.source_partition_id.low,
        },
        parent_epoch: overlay.source_epoch,
        parent_stream_name: overlay.source_stream_name,
        parent_stream_manifest_generation: overlay.source_stream_manifest_generation,
        parent_replay_offset: overlay.replay_offset,
        parent_cutover_offset: overlay.cutover_offset,
        applied_seq: overlay.cutover_seq,
        child_stream_start_seq: overlay.target_stream_start_seq,
    }
}

async fn initialize_bootstrap_root(
    partition: &Partition,
    config: &BootstrapPartitionConfig,
) -> Result<(), StorageRuntimeError> {
    // An empty native tree has no root generation to recover. Apply and remove
    // one deterministic bootstrap value before catalog visibility so the
    // first checkpoint has a durable root but exposes no reserved user key.
    for (client_sequence, operation) in [
        (
            1,
            MutationOperation::Put {
                key: Vec::new(),
                value: b"crowdb-bootstrap".to_vec(),
            },
        ),
        (2, MutationOperation::Delete { key: Vec::new() }),
    ] {
        partition
            .mutate(
                config.owner_epoch,
                RequestId {
                    client_high: config.partition_id.high,
                    client_low: config.partition_id.low,
                    client_sequence,
                },
                operation,
            )
            .await
            .map_err(|error| StorageRuntimeError::Partition(format!("bootstrap mutation: {error}")))?;
    }
    partition
        .checkpoint(config.owner_epoch)
        .await
        .map(|_| ())
        .map_err(|error| StorageRuntimeError::Partition(format!("bootstrap checkpoint: {error}")))
}

fn split_plan(transition: &SplitTransition) -> SplitPlan {
    SplitPlan {
        transition_id: TransitionId {
            high: transition.transition_id.high,
            low: transition.transition_id.low,
        },
        parent_id: partition_id(transition.parent_id),
        parent_range: partition_range(&transition.parent_range),
        parent_epoch: transition.parent_epoch,
        parent_next_epoch: transition.parent_next_epoch,
        split_key: transition.split_key.clone(),
        child: split_child(&transition.child),
    }
}

fn split_child(child: &SplitChildAssignment) -> SplitChild {
    SplitChild {
        partition_id: partition_id(child.partition_id),
        range: partition_range(&child.range),
        ownership_epoch: child.owner_epoch,
    }
}

fn partition_id(id: crowdb_protocol::chunk_kv::Id128) -> PartitionId {
    PartitionId {
        high: id.high,
        low: id.low,
    }
}

fn partition_range(range: &crowdb_protocol::chunk_kv::KeyRange) -> PartitionRange {
    PartitionRange {
        start: Some(range.start.clone()),
        end: range.end.clone(),
    }
}

fn validate_prepared_split(
    transition: &SplitTransition,
    artifact: &SplitArtifact,
) -> Result<(), crate::MonitorError> {
    let exact = artifact.transition_id
        == (TransitionId {
            high: transition.transition_id.high,
            low: transition.transition_id.low,
        })
        && artifact.parent_id == partition_id(transition.parent_id)
        && artifact.parent_epoch == transition.parent_epoch
        && artifact.parent_next_epoch == transition.parent_next_epoch
        && artifact.retained_parent.partition_id == partition_id(transition.parent_id)
        && artifact.retained_parent.range
            == PartitionRange {
                start: Some(transition.parent_range.start.clone()),
                end: Some(transition.split_key.clone()),
            }
        && artifact.retained_parent.ownership_epoch == transition.parent_next_epoch
        && artifact.retained_parent.tree_id == transition.retained_parent_artifact.tree_id
        && artifact.retained_parent.stream_name == transition.retained_parent_artifact.stream_name
        && artifact.child.partition_id == partition_id(transition.child.partition_id)
        && artifact.child.tree_id == transition.child.artifact.tree_id
        && artifact.child.stream_name == transition.child.artifact.stream_name
        && artifact.cutover_seq != 0
        && artifact.child.applied_seq == artifact.cutover_seq;
    if exact {
        Ok(())
    } else {
        Err(storage_plan_error(
            "prepared split does not match the persisted identities and frontier",
        ))
    }
}

fn storage_plan_error(error: &str) -> crate::MonitorError {
    crate::MonitorError::PlanFailed(error.into())
}

fn overlay_recovery_error(
    entry: &ChunkKvRangeCatalogEntry,
    overlay: &TailOverlayArtifact,
    error: &impl std::fmt::Display,
) -> StorageRuntimeError {
    StorageRuntimeError::Partition(format!(
        "overlay recovery failed (partition={:?}, epoch={}, tree={}, base_seq={}, cutover_seq={}): {error}",
        entry.partition_id,
        entry.owner_epoch,
        entry.artifact.tree_id,
        overlay.base_applied_seq,
        overlay.cutover_seq
    ))
}

fn assignment_recovery_error(
    entry: &ChunkKvRangeCatalogEntry,
    error: &impl std::fmt::Display,
) -> StorageRuntimeError {
    StorageRuntimeError::Partition(format!(
        "assignment recovery failed (partition={:?}, epoch={}, tree={}): {error}",
        entry.partition_id, entry.owner_epoch, entry.artifact.tree_id
    ))
}

/// Opens the production root authority adapter without a chunk IO fixture.
///
/// # Errors
///
/// Returns a root identity, KV availability or stale authority error.
#[cfg(feature = "test-util")]
pub async fn open_root_catalog_for_tests(
    kv: Arc<CrowdbKvClient>,
    store_id: u64,
    group_id: u64,
    entry: &ChunkKvRangeCatalogEntry,
) -> Result<Arc<dyn crowdb_tree_ffi::RootCatalogStore>, StorageRuntimeError> {
    let catalog = KvRootCatalogStore::for_assignment(kv, store_id, group_id, entry).await?;
    Ok(Arc::new(catalog))
}
