// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production chunk-stream dependency assembly.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, SmallWritePolicy};
use crowdb_chunk_kv::{
    MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange, PreparedChildArtifact,
    PreparedSplit, RequestId, SplitArtifact, SplitChild, SplitChildTarget, SplitPlan, StreamPartitionJournal,
    TransitionId,
};
use crowdb_chunk_stream::{ChunkStream, ProductionStreamRuntime, StreamConfig, StreamName, StreamRegistry};
use crowdb_kv_client::{BatchOp, ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogEntry, SplitChildAssignment, SplitTransition};
use crowdb_protocol::chunk_stream::{StreamBinding, StreamBindingState};
use crowdb_tree_ffi::{
    ChunkPageStoreOptions, ChunkRootCatalog, ChunkTransport, OwnedChunkRpcDiskRoute,
    OwnedChunkRpcTransportOptions, PageStore, RootCatalogObject, RootCatalogStore,
};
use thiserror::Error;

use crate::{BootstrapPartitionConfig, ChunkKvServerConfig};

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
                small_write: SmallWritePolicy::default(),
            },
            Arc::clone(&kv),
        )
        .await
        .map_err(|error| StorageRuntimeError::ChunkIo(error.to_string()))?;
        Self::from_parts(
            kv,
            chunk_io,
            config.storage.metadata_store_id,
            config.storage.stream_writer_lease_ms,
            config.storage.stream_mirror_copies,
        )
        .await
    }

    async fn from_parts_with_mirror_copies(
        kv: Arc<CrowdbKvClient>,
        chunk_io: ChunkIoClient,
        metadata_store_id: u64,
        writer_lease_ms: u64,
        stream_mirror_copies: u32,
    ) -> Result<Self, StorageRuntimeError> {
        let streams = Arc::new(
            ProductionStreamRuntime::new_with_mirror_copies(
                Arc::clone(&kv),
                &chunk_io,
                writer_lease_ms,
                ChunkReadPolicy::default(),
                StreamConfig::default(),
                stream_mirror_copies,
            )
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))?,
        );
        Self::assemble(kv, chunk_io, streams, metadata_store_id, writer_lease_ms).await
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
        )
        .await
    }

    async fn assemble(
        kv: Arc<CrowdbKvClient>,
        chunk_io: ChunkIoClient,
        streams: Arc<ProductionStreamRuntime>,
        metadata_store_id: u64,
        writer_lease_ms: u64,
    ) -> Result<Self, StorageRuntimeError> {
        let (chunkdb, disks) = chunk_io
            .native_storage_routes()
            .await
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?;
        let disk_routes = disks
            .into_iter()
            .map(|(disk_id, route)| OwnedChunkRpcDiskRoute {
                disk_id_high: disk_id.high,
                disk_id_low: disk_id.low,
                route,
            })
            .collect();
        let tree_transport = Arc::new(
            ChunkTransport::open_owned_rpc(OwnedChunkRpcTransportOptions {
                chunkdb,
                disk_routes,
                writer_lease_ms,
                rpc_timeout_ms: writer_lease_ms,
                completion_capacity: 1_024,
            })
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?,
        );
        Ok(Self {
            kv,
            chunk_io,
            streams,
            tree_transport,
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
        options: ChunkPageStoreOptions,
        catalog: Arc<ChunkRootCatalog>,
    ) -> Result<Arc<PageStore>, StorageRuntimeError> {
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
    /// and returns a partition fenced in `Prepared` state.
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
        let stream = self.open_stream(stream_name, entry.owner_epoch).await?;
        let page_store = self
            .open_durable_tree_page_store(
                ChunkPageStoreOptions {
                    tree_id: entry.artifact.tree_id,
                    owner_epoch: entry.owner_epoch,
                    pack_bytes: 0,
                    iu_size: 0,
                    max_concurrent_packs: 0,
                    materialization_bytes_per_pass: 0,
                },
                binding.metadata_group_id,
            )
            .await?;
        if let Some(overlay) = &entry.artifact.tail_overlay {
            let parent_stream = self
                .streams
                .open_read_only(
                    overlay.source_stream_name,
                    self.metadata_store_id,
                    overlay.source_epoch,
                )
                .await
                .map_err(|error| StorageRuntimeError::Stream(error.to_string()))?;
            let artifact = PreparedChildArtifact {
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
            };
            return Partition::recover_native_prepared_overlay(
                artifact,
                PartitionConfig::default(),
                crowdb_tree_ffi::Config::default(),
                page_store,
                stream,
                parent_stream,
            )
            .await
            .map_err(|error| StorageRuntimeError::Partition(error.to_string()));
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
        .map_err(|error| StorageRuntimeError::Partition(error.to_string()))
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
                    pack_bytes: 0,
                    iu_size: 0,
                    max_concurrent_packs: 0,
                    materialization_bytes_per_pass: 0,
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

    /// Rebuilds both durable split children from the authoritative parent.
    ///
    /// Existing child streams and tree roots are reopened under the same
    /// stable identities, so a `ParentPreparing` retry after restart replaces
    /// incomplete preparation with a newly fenced common frontier.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting stream bindings, stale tree authority,
    /// invalid transition identity, or an incomplete R142 split preparation.
    pub async fn prepare_split(
        &self,
        parent: &Partition,
        transition: &SplitTransition,
        max_fence_lag_records: u64,
    ) -> Result<PreparedSplit, crate::MonitorError> {
        let parent_binding = self
            .streams
            .registry()
            .load(transition.parent_artifact.stream_name)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?
            .ok_or_else(|| storage_plan_error("split parent stream binding does not exist"))?;
        let left = self
            .split_target(&transition.left, parent_binding.metadata_group_id)
            .await?;
        let right = self
            .split_target(&transition.right, parent_binding.metadata_group_id)
            .await?;
        let prepared = parent
            .prepare_split(split_plan(transition), left, right, max_fence_lag_records)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        validate_prepared_split(transition, &prepared.artifact)?;
        Ok(prepared)
    }

    async fn split_target(
        &self,
        child: &SplitChildAssignment,
        metadata_group_id: u64,
    ) -> Result<SplitChildTarget, crate::MonitorError> {
        let stream = self
            .open_or_create_empty_stream(child.artifact.stream_name, child.owner_epoch, metadata_group_id)
            .await?;
        let page_store = self
            .open_durable_tree_page_store(
                ChunkPageStoreOptions {
                    tree_id: child.artifact.tree_id,
                    owner_epoch: child.owner_epoch,
                    pack_bytes: 0,
                    iu_size: 0,
                    max_concurrent_packs: 0,
                    materialization_bytes_per_pass: 0,
                },
                metadata_group_id,
            )
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        Ok(SplitChildTarget {
            tree_id: child.artifact.tree_id,
            tree_config: crowdb_tree_ffi::Config {
                page_store: Some(page_store),
                ..crowdb_tree_ffi::Config::default()
            },
            journal: Arc::new(StreamPartitionJournal::new(stream, child.artifact.stream_name)),
        })
    }

    async fn open_or_create_empty_stream(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        metadata_group_id: u64,
    ) -> Result<ChunkStream, crate::MonitorError> {
        let registry = self.streams.registry();
        let expected = StreamBinding {
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
        if stream.tail() != 0 {
            return Err(storage_plan_error("split child WAL is not empty"));
        }
        Ok(stream)
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
        split_key: transition.split_key.clone(),
        left: split_child(&transition.left),
        right: split_child(&transition.right),
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
        && artifact.left.partition_id == partition_id(transition.left.partition_id)
        && artifact.left.tree_id == transition.left.artifact.tree_id
        && artifact.left.stream_name == transition.left.artifact.stream_name
        && artifact.right.partition_id == partition_id(transition.right.partition_id)
        && artifact.right.tree_id == transition.right.artifact.tree_id
        && artifact.right.stream_name == transition.right.artifact.stream_name
        && artifact.cutover_seq != 0
        && artifact.left.applied_seq == artifact.cutover_seq
        && artifact.right.applied_seq == artifact.cutover_seq;
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

struct KvRootCatalogStore {
    kv: Arc<CrowdbKvClient>,
    runtime: tokio::runtime::Handle,
    store_id: u64,
    group_id: u64,
    tree_id: u64,
    owner_epoch: u64,
    published_objects: ArcSwap<HashMap<Vec<u8>, Vec<u8>>>,
}

impl KvRootCatalogStore {
    async fn claim(
        kv: Arc<CrowdbKvClient>,
        store_id: u64,
        group_id: u64,
        tree_id: u64,
        owner_epoch: u64,
    ) -> Result<Self, StorageRuntimeError> {
        if group_id == 0 || tree_id == 0 || owner_epoch == 0 {
            return Err(StorageRuntimeError::Tree(
                "root catalog group, tree, and owner epoch must be nonzero".into(),
            ));
        }
        let key = catalog_key(tree_id, b"authority", 0);
        loop {
            let (current_epoch, generation, revision) = match kv
                .get(store_id, group_id, &key, ReadMode::Linearizable, None)
                .await
                .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?
            {
                GetOutcome::NotFound => (0, 0, 0),
                GetOutcome::Found { value, revision } => {
                    let (epoch, generation) = decode_authority(&value)
                        .ok_or_else(|| StorageRuntimeError::Tree("invalid root authority record".into()))?;
                    (epoch, generation, revision)
                }
            };
            if current_epoch > owner_epoch {
                return Err(StorageRuntimeError::Tree("tree root owner epoch is stale".into()));
            }
            if current_epoch == owner_epoch {
                break;
            }
            match kv
                .put_cas(
                    store_id,
                    group_id,
                    &key,
                    &encode_authority(owner_epoch, generation),
                    revision,
                )
                .await
            {
                Ok(_) => break,
                Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy) => {}
                Err(error) => return Err(StorageRuntimeError::Tree(error.to_string())),
            }
        }
        Ok(Self {
            kv,
            runtime: tokio::runtime::Handle::current(),
            store_id,
            group_id,
            tree_id,
            owner_epoch,
            published_objects: ArcSwap::from_pointee(HashMap::new()),
        })
    }

    fn wait<F: std::future::Future>(&self, future: F) -> F::Output {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<(Bytes, u64)>, crowdb_kv_client::Error> {
        self.kv
            .get(self.store_id, self.group_id, key, ReadMode::Linearizable, None)
            .await
            .map(|outcome| match outcome {
                GetOutcome::Found { value, revision } => Some((value, revision)),
                GetOutcome::NotFound => None,
            })
    }

    fn remember_published(&self, key: &[u8], value: &[u8]) {
        self.published_objects.rcu(|current| {
            let mut next = HashMap::clone(current);
            next.insert(key.to_vec(), value.to_vec());
            next
        });
    }
}

impl RootCatalogStore for KvRootCatalogStore {
    fn load(
        &self,
        tree_id: u64,
        object: RootCatalogObject,
    ) -> Result<Option<Vec<u8>>, crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let key = object_key(tree_id, object);
        if let Some(value) = self.published_objects.load().get(&key) {
            return Ok(Some(value.clone()));
        }
        let value = self
            .wait(self.get(&key))
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?;
        if value.is_none() && object == RootCatalogObject::CurrentManifest {
            let authority_key = catalog_key(tree_id, b"authority", 0);
            if let Some((authority, _)) = self
                .wait(self.get(&authority_key))
                .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?
            {
                let (_, generation) =
                    decode_authority(&authority).ok_or(crowdb_tree_ffi::CtError::Corruption)?;
                if generation != 0 {
                    return Err(crowdb_tree_ffi::CtError::Corruption);
                }
            }
        }
        Ok(value.map(|(bytes, _)| bytes.to_vec()))
    }

    fn store(
        &self,
        tree_id: u64,
        object: RootCatalogObject,
        data: &[u8],
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id || !matches!(object, RootCatalogObject::ReferenceSegment(_)) {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let key = object_key(tree_id, object);
        self.wait(self.kv.put(self.store_id, self.group_id, &key, data, None))
            .map(|_| ())
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?;
        self.remember_published(&key, data);
        Ok(())
    }

    fn publish(
        &self,
        tree_id: u64,
        expected_generation: u64,
        owner_epoch: u64,
        generation: u64,
        manifest: &[u8],
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id || owner_epoch != self.owner_epoch || generation != expected_generation + 1
        {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let authority_key = catalog_key(tree_id, b"authority", 0);
        let Some((authority, revision)) = self
            .wait(self.get(&authority_key))
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?
        else {
            return Err(crowdb_tree_ffi::CtError::Unavailable);
        };
        if decode_authority(&authority) != Some((owner_epoch, expected_generation)) {
            return Err(crowdb_tree_ffi::CtError::Unavailable);
        }
        let ops = [
            BatchOp::Put {
                key: Bytes::from(authority_key.clone()),
                value: Bytes::copy_from_slice(&encode_authority(owner_epoch, generation)),
            },
            BatchOp::Put {
                key: Bytes::from(catalog_key(tree_id, b"current", 0)),
                value: Bytes::copy_from_slice(manifest),
            },
            BatchOp::Put {
                key: Bytes::from(catalog_key(tree_id, b"manifest", generation)),
                value: Bytes::copy_from_slice(manifest),
            },
        ];
        self.wait(
            self.kv
                .batch_write_cas(self.store_id, self.group_id, &ops, &authority_key, revision),
        )
        .map(|_| ())
        .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?;
        self.remember_published(&catalog_key(tree_id, b"current", 0), manifest);
        self.remember_published(&catalog_key(tree_id, b"manifest", generation), manifest);
        Ok(())
    }

    fn allocate_reference_segment_id(&self, tree_id: u64) -> Result<u64, crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let key = catalog_key(tree_id, b"next-reference", 0);
        loop {
            let (current, revision) = match self
                .wait(self.get(&key))
                .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?
            {
                Some((value, revision)) => (
                    decode_u64(&value).ok_or(crowdb_tree_ffi::CtError::Corruption)?,
                    revision,
                ),
                None => (1, 0),
            };
            let next = current
                .checked_add(1)
                .ok_or(crowdb_tree_ffi::CtError::ResourceExhausted)?;
            match self.wait(self.kv.put_cas(
                self.store_id,
                self.group_id,
                &key,
                &next.to_be_bytes(),
                revision,
            )) {
                Ok(_) => return Ok(current),
                Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy) => {}
                Err(_) => return Err(crowdb_tree_ffi::CtError::Unavailable),
            }
        }
    }

    fn discard_reference_segments(&self, tree_id: u64, object_ids: &[u64]) -> u64 {
        if tree_id != self.tree_id || object_ids.is_empty() {
            return 0;
        }
        let ops = object_ids
            .iter()
            .map(|object_id| BatchOp::Delete {
                key: Bytes::from(catalog_key(tree_id, b"reference", *object_id)),
            })
            .collect::<Vec<_>>();
        self.wait(self.kv.batch_write(self.store_id, self.group_id, &ops))
            .map_or(0, |_| object_ids.len() as u64)
    }

    fn reclaim_before(&self, tree_id: u64, generation: u64) -> u64 {
        if tree_id != self.tree_id || generation <= 2 {
            return 0;
        }
        let floor_key = catalog_key(tree_id, b"reclaim-floor", 0);
        let (floor, revision) = match self.wait(self.get(&floor_key)) {
            Ok(Some((value, revision))) => match decode_u64(&value) {
                Some(floor) => (floor, revision),
                None => return 0,
            },
            Ok(None) => (1, 0),
            Err(_) => return 0,
        };
        let end = generation.saturating_sub(1).min(floor.saturating_add(128));
        if floor >= end {
            return 0;
        }
        let mut ops = (floor..end)
            .map(|old_generation| BatchOp::Delete {
                key: Bytes::from(catalog_key(tree_id, b"manifest", old_generation)),
            })
            .collect::<Vec<_>>();
        ops.push(BatchOp::Put {
            key: Bytes::from(floor_key.clone()),
            value: Bytes::copy_from_slice(&end.to_be_bytes()),
        });
        self.wait(
            self.kv
                .batch_write_cas(self.store_id, self.group_id, &ops, &floor_key, revision),
        )
        .map_or(0, |_| end - floor)
    }
}

fn catalog_key(tree_id: u64, kind: &[u8], object_id: u64) -> Vec<u8> {
    let mut key = b"\0crowdb/chunk-kv/root/v1/".to_vec();
    key.extend_from_slice(&tree_id.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(kind);
    key.push(b'/');
    key.extend_from_slice(&object_id.to_be_bytes());
    key
}

fn object_key(tree_id: u64, object: RootCatalogObject) -> Vec<u8> {
    match object {
        RootCatalogObject::CurrentManifest => catalog_key(tree_id, b"current", 0),
        RootCatalogObject::Manifest(generation) => catalog_key(tree_id, b"manifest", generation),
        RootCatalogObject::ReferenceSegment(object_id) => catalog_key(tree_id, b"reference", object_id),
    }
}

fn encode_authority(owner_epoch: u64, generation: u64) -> [u8; 16] {
    let mut value = [0; 16];
    value[..8].copy_from_slice(&owner_epoch.to_be_bytes());
    value[8..].copy_from_slice(&generation.to_be_bytes());
    value
}

fn decode_authority(value: &[u8]) -> Option<(u64, u64)> {
    (value.len() == 16).then(|| (decode_u64(&value[..8]).unwrap(), decode_u64(&value[8..]).unwrap()))
}

fn decode_u64(value: &[u8]) -> Option<u64> {
    value.try_into().ok().map(u64::from_be_bytes)
}
