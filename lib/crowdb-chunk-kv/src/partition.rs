// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Partition state machine: core lifecycle, mutations, and split/merge.
//! Sub-modules: [`frame`] (wire codec), [`journal`] (durability),
//! [`tree`] (ordered storage).

mod frame;
mod journal;
mod split;
mod tree;

pub use frame::{decode_frame, encode_frame, DecodedFrame, FrameDecode, MAX_FRAME_BYTES};
pub use journal::{PartitionJournal, StreamPartitionJournal};
pub use split::{PreparedSplit, PreparedSplitWriter, SplitSessionTargets, SplitWriterTarget};
pub use tree::{CrowdbPartitionTree, PartitionTree};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::{Buf, Bytes, BytesMut};
use crowdb_chunk_stream::{ChunkStream, StreamName};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

use crate::{
    canonical_operation_digest, Checkpoint, ChunkKvError, CompareCondition, JournalPosition,
    MutationOperation, MutationResult, PartitionId, PartitionLifecycle, PartitionMetrics, PartitionRange,
    PreparedSplitWriterArtifact, RequestId, Result, SplitAbortProof, SplitArtifact, SplitCommitProof,
    SplitPlan, TransitionId, ValueRevision, WalRecord,
};

#[derive(Clone, Debug)]
pub struct PartitionConfig {
    pub queue_requests: usize,
    pub queue_bytes: u64,
    pub batch_requests: usize,
    pub batch_bytes: u64,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub retained_results: usize,
    pub metadata_reclaim_pages_per_pass: usize,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            queue_requests: 1_024,
            queue_bytes: 64 * 1024 * 1024,
            batch_requests: 64,
            batch_bytes: 4 * 1024 * 1024,
            max_key_bytes: 64 * 1024,
            max_value_bytes: 16 * 1024 * 1024,
            retained_results: 65_536,
            metadata_reclaim_pages_per_pass: 128,
        }
    }
}

impl PartitionConfig {
    fn validate(&self) -> Result<()> {
        if self.queue_requests == 0
            || self.queue_bytes == 0
            || self.batch_requests == 0
            || self.batch_bytes == 0
            || self.max_key_bytes == 0
            || self.max_value_bytes == 0
            || self.retained_results == 0
            || self.metadata_reclaim_pages_per_pass == 0
        {
            return Err(ChunkKvError::InvalidRequest(
                "partition bounds must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationResponse {
    pub mutation_seq: u64,
    pub result: MutationResult,
    pub journal_position: JournalPosition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: Bytes,
    pub value: ValueRevision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPage {
    pub entries: Vec<ScanEntry>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionSnapshot {
    pub partition_id: PartitionId,
    pub range: PartitionRange,
    pub ownership_epoch: u64,
    pub lifecycle: PartitionLifecycle,
    pub stream_name: StreamName,
    pub journal_durable_seq: u64,
    pub journal_durable_offset: u64,
    pub applied_seq: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointReclaim {
    pub journal_bytes: u64,
    pub metadata_pages: u64,
    pub tree_bytes: u64,
    pub orphan_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaterializationProgress {
    pub bytes_written: u64,
    pub complete: bool,
}

struct MutationRequest {
    request_id: RequestId,
    operation: MutationOperation,
    digest: [u8; 32],
    reserved_bytes: u64,
    completion: oneshot::Sender<Result<MutationResponse>>,
}

#[derive(Clone)]
pub struct Partition {
    id: PartitionId,
    range: Arc<ArcSwap<PartitionRange>>,
    ownership_epoch: Arc<AtomicU64>,
    lifecycle: Arc<AtomicU8>,
    journal: Arc<dyn PartitionJournal>,
    tree: Arc<dyn PartitionTree>,
    sender: mpsc::Sender<MutationRequest>,
    queued_requests: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicU64>,
    journal_durable_seq: Arc<AtomicU64>,
    applied_seq: Arc<AtomicU64>,
    applied_position: Arc<AtomicU64>,
    retry_replay_offset: Arc<AtomicU64>,
    checkpoint_pin_generation: Arc<AtomicU64>,
    checkpoint_pin_transition_high: Arc<AtomicU64>,
    checkpoint_pin_transition_low: Arc<AtomicU64>,
    applied_notify: Arc<Notify>,
    admission_notify: Arc<Notify>,
    split_transition: Arc<Mutex<Option<SplitTransition>>>,
    split_ingress: Arc<ArcSwapOption<SplitIngressRoute>>,
    prepared_artifact: Option<PreparedSplitWriterArtifact>,
    inherited_position: Option<JournalPosition>,
    metrics: Arc<PartitionMetrics>,
    config: Arc<PartitionConfig>,
}

struct WorkerState {
    partition_id: PartitionId,
    ownership_epoch: Arc<AtomicU64>,
    lifecycle: Arc<AtomicU8>,
    journal: Arc<dyn PartitionJournal>,
    tree: Arc<dyn PartitionTree>,
    queued_requests: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicU64>,
    journal_durable_seq: Arc<AtomicU64>,
    applied_seq: Arc<AtomicU64>,
    applied_position: Arc<AtomicU64>,
    retry_replay_offset: Arc<AtomicU64>,
    applied_notify: Arc<Notify>,
    admission_notify: Arc<Notify>,
    metrics: Arc<PartitionMetrics>,
    config: Arc<PartitionConfig>,
    next_seq: u64,
    results: HashMap<RequestId, RetainedResult>,
    result_order: std::collections::VecDeque<RequestId>,
    expired_floor: HashMap<(u64, u64), u64>,
}

#[derive(Clone)]
struct RetainedResult {
    digest: [u8; 32],
    response: MutationResponse,
}

struct RecoverySeed {
    applied_seq: u64,
    applied_position: u64,
    retry_replay_offset: u64,
    results: HashMap<RequestId, RetainedResult>,
    result_order: std::collections::VecDeque<RequestId>,
    expired_floor: HashMap<(u64, u64), u64>,
    recovered: bool,
}

struct ReplayState {
    seed: RecoverySeed,
    checkpoint_applied_seq: u64,
    stream_name: StreamName,
    retained_results: usize,
    replayed: HashMap<u64, WalRecord>,
    last_new_sequence: Option<u64>,
}

struct SplitTransition {
    plan: SplitPlan,
    artifact: Option<SplitArtifact>,
}

/// Lock-free request routing installed after both split writers are durable.
/// The legacy parent handle remains valid while callers still hold it, but it
/// no longer appends to its old WAL or memtable.
#[derive(Clone)]
pub struct SplitIngress {
    split_key: Arc<[u8]>,
    retained_parent: Partition,
    child: Partition,
}

enum SplitIngressRoute {
    Buffering(Arc<SplitIngressBuffer>),
    Writers(Box<SplitIngress>),
}

struct BufferedSplitMutation {
    request_id: RequestId,
    operation: MutationOperation,
    completion: oneshot::Sender<Result<MutationResponse>>,
}

/// Bounded, lock-free admission path used only while the old WAL tail is
/// replayed into both split writers.  Requests are never rejected solely
/// because a split is reaching its durable frontier.
struct SplitIngressBuffer {
    sender: mpsc::Sender<BufferedSplitMutation>,
    receiver: Mutex<mpsc::Receiver<BufferedSplitMutation>>,
}

impl SplitIngressBuffer {
    fn new(capacity: usize) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel(capacity);
        Arc::new(Self {
            sender,
            receiver: Mutex::new(receiver),
        })
    }

    async fn enqueue(&self, request_id: RequestId, operation: MutationOperation) -> Result<MutationResponse> {
        let (completion, response) = oneshot::channel();
        self.sender
            .try_send(BufferedSplitMutation {
                request_id,
                operation,
                completion,
            })
            .map_err(|_| ChunkKvError::Overloaded)?;
        response.await.map_err(|_| ChunkKvError::WriteStalled)?
    }

    async fn forward_into(&self, ingress: &SplitIngress) {
        let mut receiver = self.receiver.lock().await;
        while let Ok(request) = receiver.try_recv() {
            let writer = ingress.writer_for(request.operation.key()).clone();
            let result = writer
                .mutate(
                    writer.ownership_epoch.load(Ordering::Acquire),
                    request.request_id,
                    request.operation,
                )
                .await;
            let _ = request.completion.send(result);
        }
    }
}

impl SplitIngress {
    #[must_use]
    pub fn retained_parent(&self) -> Partition {
        self.retained_parent.clone()
    }

    #[must_use]
    pub fn child(&self) -> Partition {
        self.child.clone()
    }

    fn writer_for(&self, key: &[u8]) -> &Partition {
        if key < self.split_key.as_ref() {
            &self.retained_parent
        } else {
            &self.child
        }
    }
}

struct PreparedMutation {
    request: MutationRequest,
    response: MutationResponse,
    record: WalRecord,
    frame: Bytes,
    followers: Vec<MutationRequest>,
}

impl Partition {
    /// Opens a serving partition over injected tree and journal owners.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid range/configuration, zero epoch, or
    /// inconsistent recovered frontiers.
    pub fn open(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
    ) -> Result<Self> {
        range.validate()?;
        config.validate()?;
        if ownership_epoch == 0 || tree.tree_id() == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "ownership epoch and tree identity must be nonzero".into(),
            ));
        }
        let applied = tree.last_applied_seq();
        Self::start(
            partition_id,
            range,
            ownership_epoch,
            config,
            tree,
            journal,
            RecoverySeed {
                applied_seq: applied,
                applied_position: 0,
                retry_replay_offset: 0,
                results: HashMap::new(),
                result_order: std::collections::VecDeque::new(),
                expired_floor: HashMap::new(),
                recovered: false,
            },
            PartitionLifecycle::Serving,
            None,
        )
    }

    /// Opens a serving partition while consuming its native tree store and
    /// private stream handle.
    ///
    /// # Errors
    ///
    /// Returns a typed tree-open or partition configuration error.
    #[allow(clippy::too_many_arguments)]
    pub fn open_native(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        config: PartitionConfig,
        tree_id: u64,
        tree_config: crowdb_tree_ffi::Config,
        page_store: Arc<crowdb_tree_ffi::PageStore>,
        stream: ChunkStream,
    ) -> Result<Self> {
        range.validate()?;
        config.validate()?;
        let (tree, journal) = native_storage_parts(tree_id, &range, tree_config, page_store, stream)?;
        Self::open(partition_id, range, ownership_epoch, config, tree, journal)
    }

    /// Replays the durable suffix after `checkpoint` before serving.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid checkpoint identity/frontiers, corrupt or
    /// conflicting WAL frames, stale epochs, or failed tree apply.
    pub async fn recover(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        checkpoint: Checkpoint,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
    ) -> Result<Self> {
        Self::recover_assignment(
            partition_id,
            range,
            ownership_epoch,
            checkpoint,
            config,
            tree,
            journal,
            PartitionLifecycle::Serving,
        )
        .await
    }

    /// Replays a durable assignment but keeps it fenced in `Prepared` state.
    ///
    /// The owning server must call [`Self::activate_recovered`] only after it
    /// validates matching external serving authority.
    ///
    /// # Errors
    ///
    /// Returns the same identity, frontier, replay, and storage errors as
    /// [`Self::recover`].
    pub async fn recover_prepared_assignment(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        checkpoint: Checkpoint,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
    ) -> Result<Self> {
        Self::recover_assignment(
            partition_id,
            range,
            ownership_epoch,
            checkpoint,
            config,
            tree,
            journal,
            PartitionLifecycle::Prepared,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_assignment(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        checkpoint: Checkpoint,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
        initial_lifecycle: PartitionLifecycle,
    ) -> Result<Self> {
        range.validate()?;
        config.validate()?;
        if ownership_epoch == 0
            || checkpoint.tree_id == 0
            || checkpoint.stream_manifest_generation == 0
            || checkpoint.stream_manifest_generation > journal.manifest_generation()
            || checkpoint.stream_name != journal.stream_name()
            || checkpoint.tree_id != tree.tree_id()
        {
            return Err(ChunkKvError::InvalidRequest(
                "checkpoint identity or epoch is invalid".into(),
            ));
        }
        if tree.checkpoint_state()? != (checkpoint.tree_manifest, checkpoint.applied_seq) {
            return Err(ChunkKvError::TreeCorruption(
                "tree root or frontier differs from checkpoint".into(),
            ));
        }
        let seed = replay_suffix(
            partition_id,
            ownership_epoch,
            &checkpoint,
            config.retained_results,
            tree.as_ref(),
            journal.as_ref(),
        )
        .await?;
        Self::start(
            partition_id,
            range,
            ownership_epoch,
            config,
            tree,
            journal,
            seed,
            initial_lifecycle,
            None,
        )
    }

    /// Recovers a partition while consuming its native tree store and private
    /// stream handle.
    ///
    /// # Errors
    ///
    /// Returns a typed tree-open, checkpoint, or WAL recovery error.
    #[allow(clippy::too_many_arguments)]
    pub async fn recover_native(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        checkpoint: Checkpoint,
        config: PartitionConfig,
        tree_config: crowdb_tree_ffi::Config,
        page_store: Arc<crowdb_tree_ffi::PageStore>,
        stream: ChunkStream,
    ) -> Result<Self> {
        Self::recover_native_assignment(
            partition_id,
            range,
            ownership_epoch,
            checkpoint,
            config,
            tree_config,
            page_store,
            stream,
            PartitionLifecycle::Serving,
        )
        .await
    }

    /// Reopens native storage, replays WAL, and remains `Prepared` until an
    /// exact external authority proof activates the assignment.
    ///
    /// # Errors
    ///
    /// Returns a typed tree-open, checkpoint, or WAL recovery error.
    #[allow(clippy::too_many_arguments)]
    pub async fn recover_native_prepared_assignment(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        checkpoint: Checkpoint,
        config: PartitionConfig,
        tree_config: crowdb_tree_ffi::Config,
        page_store: Arc<crowdb_tree_ffi::PageStore>,
        stream: ChunkStream,
    ) -> Result<Self> {
        Self::recover_native_assignment(
            partition_id,
            range,
            ownership_epoch,
            checkpoint,
            config,
            tree_config,
            page_store,
            stream,
            PartitionLifecycle::Prepared,
        )
        .await
    }

    /// Reopens the latest authoritative native tree root and stream manifest,
    /// then replays WAL while remaining `Prepared`.
    ///
    /// The tree root supplies its own applied frontier and WAL replay offset;
    /// callers provide only stable storage identities.
    ///
    /// # Errors
    ///
    /// Returns a typed tree-open, root-checkpoint, or WAL recovery error.
    #[allow(clippy::too_many_arguments)]
    pub async fn recover_native_latest_prepared_assignment(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        tree_id: u64,
        config: PartitionConfig,
        tree_config: crowdb_tree_ffi::Config,
        page_store: Arc<crowdb_tree_ffi::PageStore>,
        stream: ChunkStream,
    ) -> Result<Self> {
        range.validate()?;
        config.validate()?;
        if tree_id == 0 || ownership_epoch == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "tree identity and ownership epoch must be nonzero".into(),
            ));
        }
        let replay_offset = page_store.wal_replay_offset().map_err(|error| match error {
            crowdb_tree_ffi::CtError::Corruption => ChunkKvError::TreeCorruption(error.to_string()),
            _ => ChunkKvError::TreeUnavailable(error.to_string()),
        })?;
        let stream_name = stream.stream_name();
        let stream_manifest_generation = stream.manifest_generation();
        let (tree, journal) = native_storage_parts(tree_id, &range, tree_config, page_store, stream)?;
        let (tree_manifest, applied_seq) = tree.checkpoint_state()?;
        let root_manifest_generation = tree.root_manifest_generation()?;
        Self::recover_assignment(
            partition_id,
            range,
            ownership_epoch,
            Checkpoint {
                tree_id,
                tree_manifest,
                root_manifest_generation,
                applied_seq,
                stream_name,
                stream_manifest_generation,
                replay_offset,
            },
            config,
            tree,
            journal,
            PartitionLifecycle::Prepared,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_native_assignment(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        checkpoint: Checkpoint,
        config: PartitionConfig,
        tree_config: crowdb_tree_ffi::Config,
        page_store: Arc<crowdb_tree_ffi::PageStore>,
        stream: ChunkStream,
        initial_lifecycle: PartitionLifecycle,
    ) -> Result<Self> {
        range.validate()?;
        config.validate()?;
        if checkpoint.stream_name != stream.stream_name() {
            return Err(ChunkKvError::InvalidRequest(
                "checkpoint stream identity is invalid".into(),
            ));
        }
        let (tree, journal) =
            native_storage_parts(checkpoint.tree_id, &range, tree_config, page_store, stream)?;
        Self::recover_assignment(
            partition_id,
            range,
            ownership_epoch,
            checkpoint,
            config,
            tree,
            journal,
            initial_lifecycle,
        )
        .await
    }

    /// Recovers and validates one child artifact without granting service.
    ///
    /// # Errors
    ///
    /// Returns an error when the artifact, checkpoint, tree, or journal does
    /// not identify the same complete child frontier.
    pub async fn recover_prepared(
        artifact: PreparedSplitWriterArtifact,
        checkpoint: Checkpoint,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
    ) -> Result<Self> {
        if artifact.ownership_epoch == 0
            || artifact.tree_id == 0
            || artifact.root_manifest_generation == 0
            || checkpoint.stream_manifest_generation == 0
            || checkpoint.stream_manifest_generation > journal.manifest_generation()
            || artifact.stream_name != checkpoint.stream_name
            || artifact.stream_name != journal.stream_name()
            || artifact.tree_id != checkpoint.tree_id
            || artifact.tree_id != tree.tree_id()
            || artifact.tree_manifest != checkpoint.tree_manifest
            || artifact.root_manifest_generation != checkpoint.root_manifest_generation
            || artifact.applied_seq != checkpoint.applied_seq
        {
            return Err(ChunkKvError::InvalidRequest(
                "prepared artifact does not match its checkpoint".into(),
            ));
        }
        artifact.range.validate()?;
        config.validate()?;
        if tree.last_applied_seq() != checkpoint.applied_seq {
            return Err(ChunkKvError::TreeCorruption(
                "prepared tree frontier differs from checkpoint".into(),
            ));
        }
        let seed = replay_suffix(
            artifact.partition_id,
            artifact.ownership_epoch,
            &checkpoint,
            config.retained_results,
            tree.as_ref(),
            journal.as_ref(),
        )
        .await?;
        if seed.applied_seq != artifact.applied_seq {
            return Err(ChunkKvError::JournalCorruption(
                "prepared child stream advanced beyond its artifact".into(),
            ));
        }
        Self::start(
            artifact.partition_id,
            artifact.range.clone(),
            artifact.ownership_epoch,
            config,
            tree,
            journal,
            seed,
            PartitionLifecycle::Prepared,
            Some(artifact),
        )
    }

    /// Recovers a split child from its durable base, filtered parent suffix,
    /// and child journal without checkpointing the warmed overlay.
    ///
    /// A tree already warmed through the cutover is accepted for the local
    /// pre-publication handoff; a reopened base tree deterministically reapplies
    /// the same parent suffix after restart.
    ///
    /// # Errors
    ///
    /// Returns an identity, frontier, source-tail, child-tail, or apply error.
    pub async fn recover_prepared_overlay(
        artifact: PreparedSplitWriterArtifact,
        checkpoint: Checkpoint,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
        parent_journal: Arc<dyn PartitionJournal>,
    ) -> Result<Self> {
        validate_prepared_overlay(
            &artifact,
            &checkpoint,
            tree.as_ref(),
            journal.as_ref(),
            parent_journal.as_ref(),
        )?;
        artifact.range.validate()?;
        config.validate()?;
        let tree_frontier = tree.last_applied_seq();
        let warmed = tree_frontier == artifact.applied_seq;
        if !warmed && tree_frontier != artifact.base_applied_seq {
            return Err(ChunkKvError::TreeCorruption(
                "prepared child tree is neither at base nor cutover".into(),
            ));
        }
        let mut seed = replay_parent_overlay(
            &artifact,
            config.retained_results,
            tree.as_ref(),
            parent_journal.as_ref(),
            warmed,
        )
        .await?;
        seed = replay_child_overlay(
            artifact.partition_id,
            artifact.ownership_epoch,
            artifact.applied_seq,
            config.retained_results,
            tree.as_ref(),
            journal.as_ref(),
            seed,
        )
        .await?;
        seed.retry_replay_offset = 0;
        let overlay_records = artifact.applied_seq.saturating_sub(artifact.base_applied_seq);
        let overlay_bytes = artifact
            .parent_cutover_offset
            .saturating_sub(artifact.parent_replay_offset);
        let partition = Self::start(
            artifact.partition_id,
            artifact.range.clone(),
            artifact.ownership_epoch,
            config,
            tree,
            journal,
            seed,
            PartitionLifecycle::Prepared,
            Some(artifact),
        )?;
        partition
            .metrics
            .split_overlay_apply(overlay_records, overlay_bytes);
        Ok(partition)
    }

    /// Reopens native child storage plus an immutable parent stream snapshot
    /// and recovers the persisted overlay in `Prepared` state.
    ///
    /// # Errors
    ///
    /// Returns the same identity, tree, source-tail, and child-tail errors as
    /// [`Self::recover_prepared_overlay`].
    pub async fn recover_native_prepared_overlay(
        artifact: PreparedSplitWriterArtifact,
        config: PartitionConfig,
        tree_config: crowdb_tree_ffi::Config,
        page_store: Arc<crowdb_tree_ffi::PageStore>,
        stream: ChunkStream,
        parent_stream: ChunkStream,
    ) -> Result<Self> {
        let checkpoint = Checkpoint {
            tree_id: artifact.tree_id,
            tree_manifest: artifact.tree_manifest,
            root_manifest_generation: artifact.root_manifest_generation,
            applied_seq: artifact.base_applied_seq,
            stream_name: artifact.stream_name,
            stream_manifest_generation: stream.manifest_generation(),
            replay_offset: 0,
        };
        let range = artifact.range.clone();
        let (tree, journal) =
            native_storage_parts(artifact.tree_id, &range, tree_config, page_store, stream)?;
        let parent_journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(
            parent_stream,
            artifact.parent_stream_name,
        ));
        Self::recover_prepared_overlay(artifact, checkpoint, config, tree, journal, parent_journal).await
    }

    #[allow(clippy::too_many_arguments)]
    fn start(
        partition_id: PartitionId,
        range: PartitionRange,
        ownership_epoch: u64,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
        seed: RecoverySeed,
        initial_lifecycle: PartitionLifecycle,
        prepared_artifact: Option<PreparedSplitWriterArtifact>,
    ) -> Result<Self> {
        let next_seq = seed
            .applied_seq
            .checked_add(1)
            .ok_or_else(|| ChunkKvError::Faulted("mutation sequence exhausted".into()))?;
        let (sender, receiver) = mpsc::channel(config.queue_requests);
        let lifecycle = Arc::new(AtomicU8::new(lifecycle_code(initial_lifecycle)));
        let ownership_epoch = Arc::new(AtomicU64::new(ownership_epoch));
        let queued_requests = Arc::new(AtomicUsize::new(0));
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let journal_durable_seq = Arc::new(AtomicU64::new(seed.applied_seq));
        let applied_seq = Arc::new(AtomicU64::new(seed.applied_seq));
        let applied_position = Arc::new(AtomicU64::new(seed.applied_position));
        let retry_replay_offset = Arc::new(AtomicU64::new(seed.retry_replay_offset));
        let checkpoint_pin_generation = Arc::new(AtomicU64::new(0));
        let checkpoint_pin_transition_high = Arc::new(AtomicU64::new(0));
        let checkpoint_pin_transition_low = Arc::new(AtomicU64::new(0));
        let applied_notify = Arc::new(Notify::new());
        let admission_notify = Arc::new(Notify::new());
        let split_transition = Arc::new(Mutex::new(None));
        let split_ingress = Arc::new(ArcSwapOption::empty());
        let metrics = Arc::new(PartitionMetrics::default());
        if seed.recovered {
            metrics.recovery();
        }
        let config = Arc::new(config);
        let state = WorkerState {
            partition_id,
            ownership_epoch: Arc::clone(&ownership_epoch),
            lifecycle: Arc::clone(&lifecycle),
            journal: Arc::clone(&journal),
            tree: Arc::clone(&tree),
            queued_requests: Arc::clone(&queued_requests),
            queued_bytes: Arc::clone(&queued_bytes),
            journal_durable_seq: Arc::clone(&journal_durable_seq),
            applied_seq: Arc::clone(&applied_seq),
            applied_position: Arc::clone(&applied_position),
            retry_replay_offset: Arc::clone(&retry_replay_offset),
            applied_notify: Arc::clone(&applied_notify),
            admission_notify: Arc::clone(&admission_notify),
            metrics: Arc::clone(&metrics),
            config: Arc::clone(&config),
            next_seq,
            results: seed.results,
            result_order: seed.result_order,
            expired_floor: seed.expired_floor,
        };
        tokio::spawn(run_worker(state, receiver));
        let inherited_position = prepared_artifact.as_ref().map(|artifact| JournalPosition {
            stream_name: artifact.parent_stream_name,
            offset: artifact.parent_cutover_offset,
        });
        Ok(Self {
            id: partition_id,
            range: Arc::new(ArcSwap::from_pointee(range)),
            ownership_epoch,
            lifecycle,
            journal,
            tree,
            sender,
            queued_requests,
            queued_bytes,
            journal_durable_seq,
            applied_seq,
            applied_position,
            retry_replay_offset,
            checkpoint_pin_generation,
            checkpoint_pin_transition_high,
            checkpoint_pin_transition_low,
            applied_notify,
            admission_notify,
            split_transition,
            split_ingress,
            prepared_artifact,
            inherited_position,
            metrics,
            config,
        })
    }

    /// Journals and applies one mutation under the supplied ownership epoch.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, lifecycle, admission, journal, or tree error.
    pub async fn mutate(
        &self,
        ownership_epoch: u64,
        request_id: RequestId,
        operation: MutationOperation,
    ) -> Result<MutationResponse> {
        self.metrics.mutation_request();
        if let Some(route) = self.split_ingress.load_full() {
            return match route.as_ref() {
                SplitIngressRoute::Buffering(buffer) => buffer.enqueue(request_id, operation).await,
                SplitIngressRoute::Writers(ingress) => {
                    let writer = ingress.writer_for(operation.key());
                    Box::pin(writer.mutate(
                        writer.ownership_epoch.load(Ordering::Acquire),
                        request_id,
                        operation,
                    ))
                    .await
                }
            };
        }
        self.validate_epoch(ownership_epoch)?;
        if !accepts_mutations(self.lifecycle()) {
            return Err(write_state_error(self.lifecycle()));
        }
        self.validate_operation(&operation)?;
        let reserved_bytes = estimated_request_bytes(&operation)?;
        if let Err(error) = reserve_requests(&self.queued_requests, self.config.queue_requests) {
            self.metrics.admission_backpressure();
            return Err(error);
        }
        if let Err(error) = reserve_bytes(&self.queued_bytes, self.config.queue_bytes, reserved_bytes) {
            if self.queued_requests.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.admission_notify.notify_waiters();
            }
            self.metrics.admission_backpressure();
            return Err(error);
        }
        if !accepts_mutations(self.lifecycle()) {
            release_admission(
                &self.queued_requests,
                &self.queued_bytes,
                &self.admission_notify,
                reserved_bytes,
            );
            return Err(write_state_error(self.lifecycle()));
        }
        let (completion, response) = oneshot::channel();
        let request = MutationRequest {
            request_id,
            digest: canonical_operation_digest(&operation),
            operation,
            reserved_bytes,
            completion,
        };
        if self.sender.try_send(request).is_err() {
            release_admission(
                &self.queued_requests,
                &self.queued_bytes,
                &self.admission_notify,
                reserved_bytes,
            );
            self.metrics.admission_backpressure();
            return Err(ChunkKvError::Overloaded);
        }
        response.await.map_err(|_| ChunkKvError::WriteStalled)?
    }

    /// Reads one key from the applied prefix, optionally waiting for a journal position.
    ///
    /// # Errors
    ///
    /// Returns a typed range, epoch, lifecycle, or tree-read error.
    pub async fn get(
        &self,
        ownership_epoch: u64,
        key: &[u8],
        min_journal_position: Option<JournalPosition>,
    ) -> Result<Option<ValueRevision>> {
        self.metrics.point_read();
        if let Some(route) = self.split_ingress.load_full() {
            if let SplitIngressRoute::Writers(ingress) = route.as_ref() {
                let writer = ingress.writer_for(key);
                let writer_epoch = writer.ownership_epoch.load(Ordering::Acquire);
                let writer_stream = writer.snapshot().stream_name;
                let position = min_journal_position.filter(|position| position.stream_name == writer_stream);
                return Box::pin(writer.get(writer_epoch, key, position)).await;
            }
        }
        self.validate_epoch(ownership_epoch)?;
        if !self.range.load().contains(key) {
            self.metrics.range_reject();
            return Err(ChunkKvError::OutOfRange);
        }
        match self.lifecycle() {
            PartitionLifecycle::Serving
            | PartitionLifecycle::WriteStalled
            | PartitionLifecycle::SplitPreparing
            | PartitionLifecycle::SplitFinalizing => {}
            state => return Err(read_state_error(state)),
        }
        if let Some(position) = min_journal_position {
            self.wait_applied(position).await?;
        }
        self.tree.get(key).await
    }

    /// Returns the first key greater than or equal to `key` in this partition.
    ///
    /// # Errors
    ///
    /// Returns a typed range, epoch, lifecycle, or tree-read error.
    pub async fn ceiling(
        &self,
        ownership_epoch: u64,
        key: &[u8],
        min_journal_position: Option<JournalPosition>,
    ) -> Result<Option<ScanEntry>> {
        self.seek_forward(ownership_epoch, key, true, min_journal_position)
            .await
    }

    /// Returns the first key strictly greater than `key` in this partition.
    ///
    /// # Errors
    ///
    /// Returns a typed range, epoch, lifecycle, or tree-read error.
    pub async fn higher(
        &self,
        ownership_epoch: u64,
        key: &[u8],
        min_journal_position: Option<JournalPosition>,
    ) -> Result<Option<ScanEntry>> {
        self.seek_forward(ownership_epoch, key, false, min_journal_position)
            .await
    }

    async fn seek_forward(
        &self,
        ownership_epoch: u64,
        key: &[u8],
        inclusive: bool,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<Option<ScanEntry>> {
        self.metrics.forward_seek();
        self.validate_epoch(ownership_epoch)?;
        self.validate_read_lifecycle()?;
        if key.len() > self.config.max_key_bytes {
            return Err(ChunkKvError::InvalidRequest(
                "seek key exceeds configured key limit".into(),
            ));
        }
        let range = self.range.load_full();
        if !range.contains(key) {
            self.metrics.range_reject();
            return Err(ChunkKvError::OutOfRange);
        }
        if let Some(position) = min_journal_position {
            self.wait_applied(position).await?;
        }
        let byte_budget = self
            .config
            .max_key_bytes
            .saturating_add(self.config.max_value_bytes);
        let (mut entries, _) = self
            .tree
            .scan_forward(Some(key), inclusive, range.end.as_deref(), 1, byte_budget)
            .await?;
        if entries.iter().any(|entry| {
            !range.contains(&entry.key)
                || entry.key.as_ref() < key
                || (!inclusive && entry.key.as_ref() == key)
        }) {
            return Err(ChunkKvError::TreeCorruption(
                "tree seek returned an invalid key".into(),
            ));
        }
        Ok(entries.pop())
    }

    /// Returns the final key less than or equal to `key` in this partition.
    ///
    /// # Errors
    ///
    /// Returns a typed range, epoch, lifecycle, or tree-read error.
    pub async fn floor(
        &self,
        ownership_epoch: u64,
        key: &[u8],
        min_journal_position: Option<JournalPosition>,
    ) -> Result<Option<ScanEntry>> {
        self.seek_reverse(ownership_epoch, key, true, min_journal_position)
            .await
    }

    /// Returns the final key strictly less than `key` in this partition.
    ///
    /// # Errors
    ///
    /// Returns a typed range, epoch, lifecycle, or tree-read error.
    pub async fn lower(
        &self,
        ownership_epoch: u64,
        key: &[u8],
        min_journal_position: Option<JournalPosition>,
    ) -> Result<Option<ScanEntry>> {
        self.seek_reverse(ownership_epoch, key, false, min_journal_position)
            .await
    }

    async fn seek_reverse(
        &self,
        ownership_epoch: u64,
        key: &[u8],
        inclusive: bool,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<Option<ScanEntry>> {
        self.metrics.reverse_seek();
        self.validate_epoch(ownership_epoch)?;
        self.validate_read_lifecycle()?;
        if key.len() > self.config.max_key_bytes {
            return Err(ChunkKvError::InvalidRequest(
                "seek key exceeds configured key limit".into(),
            ));
        }
        let range = self.range.load_full();
        if !range.contains(key) {
            self.metrics.range_reject();
            return Err(ChunkKvError::OutOfRange);
        }
        if let Some(position) = min_journal_position {
            self.wait_applied(position).await?;
        }
        let entry = self
            .tree
            .seek_reverse(key, inclusive, range.start.as_deref())
            .await?;
        if entry.as_ref().is_some_and(|entry| {
            !range.contains(&entry.key)
                || entry.key.as_ref() > key
                || (!inclusive && entry.key.as_ref() == key)
        }) {
            return Err(ChunkKvError::TreeCorruption(
                "tree reverse seek returned an invalid key".into(),
            ));
        }
        Ok(entry)
    }

    /// Returns a bounded forward page over the half-open interval
    /// `[start_key, end_key)`, clipped to this partition.
    ///
    /// The lower bound is inclusive and the upper bound is exclusive. Bounds
    /// outside the partition are clipped; a non-intersecting interval returns
    /// no entries.
    ///
    /// # Errors
    ///
    /// Returns a typed epoch, lifecycle, bound, or tree-read error.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_forward(
        &self,
        ownership_epoch: u64,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<ScanPage> {
        self.scan_forward_bound(
            ownership_epoch,
            start_key,
            true,
            end_key,
            limit,
            byte_budget,
            min_journal_position,
        )
        .await
    }

    /// Returns a bounded forward continuation strictly after `start_after`.
    ///
    /// This is distinct from the inclusive initial range lower bound so a
    /// continuation cannot repeat its last emitted key.
    ///
    /// # Errors
    ///
    /// Returns a typed epoch, lifecycle, bound, or tree-read error.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_forward_after(
        &self,
        ownership_epoch: u64,
        start_after: &[u8],
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<ScanPage> {
        self.scan_forward_bound(
            ownership_epoch,
            Some(start_after),
            false,
            end_key,
            limit,
            byte_budget,
            min_journal_position,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_forward_bound(
        &self,
        ownership_epoch: u64,
        start_key: Option<&[u8]>,
        start_inclusive: bool,
        end_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<ScanPage> {
        self.validate_epoch(ownership_epoch)?;
        self.validate_read_lifecycle()?;
        if limit == 0 || byte_budget == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "scan count and byte bounds must be nonzero".into(),
            ));
        }
        if start_key.is_some_and(|key| key.len() > self.config.max_key_bytes)
            || end_key.is_some_and(|key| key.len() > self.config.max_key_bytes)
        {
            return Err(ChunkKvError::InvalidRequest(
                "scan bound exceeds configured key limit".into(),
            ));
        }
        if let (Some(start), Some(end)) = (start_key, end_key) {
            if start >= end {
                return Err(ChunkKvError::InvalidRequest(
                    "scan interval is empty or reversed".into(),
                ));
            }
        }
        if let Some(position) = min_journal_position {
            self.wait_applied(position).await?;
        }
        let range = self.range.load_full();
        let partition_start = range.start.as_deref();
        let partition_end = range.end.as_deref();
        if start_key
            .zip(partition_end)
            .is_some_and(|(start, end)| start >= end)
            || end_key
                .zip(partition_start)
                .is_some_and(|(end, start)| end <= start)
        {
            self.metrics.forward_scan(0);
            return Ok(ScanPage {
                entries: Vec::new(),
                truncated: false,
            });
        }
        let clipped_start = match (start_key, partition_start) {
            (Some(start), Some(partition_start)) if start < partition_start => None,
            (start, _) => start,
        };
        let clipped_end = match (end_key, partition_end) {
            (Some(end), Some(partition_end)) => Some(end.min(partition_end)),
            (Some(end), None) => Some(end),
            (None, end) => end,
        };
        let (entries, truncated) = self
            .tree
            .scan_forward(clipped_start, start_inclusive, clipped_end, limit, byte_budget)
            .await?;
        if entries.iter().any(|entry| !range.contains(&entry.key)) {
            return Err(ChunkKvError::TreeCorruption(
                "tree scan returned a key outside the partition".into(),
            ));
        }
        self.metrics.forward_scan(entries.len());
        Ok(ScanPage { entries, truncated })
    }

    /// Returns a bounded descending page clipped to this partition.
    ///
    /// `start_before` is exclusive and `begin_key` is inclusive. Bounds
    /// outside the partition are clipped; a non-intersecting interval returns
    /// no entries.
    ///
    /// # Errors
    ///
    /// Returns a typed epoch, lifecycle, bound, or tree-read error.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_reverse(
        &self,
        ownership_epoch: u64,
        start_before: Option<&[u8]>,
        begin_key: Option<&[u8]>,
        limit: usize,
        byte_budget: usize,
        min_journal_position: Option<JournalPosition>,
    ) -> Result<ScanPage> {
        self.validate_epoch(ownership_epoch)?;
        self.validate_read_lifecycle()?;
        if limit == 0 || byte_budget == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "scan count and byte bounds must be nonzero".into(),
            ));
        }
        if start_before.is_some_and(|key| key.len() > self.config.max_key_bytes)
            || begin_key.is_some_and(|key| key.len() > self.config.max_key_bytes)
        {
            return Err(ChunkKvError::InvalidRequest(
                "scan bound exceeds configured key limit".into(),
            ));
        }
        if begin_key
            .zip(start_before)
            .is_some_and(|(begin, start)| begin >= start)
        {
            return Err(ChunkKvError::InvalidRequest(
                "scan interval is empty or reversed".into(),
            ));
        }
        if let Some(position) = min_journal_position {
            self.wait_applied(position).await?;
        }
        let range = self.range.load_full();
        let partition_start = range.start.as_deref();
        let partition_end = range.end.as_deref();
        if start_before
            .zip(partition_start)
            .is_some_and(|(start, partition_start)| start <= partition_start)
            || begin_key
                .zip(partition_end)
                .is_some_and(|(begin, partition_end)| begin >= partition_end)
        {
            self.metrics.reverse_scan(0);
            return Ok(ScanPage {
                entries: Vec::new(),
                truncated: false,
            });
        }
        let clipped_start = match (start_before, partition_end) {
            (Some(start), Some(partition_end)) => Some(start.min(partition_end)),
            (Some(start), None) => Some(start),
            (None, end) => end,
        };
        let clipped_begin = match (begin_key, partition_start) {
            (Some(begin), Some(partition_start)) => Some(begin.max(partition_start)),
            (Some(begin), None) => Some(begin),
            (None, start) => start,
        };
        let (entries, truncated) = self
            .tree
            .scan_reverse(clipped_start, clipped_begin, limit, byte_budget)
            .await?;
        if entries.iter().any(|entry| !range.contains(&entry.key))
            || entries.windows(2).any(|pair| pair[0].key <= pair[1].key)
        {
            return Err(ChunkKvError::TreeCorruption(
                "tree reverse scan returned invalid key order or range".into(),
            ));
        }
        self.metrics.reverse_scan(entries.len());
        Ok(ScanPage { entries, truncated })
    }

    fn validate_read_lifecycle(&self) -> Result<()> {
        match self.lifecycle() {
            PartitionLifecycle::Serving
            | PartitionLifecycle::WriteStalled
            | PartitionLifecycle::SplitPreparing
            | PartitionLifecycle::SplitFinalizing => Ok(()),
            state => Err(read_state_error(state)),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> PartitionSnapshot {
        PartitionSnapshot {
            partition_id: self.id,
            range: self.range.load().as_ref().clone(),
            ownership_epoch: self.ownership_epoch.load(Ordering::Acquire),
            lifecycle: self.lifecycle(),
            stream_name: self.journal.stream_name(),
            journal_durable_seq: self.journal_durable_seq.load(Ordering::Acquire),
            journal_durable_offset: self.journal.tail(),
            applied_seq: self.applied_seq.load(Ordering::Acquire),
        }
    }

    #[must_use]
    pub fn tree_id(&self) -> u64 {
        self.tree.tree_id()
    }

    /// Persists an exact root-generation reference before publishing a
    /// topology artifact that depends on it.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the generation is absent or conflicts
    /// with the transition's existing pin.
    pub fn retain_generation_pin(&self, transition_id: TransitionId, generation: u64) -> Result<()> {
        let current = self.checkpoint_pin_generation.load(Ordering::Acquire);
        if current != 0
            && (current != generation
                || self.checkpoint_pin_transition_high.load(Ordering::Acquire) != transition_id.high
                || self.checkpoint_pin_transition_low.load(Ordering::Acquire) != transition_id.low)
        {
            return Err(ChunkKvError::InvalidRequest(
                "another transition already pins the partition checkpoint lineage".into(),
            ));
        }
        self.tree.pin_generation(transition_id, generation)?;
        self.checkpoint_pin_transition_high
            .store(transition_id.high, Ordering::Relaxed);
        self.checkpoint_pin_transition_low
            .store(transition_id.low, Ordering::Relaxed);
        self.checkpoint_pin_generation
            .store(generation, Ordering::Release);
        Ok(())
    }

    /// Releases this partition tree's durable generation pin after the
    /// authoritative catalog has removed its overlay dependency.
    ///
    /// # Errors
    ///
    /// Returns a storage error when durable pin deletion cannot complete.
    pub fn release_generation_pin(&self, transition_id: TransitionId) -> Result<()> {
        self.tree.unpin_generation(transition_id)?;
        if self.checkpoint_pin_transition_high.load(Ordering::Acquire) == transition_id.high
            && self.checkpoint_pin_transition_low.load(Ordering::Acquire) == transition_id.low
        {
            self.checkpoint_pin_generation.store(0, Ordering::Release);
        }
        Ok(())
    }

    #[must_use]
    pub fn lifecycle(&self) -> PartitionLifecycle {
        lifecycle_from_code(self.lifecycle.load(Ordering::Acquire))
    }

    #[must_use]
    pub fn metrics(&self) -> &PartitionMetrics {
        &self.metrics
    }

    /// Stops a transfer source after all previously admitted work completes.
    ///
    /// # Errors
    ///
    /// Returns a stale-epoch or incompatible-lifecycle error.
    pub async fn suspend_for_transfer(&self, ownership_epoch: u64) -> Result<()> {
        self.validate_epoch(ownership_epoch)?;
        match self.lifecycle.compare_exchange(
            lifecycle_code(PartitionLifecycle::Serving),
            lifecycle_code(PartitionLifecycle::WriteStalled),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(observed) if lifecycle_from_code(observed) == PartitionLifecycle::WriteStalled => {}
            Err(observed) => return Err(write_state_error(lifecycle_from_code(observed))),
        }
        self.wait_for_admitted_mutations().await;
        Ok(())
    }

    /// Starts or idempotently resumes one exact split plan while writes continue.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed/stale plan, a conflicting transition,
    /// or a parent that is not serving.
    pub async fn begin_split(&self, plan: SplitPlan) -> Result<()> {
        plan.validate()?;
        if plan.parent_id != self.id || plan.parent_range != *self.range.load_full() {
            return Err(ChunkKvError::InvalidRequest(
                "split plan does not identify this parent range".into(),
            ));
        }
        self.validate_epoch(plan.parent_epoch)?;
        let mut transition = self.split_transition.lock().await;
        if let Some(active) = transition.as_ref() {
            return if active.plan == plan {
                Ok(())
            } else {
                Err(ChunkKvError::SplitRetry(
                    "another split transition is active".into(),
                ))
            };
        }
        self.lifecycle
            .compare_exchange(
                lifecycle_code(PartitionLifecycle::Serving),
                lifecycle_code(PartitionLifecycle::SplitPreparing),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|observed| write_state_error(lifecycle_from_code(observed)))?;
        *transition = Some(SplitTransition { plan, artifact: None });
        self.metrics.split_begin();
        Ok(())
    }

    /// Starts the buffered finalization after serving catch-up reaches its budget.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown transition or incompatible lifecycle.
    pub async fn begin_split_finalization(&self, transition_id: TransitionId) -> Result<()> {
        {
            let transition = self.split_transition.lock().await;
            if transition.as_ref().map(|active| active.plan.transition_id) != Some(transition_id) {
                return Err(ChunkKvError::SplitRetry(
                    "split transition identity does not match".into(),
                ));
            }
        }
        match self.lifecycle.compare_exchange(
            lifecycle_code(PartitionLifecycle::SplitPreparing),
            lifecycle_code(PartitionLifecycle::SplitFinalizing),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(observed) if lifecycle_from_code(observed) == PartitionLifecycle::SplitFinalizing => {}
            Err(observed) => return Err(write_state_error(lifecycle_from_code(observed))),
        }
        self.wait_for_admitted_mutations().await;
        self.metrics.split_finalization();
        Ok(())
    }

    /// Records the immutable child artifact produced at the drained cutover.
    ///
    /// # Errors
    ///
    /// Returns an error unless the artifact exactly matches the active plan
    /// and current cutover frontier.
    pub async fn record_split_artifact(&self, artifact: SplitArtifact) -> Result<()> {
        if self.lifecycle() != PartitionLifecycle::SplitFinalizing
            || self.queued_requests.load(Ordering::Acquire) != 0
        {
            return Err(ChunkKvError::SplitRetry(
                "split artifact requires a drained parent finalization".into(),
            ));
        }
        let mut transition = self.split_transition.lock().await;
        let active = transition
            .as_mut()
            .ok_or_else(|| ChunkKvError::SplitRetry("no split transition is active".into()))?;
        validate_split_artifact(&active.plan, &artifact, self.applied_seq.load(Ordering::Acquire))?;
        if let Some(previous) = active.artifact.as_ref() {
            if previous != &artifact {
                return Err(ChunkKvError::SplitRetry(
                    "split artifact conflicts with the prepared cutover".into(),
                ));
            }
        } else {
            active.artifact = Some(artifact);
        }
        Ok(())
    }

    /// Returns the exact prepared split artifact for an idempotent worker retry.
    #[must_use]
    pub async fn prepared_split_artifact(&self, transition_id: TransitionId) -> Option<SplitArtifact> {
        if self.lifecycle() != PartitionLifecycle::SplitFinalizing {
            return None;
        }
        self.split_transition
            .lock()
            .await
            .as_ref()
            .filter(|active| active.plan.transition_id == transition_id)
            .and_then(|active| active.artifact.clone())
    }

    /// Returns the prepared artifact while this parent awaits catalog commit.
    #[must_use]
    pub async fn current_prepared_split_artifact(&self) -> Option<SplitArtifact> {
        if self.lifecycle() != PartitionLifecycle::SplitFinalizing {
            return None;
        }
        self.split_transition
            .lock()
            .await
            .as_ref()
            .and_then(|active| active.artifact.clone())
    }

    /// Completes the process-local handoff after both replacement writers are
    /// installed.  The old handle remains a dispatcher for stale routes, but
    /// it no longer represents an unfinished topology transition.
    ///
    /// # Errors
    ///
    /// Returns an error unless `artifact` is the exact durable artifact of
    /// the active finalization and the replacement ingress is installed.
    pub async fn complete_local_split_handoff(&self, artifact: &SplitArtifact) -> Result<()> {
        if self.lifecycle() != PartitionLifecycle::SplitFinalizing || self.split_ingress().is_none() {
            return Err(ChunkKvError::SplitRetry(
                "local split handoff requires a finalized parent ingress".into(),
            ));
        }
        let mut transition = self.split_transition.lock().await;
        if transition.as_ref().and_then(|active| active.artifact.as_ref()) != Some(artifact) {
            return Err(ChunkKvError::SplitRetry(
                "local split handoff does not match the prepared artifact".into(),
            ));
        }
        *transition = None;
        self.lifecycle
            .store(lifecycle_code(PartitionLifecycle::Serving), Ordering::Release);
        Ok(())
    }

    /// Publishes the locally durable split writers to callers that still hold
    /// the pre-split parent handle.  This is deliberately an atomic snapshot:
    /// a request observes either the old parent sequencer or one complete pair
    /// of new writers, never a half-installed route.
    ///
    /// # Errors
    ///
    /// Returns an error unless the routing pair exactly covers the active
    /// split plan and both writer epochs are the planned post-split epochs.
    pub async fn install_split_ingress(&self, retained_parent: Partition, child: Partition) -> Result<()> {
        let transition = self.split_transition.lock().await;
        let active = transition
            .as_ref()
            .ok_or_else(|| ChunkKvError::SplitRetry("no split transition is active".into()))?;
        let retained = retained_parent.snapshot();
        let child_snapshot = child.snapshot();
        let (expected_parent, expected_child) = active.plan.parent_range.split(&active.plan.split_key)?;
        if retained.partition_id != self.id
            || retained.ownership_epoch != active.plan.parent_next_epoch
            || retained.range != expected_parent
            || child_snapshot.partition_id != active.plan.child.partition_id
            || child_snapshot.ownership_epoch != active.plan.child.ownership_epoch
            || child_snapshot.range != expected_child
        {
            return Err(ChunkKvError::SplitRetry(
                "split ingress writers do not match the active plan".into(),
            ));
        }
        let ingress = SplitIngress {
            split_key: Arc::from(active.plan.split_key.clone()),
            retained_parent,
            child,
        };
        let buffered = self
            .split_ingress
            .load_full()
            .and_then(|route| match route.as_ref() {
                SplitIngressRoute::Buffering(buffer) => Some(Arc::clone(buffer)),
                SplitIngressRoute::Writers(_) => None,
            });
        self.split_ingress
            .store(Some(Arc::new(SplitIngressRoute::Writers(Box::new(
                ingress.clone(),
            )))));
        if let Some(buffer) = buffered {
            buffer.forward_into(&ingress).await;
        }
        Ok(())
    }

    /// Atomically diverts new parent requests into a bounded session buffer.
    /// Previously admitted parent mutations are allowed to finish on the old
    /// WAL; the buffer is released only after both new writers are durable.
    pub(crate) fn begin_split_ingress_buffer(&self) -> Result<()> {
        if self.lifecycle() != PartitionLifecycle::SplitPreparing {
            return Err(ChunkKvError::SplitRetry(
                "split ingress buffer requires an active split preparation".into(),
            ));
        }
        let buffer = SplitIngressBuffer::new(self.config.queue_requests);
        let previous = self.split_ingress.compare_and_swap(
            std::ptr::null(),
            Some(Arc::new(SplitIngressRoute::Buffering(buffer))),
        );
        if previous.is_some() {
            return Err(ChunkKvError::SplitRetry(
                "split ingress route is already installed".into(),
            ));
        }
        Ok(())
    }

    /// Returns the locally installed split writers, if both have become
    /// durable.  Catalog reconciliation uses this to adopt the retained
    /// parent directly instead of reconstructing it from the old range.
    #[must_use]
    pub fn split_ingress(&self) -> Option<SplitIngress> {
        self.split_ingress
            .load_full()
            .and_then(|route| match route.as_ref() {
                SplitIngressRoute::Writers(ingress) => Some((**ingress).clone()),
                SplitIngressRoute::Buffering(_) => None,
            })
    }

    /// Commits the retained parent only for an exact durable catalog proof.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent, stale, or mismatched proof.
    pub async fn commit_split(&self, proof: &SplitCommitProof) -> Result<()> {
        if proof.catalog_revision == 0 || self.lifecycle() != PartitionLifecycle::SplitFinalizing {
            return Err(ChunkKvError::SplitRetry(
                "split commit proof is absent or parent is not finalizing".into(),
            ));
        }
        let transition = self.split_transition.lock().await;
        let expected = transition.as_ref().and_then(|active| active.artifact.as_ref());
        if expected != Some(&proof.artifact) {
            return Err(ChunkKvError::SplitRetry(
                "split commit proof does not match the prepared artifact".into(),
            ));
        }
        self.ownership_epoch
            .store(proof.artifact.parent_next_epoch, Ordering::Release);
        let active = transition
            .as_ref()
            .ok_or_else(|| ChunkKvError::SplitRetry("split transition disappeared".into()))?;
        self.range.store(Arc::new(PartitionRange {
            start: active.plan.parent_range.start.clone(),
            end: Some(active.plan.split_key.clone()),
        }));
        self.lifecycle
            .store(lifecycle_code(PartitionLifecycle::Serving), Ordering::Release);
        self.metrics.split_commit();
        Ok(())
    }

    /// Activates a validated prepared child only for the exact published split
    /// artifact that contains it.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent catalog revision, mismatched artifact,
    /// or non-prepared lifecycle.
    pub fn activate_prepared(&self, proof: &SplitCommitProof) -> Result<()> {
        let expected = self.prepared_artifact.as_ref().ok_or_else(|| {
            ChunkKvError::SplitRetry("partition was not opened from a prepared artifact".into())
        })?;
        if proof.catalog_revision == 0 || &proof.artifact.child != expected {
            return Err(ChunkKvError::SplitRetry(
                "catalog proof does not contain the exact prepared child".into(),
            ));
        }
        self.lifecycle
            .compare_exchange(
                lifecycle_code(PartitionLifecycle::Prepared),
                lifecycle_code(PartitionLifecycle::Serving),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|observed| read_state_error(lifecycle_from_code(observed)))?;
        Ok(())
    }

    /// Activates either durable writer of one locally installed split session.
    /// Unlike [`Self::activate_prepared`], retained-parent and child artifacts
    /// are both accepted; external catalog publication still validates the
    /// complete enclosing split proof.
    ///
    /// # Errors
    ///
    /// Returns an error when the proof does not name this prepared writer or
    /// its lifecycle cannot transition to serving.
    pub fn activate_split_writer(&self, proof: &SplitCommitProof) -> Result<()> {
        let expected = self.prepared_artifact.as_ref().ok_or_else(|| {
            ChunkKvError::SplitRetry("partition was not opened from a prepared split writer".into())
        })?;
        if proof.catalog_revision == 0
            || (expected != &proof.artifact.child && expected != &proof.artifact.retained_parent)
        {
            return Err(ChunkKvError::SplitRetry(
                "catalog proof does not contain the prepared split writer".into(),
            ));
        }
        self.lifecycle
            .compare_exchange(
                lifecycle_code(PartitionLifecycle::Prepared),
                lifecycle_code(PartitionLifecycle::Serving),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|observed| read_state_error(lifecycle_from_code(observed)))?;
        Ok(())
    }

    /// Enables a durable split writer only behind its old-parent ingress
    /// route.  It does not grant external catalog service: callers must keep
    /// the writer out of the catalog snapshot until the group-0 generation is
    /// published.
    ///
    /// # Errors
    ///
    /// Returns an error when the artifact does not name this prepared writer
    /// or its lifecycle cannot transition to serving.
    pub fn activate_local_split_writer(&self, artifact: &SplitArtifact) -> Result<()> {
        let expected = self.prepared_artifact.as_ref().ok_or_else(|| {
            ChunkKvError::SplitRetry("partition was not opened from a prepared split writer".into())
        })?;
        if expected != &artifact.child && expected != &artifact.retained_parent {
            return Err(ChunkKvError::SplitRetry(
                "local split activation does not match its durable artifact".into(),
            ));
        }
        self.lifecycle
            .compare_exchange(
                lifecycle_code(PartitionLifecycle::Prepared),
                lifecycle_code(PartitionLifecycle::Serving),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|observed| read_state_error(lifecycle_from_code(observed)))?;
        Ok(())
    }

    /// Returns whether this prepared assignment carries a split-child
    /// artifact and therefore requires an exact catalog commit proof before
    /// it can serve.
    #[must_use]
    pub fn is_prepared_split_child(&self) -> bool {
        self.prepared_artifact.is_some()
    }

    /// Activates a replayed assignment after its owner validates external
    /// catalog and lease authority for the exact epoch.
    ///
    /// # Errors
    ///
    /// Returns a stale-epoch or lifecycle error. Split children must continue
    /// through [`Self::activate_prepared`] with their catalog commit proof.
    pub fn activate_recovered(&self, ownership_epoch: u64) -> Result<()> {
        self.validate_epoch(ownership_epoch)?;
        if matches!(
            self.lifecycle(),
            PartitionLifecycle::Serving | PartitionLifecycle::SplitPreparing
        ) {
            return Ok(());
        }
        if self.prepared_artifact.is_some() {
            return Err(ChunkKvError::InvalidRequest(
                "prepared split child requires a split commit proof".into(),
            ));
        }
        match self.lifecycle() {
            PartitionLifecycle::Serving | PartitionLifecycle::SplitPreparing => Ok(()),
            PartitionLifecycle::Prepared => self
                .lifecycle
                .compare_exchange(
                    lifecycle_code(PartitionLifecycle::Prepared),
                    lifecycle_code(PartitionLifecycle::Serving),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .map(|_| ())
                .map_err(|observed| read_state_error(lifecycle_from_code(observed))),
            state => Err(read_state_error(state)),
        }
    }

    /// Resumes the parent only after authoritative proof of non-publication.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent or mismatched proof.
    pub async fn abort_split(&self, proof: &SplitAbortProof) -> Result<()> {
        if proof.catalog_revision == 0 {
            return Err(ChunkKvError::SplitRetry("split abort proof is absent".into()));
        }
        let mut transition = self.split_transition.lock().await;
        let active = transition
            .as_ref()
            .ok_or_else(|| ChunkKvError::SplitRetry("no split transition is active".into()))?;
        if proof.transition_id != active.plan.transition_id
            || proof.parent_id != active.plan.parent_id
            || proof.parent_epoch != active.plan.parent_epoch
        {
            return Err(ChunkKvError::SplitRetry(
                "split abort proof does not match the active plan".into(),
            ));
        }
        match self.lifecycle() {
            PartitionLifecycle::SplitPreparing | PartitionLifecycle::SplitFinalizing => {}
            state => return Err(write_state_error(state)),
        }
        *transition = None;
        self.split_ingress.store(None);
        self.lifecycle
            .store(lifecycle_code(PartitionLifecycle::Serving), Ordering::Release);
        self.metrics.split_abort();
        Ok(())
    }

    /// Creates an exact checkpoint while a transfer source is quiesced.
    ///
    /// # Errors
    ///
    /// Returns an error unless the partition is drained and fenced, or if the
    /// tree checkpoint fails.
    pub async fn checkpoint_quiesced(&self, ownership_epoch: u64) -> Result<Checkpoint> {
        self.validate_epoch(ownership_epoch)?;
        let _maintenance = self.split_transition.lock().await;
        if self.lifecycle() != PartitionLifecycle::WriteStalled
            || self.queued_requests.load(Ordering::Acquire) != 0
        {
            return Err(ChunkKvError::InvalidRequest(
                "checkpoint requires a quiesced mutation source".into(),
            ));
        }
        self.create_checkpoint().await
    }

    /// Publishes a recoverable checkpoint while the partition continues
    /// serving. The replay offset is captured before the tree flush, so a
    /// concurrent mutation can only make recovery replay an already included
    /// record; it cannot create a WAL gap.
    ///
    /// # Errors
    ///
    /// Returns a stale-epoch, lifecycle, tree, or frontier error.
    pub async fn checkpoint(&self, ownership_epoch: u64) -> Result<Checkpoint> {
        self.validate_epoch(ownership_epoch)?;
        let _maintenance = self.split_transition.lock().await;
        if self.checkpoint_pin_generation.load(Ordering::Acquire) != 0 {
            return Err(ChunkKvError::SplitRetry(
                "checkpoint publication is suppressed by an exact-root transition pin".into(),
            ));
        }
        match self.lifecycle() {
            PartitionLifecycle::Serving
            | PartitionLifecycle::WriteStalled
            | PartitionLifecycle::SplitFinalizing => self.create_checkpoint().await,
            state => Err(read_state_error(state)),
        }
    }

    async fn create_checkpoint(&self) -> Result<Checkpoint> {
        let replay_offset = self.retry_replay_offset.load(Ordering::Acquire);
        let stream_manifest_generation = self.journal.manifest_generation();
        let (tree_manifest, applied_seq) = self.tree.checkpoint(replay_offset).await?;
        let root_manifest_generation = self.tree.root_manifest_generation()?;
        if applied_seq > self.journal_durable_seq.load(Ordering::Acquire)
            || self.tree.last_applied_seq() < applied_seq
        {
            return Err(ChunkKvError::ApplyStateUnknown);
        }
        self.metrics.checkpoint();
        Ok(Checkpoint {
            tree_id: self.tree.tree_id(),
            tree_manifest,
            root_manifest_generation,
            applied_seq,
            stream_name: self.journal.stream_name(),
            stream_manifest_generation,
            replay_offset,
        })
    }

    /// Trims WAL bytes only after the caller supplies the published checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for stale identity/frontiers or journal GC failure.
    pub async fn trim_published_checkpoint(
        &self,
        ownership_epoch: u64,
        checkpoint: &Checkpoint,
    ) -> Result<u64> {
        self.validate_epoch(ownership_epoch)?;
        if checkpoint.stream_name != self.journal.stream_name()
            || checkpoint.stream_manifest_generation == 0
            || checkpoint.stream_manifest_generation > self.journal.manifest_generation()
            || checkpoint.applied_seq > self.applied_seq.load(Ordering::Acquire)
            || checkpoint.replay_offset > self.journal.tail()
            || checkpoint.replay_offset > self.retry_replay_offset.load(Ordering::Acquire)
        {
            return Err(ChunkKvError::InvalidRequest(
                "checkpoint does not belong to this frontier".into(),
            ));
        }
        self.journal.trim_prefix(checkpoint.replay_offset).await
    }

    /// Advances both durable retention watermarks after the caller has
    /// published this exact checkpoint, then reclaims unreferenced chunk
    /// objects left by interrupted page-store work.
    ///
    /// # Errors
    ///
    /// Returns an error for stale authority, a mismatched checkpoint, or a
    /// journal/tree maintenance failure.
    pub async fn reclaim_published_checkpoint(
        &self,
        ownership_epoch: u64,
        checkpoint: &Checkpoint,
    ) -> Result<CheckpointReclaim> {
        let journal_bytes = self
            .trim_published_checkpoint(ownership_epoch, checkpoint)
            .await?;
        let metadata_pages = self
            .journal
            .reclaim_metadata_before(
                checkpoint.stream_manifest_generation,
                self.config.metadata_reclaim_pages_per_pass,
            )
            .await?;
        let tree_bytes = self.tree.reclaim_before(checkpoint.root_manifest_generation)?;
        let orphan_bytes = self.tree.reclaim_orphans()?;
        self.metrics
            .reclaim(journal_bytes, metadata_pages, tree_bytes, orphan_bytes);
        Ok(CheckpointReclaim {
            journal_bytes,
            metadata_pages,
            tree_bytes,
            orphan_bytes,
        })
    }

    /// Returns native chunk page-store counters when this partition uses the
    /// R140 backend.
    ///
    /// # Errors
    ///
    /// Returns a typed storage error if the native counters are unavailable.
    pub fn chunk_storage_stats(&self) -> Result<Option<crowdb_tree_ffi::ChunkPageStoreStats>> {
        self.tree.chunk_stats()
    }

    /// Runs one bounded R140 ownership-materialization pass after a split
    /// child has been activated. Call again until `complete` is true.
    ///
    /// Foreground reads and mutations remain available. Maintenance work is
    /// serialized with checkpoint and split preparation for this partition.
    ///
    /// # Errors
    ///
    /// Returns a stale-epoch, lifecycle, or storage-maintenance error.
    pub async fn materialize_split_ownership(&self, ownership_epoch: u64) -> Result<MaterializationProgress> {
        self.validate_epoch(ownership_epoch)?;
        let _maintenance = self.split_transition.lock().await;
        if self.lifecycle() != PartitionLifecycle::Serving {
            return Err(read_state_error(self.lifecycle()));
        }
        let started = Instant::now();
        match self.tree.materialize_ownership() {
            Ok((bytes_written, complete)) => {
                self.metrics
                    .materialization(Ok((bytes_written, complete)), elapsed_us(started));
                Ok(MaterializationProgress {
                    bytes_written,
                    complete,
                })
            }
            Err(error) => {
                self.metrics.materialization(Err(()), elapsed_us(started));
                Err(error)
            }
        }
    }

    fn validate_epoch(&self, ownership_epoch: u64) -> Result<()> {
        if ownership_epoch != self.ownership_epoch.load(Ordering::Acquire) {
            self.metrics.stale_epoch();
            return Err(ChunkKvError::StaleEpoch);
        }
        Ok(())
    }

    fn validate_operation(&self, operation: &MutationOperation) -> Result<()> {
        if !self.range.load().contains(operation.key()) {
            self.metrics.range_reject();
            return Err(ChunkKvError::OutOfRange);
        }
        if operation.key().len() > self.config.max_key_bytes {
            return Err(ChunkKvError::InvalidRequest(
                "key exceeds configured limit".into(),
            ));
        }
        if operation
            .successful_value()
            .is_some_and(|value| value.len() > self.config.max_value_bytes)
        {
            return Err(ChunkKvError::InvalidRequest(
                "value exceeds configured limit".into(),
            ));
        }
        Ok(())
    }

    async fn wait_applied(&self, position: JournalPosition) -> Result<()> {
        if self.inherited_position.is_some_and(|inherited| {
            inherited.stream_name == position.stream_name && inherited.offset >= position.offset
        }) {
            return Ok(());
        }
        if position.stream_name != self.journal.stream_name() {
            return Err(ChunkKvError::InvalidRequest(
                "journal position belongs to another stream".into(),
            ));
        }
        loop {
            let notified = self.applied_notify.notified();
            if self.applied_position.load(Ordering::Acquire) >= position.offset {
                return Ok(());
            }
            match self.lifecycle() {
                PartitionLifecycle::Recovering => return Err(ChunkKvError::Recovering),
                PartitionLifecycle::Faulted => return Err(ChunkKvError::Faulted("partition faulted".into())),
                _ => notified.await,
            }
        }
    }

    async fn wait_for_admitted_mutations(&self) {
        loop {
            let notified = self.admission_notify.notified();
            if self.queued_requests.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

fn native_storage_parts(
    tree_id: u64,
    range: &PartitionRange,
    mut tree_config: crowdb_tree_ffi::Config,
    page_store: Arc<crowdb_tree_ffi::PageStore>,
    stream: ChunkStream,
) -> Result<(Arc<dyn PartitionTree>, Arc<dyn PartitionJournal>)> {
    tree_config.page_store = Some(page_store);
    tree_config.key_range = crowdb_tree_ffi::KeyRange::Bounded {
        start: range.start.clone(),
        end: range.end.clone(),
    };
    let tree: Arc<dyn PartitionTree> = Arc::new(CrowdbPartitionTree::open(tree_id, &tree_config)?);
    let stream_name = stream.stream_name();
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name));
    Ok((tree, journal))
}

fn validate_split_artifact(plan: &SplitPlan, artifact: &SplitArtifact, cutover_seq: u64) -> Result<()> {
    if artifact.transition_id != plan.transition_id
        || artifact.parent_id != plan.parent_id
        || artifact.parent_epoch != plan.parent_epoch
        || artifact.parent_next_epoch != plan.parent_next_epoch
        || artifact.cutover_seq != cutover_seq
        || artifact.child.partition_id != plan.child.partition_id
        || artifact.child.range != plan.child.range
        || artifact.child.ownership_epoch != plan.child.ownership_epoch
        || artifact.child.applied_seq != cutover_seq
        || artifact.child.tree_id == 0
        || artifact.child.parent_id != plan.parent_id
        || artifact.child.parent_epoch != plan.parent_epoch
        || artifact.child.base_applied_seq > cutover_seq
        || artifact.child.child_stream_start_seq != cutover_seq.checked_add(1).unwrap_or(0)
        || artifact.retained_parent.partition_id != plan.parent_id
        || artifact.retained_parent.range != plan.parent_range.split(&plan.split_key)?.0
        || artifact.retained_parent.ownership_epoch != plan.parent_next_epoch
        || artifact.retained_parent.applied_seq != cutover_seq
        || artifact.retained_parent.tree_id == 0
        || artifact.retained_parent.parent_id != plan.parent_id
        || artifact.retained_parent.parent_epoch != plan.parent_epoch
        || artifact.retained_parent.base_applied_seq > cutover_seq
        || artifact.retained_parent.child_stream_start_seq != cutover_seq.checked_add(1).unwrap_or(0)
    {
        return Err(ChunkKvError::SplitRetry(
            "split artifact does not exactly match plan and cutover".into(),
        ));
    }
    Ok(())
}

fn validate_prepared_overlay(
    artifact: &PreparedSplitWriterArtifact,
    checkpoint: &Checkpoint,
    tree: &dyn PartitionTree,
    journal: &dyn PartitionJournal,
    parent_journal: &dyn PartitionJournal,
) -> Result<()> {
    let valid = artifact.ownership_epoch != 0
        && artifact.tree_id != 0
        && artifact.tree_id == checkpoint.tree_id
        && artifact.tree_id == tree.tree_id()
        && artifact.root_manifest_generation != 0
        && artifact.tree_manifest == checkpoint.tree_manifest
        && artifact.root_manifest_generation == checkpoint.root_manifest_generation
        && artifact.base_applied_seq == checkpoint.applied_seq
        && artifact.base_applied_seq <= artifact.applied_seq
        && artifact.child_stream_start_seq == artifact.applied_seq.checked_add(1).unwrap_or(0)
        && artifact.parent_id != PartitionId::default()
        && artifact.parent_epoch != 0
        && artifact.stream_name == checkpoint.stream_name
        && artifact.stream_name == journal.stream_name()
        && checkpoint.stream_manifest_generation != 0
        && checkpoint.stream_manifest_generation <= journal.manifest_generation()
        && artifact.parent_stream_name == parent_journal.stream_name()
        && artifact.parent_stream_name != artifact.stream_name
        && artifact.parent_stream_manifest_generation != 0
        && artifact.parent_stream_manifest_generation <= parent_journal.manifest_generation()
        && artifact.parent_replay_offset <= artifact.parent_cutover_offset
        && artifact.parent_cutover_offset <= parent_journal.tail();
    if !valid {
        return Err(ChunkKvError::InvalidRequest(
            "prepared overlay identities or frontiers are invalid".into(),
        ));
    }
    let observed_checkpoint = tree.checkpoint_state()?;
    if observed_checkpoint != (artifact.tree_manifest, artifact.base_applied_seq) {
        return Err(ChunkKvError::TreeCorruption(format!(
            "prepared overlay base differs from the tree checkpoint: expected ({}, {}), observed ({}, {})",
            artifact.tree_manifest, artifact.base_applied_seq, observed_checkpoint.0, observed_checkpoint.1
        )));
    }
    Ok(())
}

async fn replay_parent_overlay(
    artifact: &PreparedSplitWriterArtifact,
    retained_results: usize,
    tree: &dyn PartitionTree,
    journal: &dyn PartitionJournal,
    warmed: bool,
) -> Result<RecoverySeed> {
    let replay_checkpoint = Checkpoint {
        tree_id: artifact.tree_id,
        tree_manifest: artifact.tree_manifest,
        root_manifest_generation: artifact.root_manifest_generation,
        applied_seq: if warmed {
            artifact.applied_seq
        } else {
            artifact.base_applied_seq
        },
        stream_name: artifact.parent_stream_name,
        stream_manifest_generation: artifact.parent_stream_manifest_generation,
        replay_offset: artifact.parent_replay_offset,
    };
    let mut replay = ReplayState::new(&replay_checkpoint, retained_results);
    let mut read_offset = artifact.parent_replay_offset;
    let mut frame_offset = read_offset;
    let mut buffered = BytesMut::new();
    while read_offset < artifact.parent_cutover_offset {
        let remaining = artifact.parent_cutover_offset - read_offset;
        let max_bytes = usize::try_from(remaining.min(1024 * 1024)).unwrap_or(1024 * 1024);
        let bytes = journal.read_window(read_offset, max_bytes).await?;
        if bytes.is_empty() || bytes.len() as u64 > remaining {
            return Err(ChunkKvError::JournalCorruption(
                "parent overlay read did not preserve its cutover".into(),
            ));
        }
        read_offset += bytes.len() as u64;
        buffered.extend_from_slice(&bytes);
        while let FrameDecode::Complete(decoded) = decode_frame(&buffered)? {
            validate_replay_record(artifact.parent_id, artifact.parent_epoch, &decoded.record)?;
            let belongs = artifact.range.contains(decoded.record.operation.key());
            replay
                .process(tree, frame_offset, decoded.record, belongs)
                .await?;
            buffered.advance(decoded.bytes_consumed);
            frame_offset += decoded.bytes_consumed as u64;
        }
        if buffered.len() > crate::MAX_FRAME_BYTES {
            return Err(ChunkKvError::JournalCorruption(
                "parent overlay frame exceeds maximum size".into(),
            ));
        }
    }
    if !buffered.is_empty()
        || frame_offset != artifact.parent_cutover_offset
        || replay.seed.applied_seq != artifact.applied_seq
        || (artifact.base_applied_seq != artifact.applied_seq
            && replay.last_new_sequence != Some(artifact.applied_seq))
    {
        return Err(ChunkKvError::JournalCorruption(
            "parent overlay does not reach the exact cutover".into(),
        ));
    }
    replay.seed.applied_position = 0;
    Ok(replay.seed)
}

async fn replay_child_overlay(
    partition_id: PartitionId,
    ownership_epoch: u64,
    cutover_seq: u64,
    retained_results: usize,
    tree: &dyn PartitionTree,
    journal: &dyn PartitionJournal,
    seed: RecoverySeed,
) -> Result<RecoverySeed> {
    let mut replay = ReplayState {
        seed,
        checkpoint_applied_seq: cutover_seq,
        stream_name: journal.stream_name(),
        retained_results,
        replayed: HashMap::new(),
        last_new_sequence: None,
    };
    let mut read_offset = 0;
    let mut frame_offset = 0;
    let mut buffered = BytesMut::new();
    while read_offset < journal.tail() {
        let bytes = journal.read_window(read_offset, 1024 * 1024).await?;
        if bytes.is_empty() {
            return Err(ChunkKvError::JournalCorruption(
                "child overlay returned no bytes before its tail".into(),
            ));
        }
        read_offset += bytes.len() as u64;
        buffered.extend_from_slice(&bytes);
        while let FrameDecode::Complete(decoded) = decode_frame(&buffered)? {
            validate_replay_record(partition_id, ownership_epoch, &decoded.record)?;
            replay.process(tree, frame_offset, decoded.record, true).await?;
            buffered.advance(decoded.bytes_consumed);
            frame_offset += decoded.bytes_consumed as u64;
        }
        if buffered.len() > crate::MAX_FRAME_BYTES {
            return Err(ChunkKvError::JournalCorruption(
                "child overlay frame exceeds maximum size".into(),
            ));
        }
    }
    if !buffered.is_empty() {
        return Err(ChunkKvError::IncompleteFrame);
    }
    Ok(replay.seed)
}

async fn replay_suffix(
    partition_id: PartitionId,
    ownership_epoch: u64,
    checkpoint: &Checkpoint,
    retained_results: usize,
    tree: &dyn PartitionTree,
    journal: &dyn PartitionJournal,
) -> Result<RecoverySeed> {
    const WINDOW_BYTES: usize = 1024 * 1024;
    let tail = journal.tail();
    if checkpoint.replay_offset > tail {
        return Err(ChunkKvError::JournalCorruption(
            "checkpoint replay offset exceeds journal tail".into(),
        ));
    }
    let mut replay = ReplayState::new(checkpoint, retained_results);
    let mut read_offset = checkpoint.replay_offset;
    let mut frame_offset = read_offset;
    let mut buffered = BytesMut::new();
    while read_offset < tail {
        let bytes = journal.read_window(read_offset, WINDOW_BYTES).await?;
        if bytes.is_empty() {
            return Err(ChunkKvError::JournalCorruption(
                "journal returned no bytes before durable tail".into(),
            ));
        }
        read_offset = read_offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| ChunkKvError::JournalCorruption("journal replay offset overflows".into()))?;
        buffered.extend_from_slice(&bytes);
        loop {
            let decoded = decode_frame(&buffered)?;
            let FrameDecode::Complete(decoded) = decoded else {
                break;
            };
            validate_replay_record(partition_id, ownership_epoch, &decoded.record)?;
            replay.process(tree, frame_offset, decoded.record, true).await?;
            let consumed = decoded.bytes_consumed;
            buffered.advance(consumed);
            frame_offset = frame_offset
                .checked_add(consumed as u64)
                .ok_or_else(|| ChunkKvError::JournalCorruption("journal frame offset overflows".into()))?;
        }
        if buffered.len() > crate::MAX_FRAME_BYTES {
            return Err(ChunkKvError::JournalCorruption(
                "incomplete frame exceeds maximum size".into(),
            ));
        }
    }
    if !buffered.is_empty() {
        return Err(ChunkKvError::IncompleteFrame);
    }
    Ok(replay.seed)
}

impl ReplayState {
    fn new(checkpoint: &Checkpoint, retained_results: usize) -> Self {
        Self {
            seed: RecoverySeed {
                applied_seq: checkpoint.applied_seq,
                applied_position: checkpoint.replay_offset,
                retry_replay_offset: checkpoint.replay_offset,
                results: HashMap::new(),
                result_order: std::collections::VecDeque::new(),
                expired_floor: HashMap::new(),
                recovered: true,
            },
            checkpoint_applied_seq: checkpoint.applied_seq,
            stream_name: checkpoint.stream_name,
            retained_results,
            replayed: HashMap::new(),
            last_new_sequence: None,
        }
    }

    async fn process(
        &mut self,
        tree: &dyn PartitionTree,
        frame_offset: u64,
        record: WalRecord,
        belongs_to_partition: bool,
    ) -> Result<()> {
        if let Some(previous) = self.replayed.get(&record.mutation_seq) {
            if previous != &record {
                return Err(ChunkKvError::JournalCorruption(
                    "conflicting duplicate mutation sequence".into(),
                ));
            }
            return Ok(());
        }
        if self
            .last_new_sequence
            .is_some_and(|last| last.checked_add(1) != Some(record.mutation_seq))
        {
            return Err(ChunkKvError::JournalCorruption(
                "journal mutation sequence has a gap".into(),
            ));
        }
        if record.mutation_seq > self.checkpoint_applied_seq {
            let expected = self
                .seed
                .applied_seq
                .checked_add(1)
                .ok_or_else(|| ChunkKvError::JournalCorruption("mutation sequence overflows".into()))?;
            if record.mutation_seq != expected {
                return Err(ChunkKvError::JournalCorruption(
                    "journal mutation sequence has a gap".into(),
                ));
            }
            if belongs_to_partition && record.result.applied() {
                tree.apply(record.mutation_seq, &record.operation)
                    .await
                    .map_err(|error| {
                        ChunkKvError::Internal(format!(
                            "replay tree apply failed at mutation sequence {}: {error}",
                            record.mutation_seq
                        ))
                    })?;
            } else {
                tree.advance_noop(record.mutation_seq).await.map_err(|error| {
                    ChunkKvError::Internal(format!(
                        "replay tree no-op failed at mutation sequence {}: {error}",
                        record.mutation_seq
                    ))
                })?;
            }
            self.seed.applied_seq = record.mutation_seq;
        }
        self.seed.applied_position = frame_offset;
        if belongs_to_partition {
            let response = MutationResponse {
                mutation_seq: record.mutation_seq,
                result: record.result.clone(),
                journal_position: JournalPosition {
                    stream_name: self.stream_name,
                    offset: frame_offset,
                },
            };
            retain_recovered(
                &mut self.seed,
                self.retained_results,
                record.request_id,
                record.operation_digest,
                response,
            )?;
        }
        self.last_new_sequence = Some(record.mutation_seq);
        self.replayed.insert(record.mutation_seq, record);
        Ok(())
    }
}

fn validate_replay_record(partition_id: PartitionId, ownership_epoch: u64, record: &WalRecord) -> Result<()> {
    if record.partition_id != partition_id || record.ownership_epoch > ownership_epoch {
        return Err(ChunkKvError::JournalCorruption(
            "WAL record partition or epoch is invalid".into(),
        ));
    }
    if record.operation_digest != canonical_operation_digest(&record.operation) {
        return Err(ChunkKvError::JournalCorruption(
            "WAL operation digest mismatch".into(),
        ));
    }
    match record.result {
        MutationResult::Applied { revision } if revision != record.mutation_seq => Err(
            ChunkKvError::JournalCorruption("applied revision differs from mutation sequence".into()),
        ),
        _ => Ok(()),
    }
}

fn retain_recovered(
    seed: &mut RecoverySeed,
    retained_results: usize,
    request_id: RequestId,
    digest: [u8; 32],
    response: MutationResponse,
) -> Result<()> {
    let client = (request_id.client_high, request_id.client_low);
    if seed.results.contains_key(&request_id)
        || seed
            .expired_floor
            .get(&client)
            .is_some_and(|floor| request_id.client_sequence <= *floor)
    {
        return Err(ChunkKvError::JournalCorruption(
            "request identifier is reused by multiple mutations".into(),
        ));
    }
    seed.results
        .insert(request_id, RetainedResult { digest, response });
    seed.result_order.push_back(request_id);
    while seed.result_order.len() > retained_results {
        let Some(expired) = seed.result_order.pop_front() else {
            break;
        };
        seed.results.remove(&expired);
        seed.expired_floor
            .entry((expired.client_high, expired.client_low))
            .and_modify(|floor| *floor = (*floor).max(expired.client_sequence))
            .or_insert(expired.client_sequence);
    }
    if let Some(oldest) = seed
        .result_order
        .front()
        .and_then(|request_id| seed.results.get(request_id))
    {
        seed.retry_replay_offset = oldest.response.journal_position.offset;
    }
    Ok(())
}

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

async fn run_worker(mut state: WorkerState, mut receiver: mpsc::Receiver<MutationRequest>) {
    let mut pending = None;
    loop {
        let first = match pending.take() {
            Some(request) => request,
            None => match receiver.recv().await {
                Some(request) => request,
                None => break,
            },
        };
        let lifecycle = lifecycle_from_code(state.lifecycle.load(Ordering::Acquire));
        if !accepts_mutations(lifecycle) && lifecycle != PartitionLifecycle::SplitFinalizing {
            finish_request(&state, first, Err(write_state_error(lifecycle)));
            continue;
        }
        let mut requests = vec![first];
        let mut bytes = requests[0].reserved_bytes;
        while requests.len() < state.config.batch_requests && bytes < state.config.batch_bytes {
            let Ok(request) = receiver.try_recv() else {
                break;
            };
            if bytes
                .checked_add(request.reserved_bytes)
                .is_some_and(|total| total <= state.config.batch_bytes)
            {
                bytes += request.reserved_bytes;
                requests.push(request);
            } else {
                pending = Some(request);
                break;
            }
        }
        process_batch(&mut state, requests).await;
    }
}

async fn process_batch(state: &mut WorkerState, requests: Vec<MutationRequest>) {
    let prepared = prepare_batch(state, requests).await;
    append_and_apply(state, prepared).await;
}

async fn prepare_batch(state: &mut WorkerState, requests: Vec<MutationRequest>) -> Vec<PreparedMutation> {
    let mut prepared: Vec<PreparedMutation> = Vec::new();
    let mut local_ids: HashMap<RequestId, ([u8; 32], usize)> = HashMap::new();
    let mut overlay: HashMap<Vec<u8>, Option<ValueRevision>> = HashMap::new();

    for request in requests {
        if let Some(retained) = state.results.get(&request.request_id) {
            let result = if retained.digest == request.digest {
                Ok(retained.response.clone())
            } else {
                Err(ChunkKvError::RequestConflict)
            };
            finish_request(state, request, result);
            continue;
        }
        let client = (request.request_id.client_high, request.request_id.client_low);
        if state
            .expired_floor
            .get(&client)
            .is_some_and(|floor| request.request_id.client_sequence <= *floor)
        {
            finish_request(state, request, Err(ChunkKvError::RequestExpired));
            continue;
        }
        if let Some((digest, index)) = local_ids.get(&request.request_id).copied() {
            if digest == request.digest {
                prepared[index].followers.push(request);
            } else {
                finish_request(state, request, Err(ChunkKvError::RequestConflict));
            }
            continue;
        }
        let mutation_seq = state.next_seq;
        let Some(next_seq) = mutation_seq.checked_add(1) else {
            state
                .lifecycle
                .store(lifecycle_code(PartitionLifecycle::Faulted), Ordering::Release);
            finish_request(
                state,
                request,
                Err(ChunkKvError::Faulted("mutation sequence exhausted".into())),
            );
            continue;
        };
        let current = if let Some(value) = overlay.get(request.operation.key()) {
            value.clone()
        } else if operation_needs_current(&request.operation) {
            match state.tree.get(request.operation.key()).await {
                Ok(value) => value,
                Err(error) => {
                    finish_request(state, request, Err(error));
                    continue;
                }
            }
        } else {
            None
        };
        let result = resolve_condition(mutation_seq, &request.operation, current.as_ref());
        let record = WalRecord {
            partition_id: state.partition_id,
            ownership_epoch: state.ownership_epoch.load(Ordering::Acquire),
            mutation_seq,
            request_id: request.request_id,
            operation_digest: request.digest,
            result: result.clone(),
            operation: request.operation.clone(),
        };
        let frame = match encode_frame(&record) {
            Ok(frame) => Bytes::from(frame),
            Err(error) => {
                finish_request(state, request, Err(error));
                continue;
            }
        };
        if result.applied() {
            overlay.insert(
                request.operation.key().to_vec(),
                request.operation.successful_value().map(|value| ValueRevision {
                    revision: mutation_seq,
                    value: value.to_vec(),
                }),
            );
        }
        state.next_seq = next_seq;
        let index = prepared.len();
        local_ids.insert(request.request_id, (request.digest, index));
        prepared.push(PreparedMutation {
            request,
            response: MutationResponse {
                mutation_seq,
                result,
                journal_position: JournalPosition::default(),
            },
            record,
            frame,
            followers: Vec::new(),
        });
    }
    prepared
}

async fn append_and_apply(state: &mut WorkerState, prepared: Vec<PreparedMutation>) {
    if prepared.is_empty() {
        return;
    }
    let frames: Vec<Bytes> = prepared.iter().map(|entry| entry.frame.clone()).collect();
    let positions = match state.journal.append_frames(&frames).await {
        Ok(positions) if positions.len() == prepared.len() => positions,
        Ok(_) => {
            fail_prepared(
                state,
                prepared,
                &ChunkKvError::Internal("journal returned wrong position count".into()),
            );
            return;
        }
        Err(error) => {
            state.lifecycle.store(
                lifecycle_code(PartitionLifecycle::WriteStalled),
                Ordering::Release,
            );
            state.metrics.write_stall();
            fail_prepared(state, prepared, &error);
            return;
        }
    };
    if let Some(last) = prepared.last() {
        state
            .journal_durable_seq
            .store(last.record.mutation_seq, Ordering::Release);
    }
    let mut entries = prepared.into_iter().zip(positions);
    while let Some((mut entry, position)) = entries.next() {
        entry.response.journal_position = position;
        let apply = if entry.response.result.applied() {
            state
                .tree
                .apply(entry.record.mutation_seq, &entry.record.operation)
                .await
        } else {
            state.tree.advance_noop(entry.record.mutation_seq).await
        };
        if apply.is_err() {
            state
                .lifecycle
                .store(lifecycle_code(PartitionLifecycle::Recovering), Ordering::Release);
            state.metrics.apply_unknown();
            finish_request(state, entry.request, Err(ChunkKvError::ApplyStateUnknown));
            for follower in entry.followers {
                finish_request(state, follower, Err(ChunkKvError::ApplyStateUnknown));
            }
            for (remaining, _) in entries {
                finish_request(state, remaining.request, Err(ChunkKvError::ApplyStateUnknown));
                for follower in remaining.followers {
                    finish_request(state, follower, Err(ChunkKvError::ApplyStateUnknown));
                }
            }
            return;
        }
        state
            .applied_seq
            .store(entry.record.mutation_seq, Ordering::Release);
        state.applied_position.store(position.offset, Ordering::Release);
        state.applied_notify.notify_waiters();
        state.metrics.mutation_result(entry.response.result.applied());
        retain_result(
            state,
            entry.record.request_id,
            entry.record.operation_digest,
            entry.response.clone(),
        );
        finish_request(state, entry.request, Ok(entry.response.clone()));
        for follower in entry.followers {
            finish_request(state, follower, Ok(entry.response.clone()));
        }
    }
}

fn operation_needs_current(operation: &MutationOperation) -> bool {
    matches!(
        operation,
        MutationOperation::PutIfAbsent { .. }
            | MutationOperation::CompareExchange { .. }
            | MutationOperation::ConditionalDelete { .. }
    )
}

fn resolve_condition(
    mutation_seq: u64,
    operation: &MutationOperation,
    current: Option<&ValueRevision>,
) -> MutationResult {
    let matches = match operation {
        MutationOperation::Put { .. } | MutationOperation::Delete { .. } => true,
        MutationOperation::PutIfAbsent { .. } => current.is_none(),
        MutationOperation::CompareExchange { condition, .. }
        | MutationOperation::ConditionalDelete { condition, .. } => condition_matches(condition, current),
    };
    if matches {
        MutationResult::Applied {
            revision: mutation_seq,
        }
    } else {
        MutationResult::ConditionFailed {
            observed: current.cloned(),
        }
    }
}

fn condition_matches(condition: &CompareCondition, current: Option<&ValueRevision>) -> bool {
    match condition {
        CompareCondition::Revision(revision) => current.is_some_and(|value| value.revision == *revision),
        CompareCondition::Value(expected) => current.is_some_and(|value| value.value == *expected),
    }
}

fn retain_result(
    state: &mut WorkerState,
    request_id: RequestId,
    digest: [u8; 32],
    response: MutationResponse,
) {
    state
        .results
        .insert(request_id, RetainedResult { digest, response });
    state.result_order.push_back(request_id);
    while state.result_order.len() > state.config.retained_results {
        let Some(expired) = state.result_order.pop_front() else {
            break;
        };
        state.results.remove(&expired);
        state
            .expired_floor
            .entry((expired.client_high, expired.client_low))
            .and_modify(|floor| *floor = (*floor).max(expired.client_sequence))
            .or_insert(expired.client_sequence);
    }
    if let Some(oldest) = state
        .result_order
        .front()
        .and_then(|request_id| state.results.get(request_id))
    {
        state
            .retry_replay_offset
            .store(oldest.response.journal_position.offset, Ordering::Release);
    }
}

fn fail_prepared(state: &WorkerState, prepared: Vec<PreparedMutation>, error: &ChunkKvError) {
    for entry in prepared {
        finish_request(state, entry.request, Err(error.clone()));
        for follower in entry.followers {
            finish_request(state, follower, Err(error.clone()));
        }
    }
}

fn finish_request(state: &WorkerState, request: MutationRequest, result: Result<MutationResponse>) {
    release_admission(
        &state.queued_requests,
        &state.queued_bytes,
        &state.admission_notify,
        request.reserved_bytes,
    );
    let _ = request.completion.send(result);
}

fn estimated_request_bytes(operation: &MutationOperation) -> Result<u64> {
    let value = operation.successful_value().map_or(0, <[u8]>::len);
    let condition = match operation {
        MutationOperation::CompareExchange {
            condition: CompareCondition::Value(value),
            ..
        }
        | MutationOperation::ConditionalDelete {
            condition: CompareCondition::Value(value),
            ..
        } => value.len(),
        _ => 0,
    };
    operation
        .key()
        .len()
        .checked_add(value)
        .and_then(|bytes| bytes.checked_add(condition))
        .and_then(|bytes| bytes.checked_add(512))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| ChunkKvError::InvalidRequest("request memory estimate overflows".into()))
}

fn reserve_requests(counter: &AtomicUsize, limit: usize) -> Result<()> {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        if current >= limit {
            return Err(ChunkKvError::Overloaded);
        }
        match counter.compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

fn reserve_bytes(counter: &AtomicU64, limit: u64, bytes: u64) -> Result<()> {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let Some(next) = current.checked_add(bytes) else {
            return Err(ChunkKvError::Overloaded);
        };
        if next > limit {
            return Err(ChunkKvError::Overloaded);
        }
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

fn release_admission(requests: &AtomicUsize, bytes: &AtomicU64, notify: &Notify, reserved_bytes: u64) {
    if requests.fetch_sub(1, Ordering::AcqRel) == 1 {
        notify.notify_waiters();
    }
    bytes.fetch_sub(reserved_bytes, Ordering::AcqRel);
}

fn lifecycle_code(state: PartitionLifecycle) -> u8 {
    match state {
        PartitionLifecycle::Closed => 0,
        PartitionLifecycle::Recovering => 1,
        PartitionLifecycle::WriteStalled => 2,
        PartitionLifecycle::Prepared => 3,
        PartitionLifecycle::Serving => 4,
        PartitionLifecycle::SplitPreparing => 5,
        PartitionLifecycle::SplitFinalizing => 6,
        PartitionLifecycle::Retired => 7,
        PartitionLifecycle::Faulted => 8,
    }
}

fn accepts_mutations(state: PartitionLifecycle) -> bool {
    matches!(
        state,
        PartitionLifecycle::Serving | PartitionLifecycle::SplitPreparing
    )
}

fn lifecycle_from_code(code: u8) -> PartitionLifecycle {
    match code {
        0 => PartitionLifecycle::Closed,
        1 => PartitionLifecycle::Recovering,
        2 => PartitionLifecycle::WriteStalled,
        3 => PartitionLifecycle::Prepared,
        4 => PartitionLifecycle::Serving,
        5 => PartitionLifecycle::SplitPreparing,
        6 => PartitionLifecycle::SplitFinalizing,
        7 => PartitionLifecycle::Retired,
        _ => PartitionLifecycle::Faulted,
    }
}

fn write_state_error(state: PartitionLifecycle) -> ChunkKvError {
    match state {
        PartitionLifecycle::Recovering => ChunkKvError::Recovering,
        PartitionLifecycle::WriteStalled => ChunkKvError::WriteStalled,
        PartitionLifecycle::Faulted => ChunkKvError::Faulted("partition faulted".into()),
        _ => ChunkKvError::NotServing(format!("{state:?}")),
    }
}

fn read_state_error(state: PartitionLifecycle) -> ChunkKvError {
    match state {
        PartitionLifecycle::Recovering => ChunkKvError::Recovering,
        PartitionLifecycle::Faulted => ChunkKvError::Faulted("partition faulted".into()),
        _ => ChunkKvError::NotServing(format!("{state:?}")),
    }
}
