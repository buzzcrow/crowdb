// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! [`KVEngine`] implementation backed by the crowdb-tree C++ storage engine
//! (FFI adapter over the crowdb-tree C ABI, via `crowdb_tree_ffi`).

#[cfg(feature = "test-util")]
use super::op::Cell;
use super::{
    Batch, KVEngine, KVFuture, ScanDirection, SnapshotChunk, SnapshotExporter, SnapshotFormat,
    SnapshotImporter, SnapshotMetadata, SnapshotViewEntry,
};
use bytes::Bytes;
use crowdb_tree_ffi::{AsyncCrowdbtree, Crowdbtree, CtError, GetOutcome, OwnedGetOutcome, ScanOutcome};
use std::sync::Arc;

mod admission;
mod maintenance;
mod metrics;

pub use crowdb_tree_ffi::Config as CrowdbTreeConfig;
pub use crowdb_tree_ffi::PageStoreBackend as CrowdbTreeBackend;
pub use crowdb_tree_ffi::Stats as CrowdbTreeStats;

/// `KVEngine` backed by [`crowdb_tree_ffi::Crowdbtree`], via [`AsyncCrowdbtree`].
///
/// Runtime proposals acquire an owned native permit before consensus chooses
/// a slot. The permit's apply future awaits prepaid native continuations.
/// Reads dispatch through the shared native runtime and may complete on the
/// first poll or await CPU/backend progress. The
/// compatibility `apply` entry remains available for engines without permits.
///
/// Test iteration scans L0 and L1 with tombstones included, so successful
/// writes are visible without an explicit `flush()`, matching `InMemKV`.
pub struct CrowdbTreeEngine {
    // Stop the collector before closing the native Tree.
    work_collector: Option<crowdb_tree_ffi::TreeWorkCollector>,
    work_window: arc_swap::ArcSwapOption<crowdb_tree_ffi::TreeWorkSnapshot>,
    inner: AsyncCrowdbtree,
    has_backend: bool,
    maintenance_pending: Arc<std::sync::atomic::AtomicU64>,
}

impl CrowdbTreeEngine {
    /// Open (recovering durable state when `opt.path` is set, else fresh
    /// in-memory). See [`crowdb_tree_ffi::Crowdbtree::open`].
    ///
    /// # Errors
    /// Returns the underlying [`CtError`] if the engine fails to open (e.g. a
    /// corrupt or unreadable durable file).
    pub fn open(opt: &CrowdbTreeConfig) -> Result<Self, CtError> {
        let inner = AsyncCrowdbtree::open(opt)?;
        let work_collector = Some(inner.handle().work_metrics_collector()?);
        Ok(Self {
            work_collector,
            work_window: arc_swap::ArcSwapOption::empty(),
            inner,
            has_backend: opt.page_store.is_some() || opt.path.as_deref().is_some_and(|path| !path.is_empty()),
            maintenance_pending: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Borrow the underlying FFI handle for engine-specific operations
    /// (`flush`/`snapshot`/snapshot sessions/GC watermark)
    /// that aren't part of the [`KVEngine`] trait surface. Cheap: an `Arc`
    /// clone, sharing the same tree `get`/`scan`/`apply` above operate on.
    #[must_use]
    pub fn handle(&self) -> Arc<Crowdbtree> {
        self.inner.handle()
    }

    /// Compatibility diagnostics from Core publication and cache counters.
    /// Sampling does not scan pool frames; historical field names remain for
    /// callers of this schema. Use `handle().state_stats()` and the shared work
    /// metrics window for the full runtime statistics.
    #[must_use]
    pub fn stats(&self) -> CrowdbTreeStats {
        self.inner.handle().stats()
    }

    /// Flush C++ metrics into a formatted string for the `cpp-tree`
    /// log section. Delegates to `crowdb_tree_ffi::Crowdbtree::flush_metrics_str`.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn flush_metrics_str(&self, window_secs: f64, timestamp: &str, width: usize) -> String {
        self.inner
            .handle()
            .flush_metrics_str(window_secs, timestamp, width)
    }

    /// Extended flush with negotiated column widths.
    #[must_use]
    pub fn flush_metrics_str_ext(
        &self,
        window_secs: f64,
        timestamp: &str,
        width: usize,
        count_w: usize,
        tps_w: usize,
    ) -> String {
        self.inner
            .handle()
            .flush_metrics_str_ext(window_secs, timestamp, width, count_w, tps_w)
    }

    /// Negotiate column widths with C++. Returns (`count_w`, `tps_w`).
    #[must_use]
    pub fn negotiate_widths(&self, rust_count_w: usize, rust_tps_w: usize) -> (usize, usize) {
        self.inner.handle().negotiate_widths(rust_count_w, rust_tps_w)
    }

    /// Current max metric name length from the C++ registry.
    #[must_use]
    pub fn max_name_len(&self) -> usize {
        self.inner.handle().max_name_len()
    }

    /// Full ordered stream including tombstones. Test-only utility.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn iter_all_for_tests(&self) -> Vec<(Vec<u8>, u64, Cell)> {
        // Scan the complete L0+L1 keyspace, including tombstones. No flush
        // is needed for visibility. The test-only Cell::Value(Vec<u8>) shape
        // copies the returned shared Bytes.
        match self.inner.handle().scan(b"", b"", b"", 0, 0, false, 0, true) {
            Ok((entries, _)) => entries
                .into_iter()
                .map(|e| {
                    let cell = if e.tombstone {
                        Cell::Tombstone
                    } else {
                        Cell::Value(e.value.to_vec())
                    };
                    (e.key.to_vec(), e.slot, cell)
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

impl KVEngine for CrowdbTreeEngine {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn requires_contiguous_apply(&self) -> bool {
        true
    }

    fn admit_proposal(&self, payload: Bytes) -> KVFuture<super::ApplyAdmissionResult> {
        KVFuture::ready(admission::prepare(&self.inner.handle(), &payload))
    }

    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>> {
        admission::replay(self.inner.handle(), slot, batch)
    }

    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>> {
        // Use the first native completion poll when ready. Resident reads
        // can also await shared CPU dispatch; native state/result allocations
        // remain charged in either case.
        match self.inner.try_get(key) {
            GetOutcome::Ready(result) => KVFuture::ready(result.ok().flatten()),
            GetOutcome::Pending(fut) => KVFuture::Pending(Box::pin(async move { fut.await.ok().flatten() })),
        }
    }

    fn get_bytes(&self, key: &[u8]) -> KVFuture<Option<(u64, Bytes)>> {
        match self.inner.try_get_owned(key) {
            OwnedGetOutcome::Ready(result) => KVFuture::ready(result.ok().flatten()),
            OwnedGetOutcome::Pending(future) => {
                KVFuture::Pending(Box::pin(async move { future.await.ok().flatten() }))
            }
        }
    }

    fn get_versioned(&self, key: &[u8]) -> KVFuture<Result<Option<(u64, Bytes)>, String>> {
        match self.inner.try_get_owned(key) {
            OwnedGetOutcome::Ready(result) => KVFuture::ready(result.map_err(|error| error.to_string())),
            OwnedGetOutcome::Pending(future) => KVFuture::Pending(Box::pin(async move {
                future.await.map_err(|error| error.to_string())
            })),
        }
    }

    fn scan_directional(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
        direction: ScanDirection,
    ) -> KVFuture<Result<(Vec<(Bytes, u64, Bytes)>, bool), String>> {
        // Native traversal applies the direction, cursor, end, count and
        // byte/deadline limits across bounded internal windows. Keys-only
        // scans skip overflow value assembly. Non-strict byte limits allow
        // one oversized first record when resource admission permits it.
        // ScanEntry retains shared slices of the charged packed buffer;
        // decode_scan re-packages them and propagates native errors.
        let prefix_owned = prefix.to_vec();
        let start_after_owned = start_after.to_vec();
        let end_key_owned = end_key.to_vec();

        let ffi_direction = match direction {
            ScanDirection::Forward => crowdb_tree_ffi::ScanDirection::Forward,
            ScanDirection::Reverse => crowdb_tree_ffi::ScanDirection::Reverse,
        };
        match self.inner.try_scan_directional(
            prefix_owned,
            start_after_owned,
            end_key_owned,
            limit,
            byte_budget,
            keys_only,
            deadline_ms,
            ffi_direction,
        ) {
            ScanOutcome::Ready(result) => KVFuture::ready(decode_scan(result)),
            ScanOutcome::Pending(fut) => KVFuture::Pending(Box::pin(async move { decode_scan(fut.await) })),
        }
    }

    fn clear(&self) -> Result<(), String> {
        self.inner.handle().clear().map_err(|error| error.to_string())
    }

    fn is_healthy(&self) -> bool {
        // `io_failed` is a latched flag set when a demand-load hit an I/O
        // error or CRC mismatch on a committed page; it stays set until an
        // explicit `clear_io_error`, which nothing in this codebase calls
        // yet -- so once tripped, this stays `false` for the engine's
        // lifetime, which is the intended "fail out, don't silently retry"
        // semantics for a durable-storage fault.
        !self.inner.handle().io_failed()
    }

    fn flush(&self) {
        // Gap 3: log flush errors instead of silently swallowing. flush()
        // is in-memory (L0→L1 drain) so it can't hit I/O errors, but
        // internal tree errors (corruption, assertion failures) should be
        // visible. The `io_failed` flag is set by the C++ engine on
        // demand-load faults, not by flush — `is_healthy()` catches that.
        if let Err(e) = self.inner.handle().flush() {
            tracing::error!("flush failed: {:?}", e);
        }
    }

    fn flush_async(&self) -> Option<KVFuture<Result<super::EngineFlushReport, String>>> {
        Some(self.async_flush())
    }

    fn writeback_pending(&self) -> bool {
        self.has_backend
            && self
                .inner
                .handle()
                .state_stats()
                .is_ok_and(|state| state.logical.writeback_candidate_pages != 0)
    }

    fn writeback_async(&self) -> Option<KVFuture<Result<super::EngineCheckpointReport, String>>> {
        Some(self.async_checkpoint(false))
    }

    fn persist_snapshot_async(&self) -> Option<KVFuture<Result<super::EngineCheckpointReport, String>>> {
        Some(self.async_checkpoint(true))
    }

    fn apply_noop(&self, slot: u64) -> KVFuture<Result<(), String>> {
        admission::replay_empty(self.inner.handle(), slot)
    }

    fn noop(&self, slot: u64) {
        // Record this slot without declaring earlier, still-in-flight slots
        // applied. Forced advancement is reserved for recovery initialization.
        self.inner
            .handle()
            .apply_batch(slot, &[])
            .expect("empty NoOp batch uses a valid tree handle");
    }

    fn flush_pending(&self) -> bool {
        self.maintenance_pending
            .load(std::sync::atomic::Ordering::Acquire)
            & 1
            != 0
            || self.inner.handle().frozen_table_count() > 0
    }

    fn resume_from_slot(&self) -> u64 {
        // Recovery/WAL retention must use the committed checkpoint frontier.
        // Publication can advance in memory without any durable backend.
        self.inner.handle().durable_coverage()
    }

    fn persist_snapshot(&self) -> u64 {
        // Synchronous compatibility entry: wait for the native checkpoint,
        // which captures a finite handoff and commits its durable frontier.
        // Native async maintenance uses checkpoint_report instead. A prior
        // flush may advance publication but does not establish durability.
        let snap_t0 = std::time::Instant::now();
        let snap_result = self.inner.handle().snapshot();
        let snap_ms = snap_t0.elapsed().as_millis();
        match &snap_result {
            Ok(slot) => tracing::info!("persist_snapshot: snapshot_ms={} snapshot_slot={}", snap_ms, slot),
            Err(e) => tracing::error!("persist_snapshot FAILED: snapshot_ms={} err={:?}", snap_ms, e),
        }
        snap_result.unwrap_or(0)
    }

    fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64) {
        self.inner.handle().set_gc_watermark(snapshot_slot, safe_slot);
    }

    fn compact_sparse_blocks_async(&self) -> Option<KVFuture<Result<(), String>>> {
        let tree = self.inner.clone();
        Some(KVFuture::Pending(Box::pin(async move {
            tree.compact_sparse_blocks()
                .await
                .map_err(|error| error.to_string())?
                .status
                .map_err(|error| error.to_string())
        })))
    }

    fn compact_sparse_blocks(&self) -> Result<(), String> {
        self.inner
            .handle()
            .compact_sparse_blocks()
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn snapshot_export_begin(&self, chunk_bytes: usize) -> Result<Box<dyn SnapshotExporter>, String> {
        self.inner.handle().flush().map_err(|error| error.to_string())?;
        let session = self
            .inner
            .handle()
            .snapshot_export_begin(chunk_bytes)
            .map_err(|error| error.to_string())?;
        Ok(Box::new(CrowdbTreeSnapshotExporter { session }))
    }

    fn snapshot_import_begin(&self) -> Result<Box<dyn SnapshotImporter>, String> {
        let session = self
            .inner
            .handle()
            .snapshot_import_begin()
            .map_err(|error| error.to_string())?;
        Ok(Box::new(CrowdbTreeSnapshotImporter { session }))
    }

    fn snapshot_view(&self) -> Result<(u64, Vec<SnapshotViewEntry>), String> {
        // Flush first so the view reflects every `apply` up to now, not
        // just whatever an earlier `flush` already moved into L1.
        self.inner
            .handle()
            .flush()
            .map_err(|error| format!("flush before snapshot_view: {error:?}"))?;
        let (at_slot, entries) = self
            .inner
            .handle()
            .snapshot_view()
            .map_err(|e| format!("snapshot_view: {e:?}"))?;
        let out = entries
            .into_iter()
            .map(|e| SnapshotViewEntry {
                key: e.key,
                slot: e.slot,
                tombstone: e.tombstone,
                value: e.value,
            })
            .collect();
        Ok((at_slot, out))
    }

    // `compare` uses the trait's default implementation (diffs `iter_all`
    // of both sides); no override needed.
}

struct CrowdbTreeSnapshotExporter {
    session: crowdb_tree_ffi::SnapshotExportSession,
}

impl SnapshotExporter for CrowdbTreeSnapshotExporter {
    fn metadata(&self) -> SnapshotMetadata {
        let metadata = self.session.metadata();
        SnapshotMetadata {
            format: SnapshotFormat::CrowdbTreePortable,
            at_slot: metadata.at_slot,
            total_bytes: metadata.total_bytes,
            final_crc32c: metadata.final_crc32c,
            chunk_bytes: metadata.chunk_bytes,
        }
    }

    fn offset(&self) -> u64 {
        self.session.offset()
    }

    fn read(&mut self, offset: u64) -> Result<SnapshotChunk, String> {
        self.session
            .read_owned(offset)
            .map(|chunk| SnapshotChunk {
                offset: chunk.offset,
                bytes: chunk.bytes,
                done: chunk.done,
            })
            .map_err(|error| error.to_string())
    }
}

struct CrowdbTreeSnapshotImporter {
    session: crowdb_tree_ffi::SnapshotImportSession,
}

impl SnapshotImporter for CrowdbTreeSnapshotImporter {
    fn feed(&mut self, chunk: &[u8]) -> Result<(), String> {
        self.session.feed(chunk).map_err(|error| error.to_string())
    }

    fn finish(self: Box<Self>) -> Result<u64, String> {
        self.session.finish().map_err(|error| error.to_string())
    }

    fn abort(self: Box<Self>) {
        self.session.abort();
    }
}

/// Shared tail of [`CrowdbTreeEngine::scan`]'s `Ready`/`Pending` arms:
/// converts a raw `crowdb_tree_ffi::ScanEntry` result (keys/values are
/// already zero-copy `Bytes` slices into the packed buffer) into the
/// `KVEngine::scan` return shape. The C++ engine already applied both the
/// `start_after` exclusive lower bound and the `limit`, so the truncated
/// flag is directly trustworthy. Errors propagate as `Err` instead of
/// being silently swallowed as an empty `ok` result.
type ScanResult = Result<(Vec<(Bytes, u64, Bytes)>, bool), String>;

fn decode_scan(result: Result<(Vec<crowdb_tree_ffi::ScanEntry>, bool), CtError>) -> ScanResult {
    match result {
        Ok((entries, truncated)) => {
            let items: Vec<(Bytes, u64, Bytes)> =
                entries.into_iter().map(|e| (e.key, e.slot, e.value)).collect();
            Ok((items, truncated))
        }
        Err(e) => Err(e.to_string()),
    }
}
