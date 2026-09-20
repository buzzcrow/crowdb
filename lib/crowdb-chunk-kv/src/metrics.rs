// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct PartitionMetrics {
    point_reads: AtomicU64,
    forward_seeks: AtomicU64,
    reverse_seeks: AtomicU64,
    forward_scans: AtomicU64,
    reverse_scans: AtomicU64,
    scan_entries: AtomicU64,
    mutation_requests: AtomicU64,
    mutation_applied: AtomicU64,
    condition_failed: AtomicU64,
    range_rejects: AtomicU64,
    stale_epochs: AtomicU64,
    admission_backpressure: AtomicU64,
    write_stalls: AtomicU64,
    apply_unknown: AtomicU64,
    recoveries: AtomicU64,
    checkpoints: AtomicU64,
    reclaimed_journal_bytes: AtomicU64,
    reclaimed_tree_bytes: AtomicU64,
    reclaimed_orphan_bytes: AtomicU64,
    reclaimed_stream_metadata_pages: AtomicU64,
    split_begins: AtomicU64,
    split_entries_examined: AtomicU64,
    split_entries_emitted: AtomicU64,
    split_pages_reused: AtomicU64,
    split_pages_rebuilt: AtomicU64,
    split_delta_records: AtomicU64,
    split_tail_bytes: AtomicU64,
    split_catchup_lag_records: AtomicU64,
    split_preparation_duration_us: AtomicU64,
    split_base_checkpoint_duration_us: AtomicU64,
    split_finalization_duration_us: AtomicU64,
    split_overlay_apply_records: AtomicU64,
    split_overlay_apply_bytes: AtomicU64,
    split_finalizations: AtomicU64,
    split_commits: AtomicU64,
    split_aborts: AtomicU64,
    materialization_passes: AtomicU64,
    materialization_bytes: AtomicU64,
    materialization_failures: AtomicU64,
    materialization_duration_us: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartitionMetricsSnapshot {
    pub point_reads: u64,
    pub forward_seeks: u64,
    pub reverse_seeks: u64,
    pub forward_scans: u64,
    pub reverse_scans: u64,
    pub scan_entries: u64,
    pub mutation_requests: u64,
    pub mutation_applied: u64,
    pub condition_failed: u64,
    pub range_rejects: u64,
    pub stale_epochs: u64,
    pub admission_backpressure: u64,
    pub write_stalls: u64,
    pub apply_unknown: u64,
    pub recoveries: u64,
    pub checkpoints: u64,
    pub reclaimed_journal_bytes: u64,
    pub reclaimed_tree_bytes: u64,
    pub reclaimed_orphan_bytes: u64,
    pub reclaimed_stream_metadata_pages: u64,
    pub split_begins: u64,
    pub split_entries_examined: u64,
    pub split_entries_emitted: u64,
    pub split_pages_reused: u64,
    pub split_pages_rebuilt: u64,
    pub split_delta_records: u64,
    pub split_tail_bytes: u64,
    pub split_catchup_lag_records: u64,
    pub split_preparation_duration_us: u64,
    pub split_base_checkpoint_duration_us: u64,
    pub split_finalization_duration_us: u64,
    pub split_overlay_apply_records: u64,
    pub split_overlay_apply_bytes: u64,
    pub split_finalizations: u64,
    pub split_commits: u64,
    pub split_aborts: u64,
    pub materialization_passes: u64,
    pub materialization_bytes: u64,
    pub materialization_failures: u64,
    pub materialization_duration_us: u64,
}

impl PartitionMetrics {
    #[must_use]
    pub fn snapshot(&self) -> PartitionMetricsSnapshot {
        PartitionMetricsSnapshot {
            point_reads: self.point_reads.load(Ordering::Relaxed),
            forward_seeks: self.forward_seeks.load(Ordering::Relaxed),
            reverse_seeks: self.reverse_seeks.load(Ordering::Relaxed),
            forward_scans: self.forward_scans.load(Ordering::Relaxed),
            reverse_scans: self.reverse_scans.load(Ordering::Relaxed),
            scan_entries: self.scan_entries.load(Ordering::Relaxed),
            mutation_requests: self.mutation_requests.load(Ordering::Relaxed),
            mutation_applied: self.mutation_applied.load(Ordering::Relaxed),
            condition_failed: self.condition_failed.load(Ordering::Relaxed),
            range_rejects: self.range_rejects.load(Ordering::Relaxed),
            stale_epochs: self.stale_epochs.load(Ordering::Relaxed),
            admission_backpressure: self.admission_backpressure.load(Ordering::Relaxed),
            write_stalls: self.write_stalls.load(Ordering::Relaxed),
            apply_unknown: self.apply_unknown.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            checkpoints: self.checkpoints.load(Ordering::Relaxed),
            reclaimed_journal_bytes: self.reclaimed_journal_bytes.load(Ordering::Relaxed),
            reclaimed_tree_bytes: self.reclaimed_tree_bytes.load(Ordering::Relaxed),
            reclaimed_orphan_bytes: self.reclaimed_orphan_bytes.load(Ordering::Relaxed),
            reclaimed_stream_metadata_pages: self.reclaimed_stream_metadata_pages.load(Ordering::Relaxed),
            split_begins: self.split_begins.load(Ordering::Relaxed),
            split_entries_examined: self.split_entries_examined.load(Ordering::Relaxed),
            split_entries_emitted: self.split_entries_emitted.load(Ordering::Relaxed),
            split_pages_reused: self.split_pages_reused.load(Ordering::Relaxed),
            split_pages_rebuilt: self.split_pages_rebuilt.load(Ordering::Relaxed),
            split_delta_records: self.split_delta_records.load(Ordering::Relaxed),
            split_tail_bytes: self.split_tail_bytes.load(Ordering::Relaxed),
            split_catchup_lag_records: self.split_catchup_lag_records.load(Ordering::Relaxed),
            split_preparation_duration_us: self.split_preparation_duration_us.load(Ordering::Relaxed),
            split_base_checkpoint_duration_us: self.split_base_checkpoint_duration_us.load(Ordering::Relaxed),
            split_finalization_duration_us: self.split_finalization_duration_us.load(Ordering::Relaxed),
            split_overlay_apply_records: self.split_overlay_apply_records.load(Ordering::Relaxed),
            split_overlay_apply_bytes: self.split_overlay_apply_bytes.load(Ordering::Relaxed),
            split_finalizations: self.split_finalizations.load(Ordering::Relaxed),
            split_commits: self.split_commits.load(Ordering::Relaxed),
            split_aborts: self.split_aborts.load(Ordering::Relaxed),
            materialization_passes: self.materialization_passes.load(Ordering::Relaxed),
            materialization_bytes: self.materialization_bytes.load(Ordering::Relaxed),
            materialization_failures: self.materialization_failures.load(Ordering::Relaxed),
            materialization_duration_us: self.materialization_duration_us.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn point_read(&self) {
        self.point_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn forward_seek(&self) {
        self.forward_seeks.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reverse_seek(&self) {
        self.reverse_seeks.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn forward_scan(&self, entries: usize) {
        self.forward_scans.fetch_add(1, Ordering::Relaxed);
        self.scan_entries
            .fetch_add(u64::try_from(entries).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub(crate) fn reverse_scan(&self, entries: usize) {
        self.reverse_scans.fetch_add(1, Ordering::Relaxed);
        self.scan_entries
            .fetch_add(u64::try_from(entries).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub(crate) fn mutation_request(&self) {
        self.mutation_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mutation_result(&self, applied: bool) {
        let counter = if applied {
            &self.mutation_applied
        } else {
            &self.condition_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn range_reject(&self) {
        self.range_rejects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn stale_epoch(&self) {
        self.stale_epochs.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn admission_backpressure(&self) {
        self.admission_backpressure.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn write_stall(&self) {
        self.write_stalls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn apply_unknown(&self) {
        self.apply_unknown.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn recovery(&self) {
        self.recoveries.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn checkpoint(&self) {
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reclaim(&self, journal: u64, metadata_pages: u64, tree: u64, orphans: u64) {
        self.reclaimed_journal_bytes.fetch_add(journal, Ordering::Relaxed);
        self.reclaimed_stream_metadata_pages
            .fetch_add(metadata_pages, Ordering::Relaxed);
        self.reclaimed_tree_bytes.fetch_add(tree, Ordering::Relaxed);
        self.reclaimed_orphan_bytes.fetch_add(orphans, Ordering::Relaxed);
    }

    pub(crate) fn split_begin(&self) {
        self.split_begins.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn split_rebuild(&self, child: crowdb_tree_ffi::RangeRebuildStats) {
        self.split_entries_examined
            .fetch_add(child.entries_examined, Ordering::Relaxed);
        self.split_entries_emitted
            .fetch_add(child.entries_emitted, Ordering::Relaxed);
        self.split_pages_reused
            .fetch_add(child.pages_reused, Ordering::Relaxed);
        self.split_pages_rebuilt
            .fetch_add(child.pages_rebuilt, Ordering::Relaxed);
    }

    pub(crate) fn split_catchup(&self, delta_records: u64, tail_bytes: u64, catchup_lag_records: u64) {
        self.split_delta_records
            .fetch_add(delta_records, Ordering::Relaxed);
        self.split_tail_bytes.fetch_add(tail_bytes, Ordering::Relaxed);
        self.split_catchup_lag_records
            .store(catchup_lag_records, Ordering::Relaxed);
    }

    pub(crate) fn split_preparation_duration(&self, duration_us: u64) {
        self.split_preparation_duration_us
            .fetch_max(duration_us, Ordering::Relaxed);
    }

    pub(crate) fn split_base_checkpoint_duration(&self, duration_us: u64) {
        self.split_base_checkpoint_duration_us
            .fetch_max(duration_us, Ordering::Relaxed);
    }

    pub(crate) fn split_overlay_apply(&self, records: u64, bytes: u64) {
        self.split_overlay_apply_records
            .fetch_add(records, Ordering::Relaxed);
        self.split_overlay_apply_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn split_finalization(&self) {
        self.split_finalizations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn split_finalization_duration(&self, duration_us: u64) {
        self.split_finalization_duration_us
            .fetch_max(duration_us, Ordering::Relaxed);
    }

    pub(crate) fn split_commit(&self) {
        self.split_commits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn split_abort(&self) {
        self.split_aborts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn materialization(&self, result: Result<(u64, bool), ()>, duration_us: u64) {
        self.materialization_passes.fetch_add(1, Ordering::Relaxed);
        self.materialization_duration_us
            .fetch_add(duration_us, Ordering::Relaxed);
        match result {
            Ok((bytes, _)) => {
                self.materialization_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            Err(()) => {
                self.materialization_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
