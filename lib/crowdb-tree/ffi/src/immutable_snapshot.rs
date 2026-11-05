// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Independently owned immutable runtime view. No second writer is opened.
use crate::error::check;
use crate::reactor::{try_poll_ct_future, FutureGuard, FutureKind};
use crate::{sys, Config, Crowdbtree, CtError, RangeRebuildStats, ScanDirection, ScanEntry, ScanOptions};
use std::ptr::NonNull;

#[derive(Debug)]
pub struct ImmutableTreeSnapshot {
    handle: NonNull<sys::ct_immutable_snapshot>,
    sequence: u64,
    coverage: u64,
}

// SAFETY: the native handle owns an immutable mapping; each read has independent
// paid operation state and backend ownership. Destruction follows host ownership.
unsafe impl Send for ImmutableTreeSnapshot {}
unsafe impl Sync for ImmutableTreeSnapshot {}

impl Drop for ImmutableTreeSnapshot {
    fn drop(&mut self) {
        unsafe { sys::ct_immutable_snapshot_free(self.handle.as_ptr()) };
    }
}

impl Crowdbtree {
    /// Capture exactly the expected completed L1 coverage. A nonzero sequence
    /// also requires the current durable checkpoint pair to match.
    /// # Errors
    /// Rejects changed coverage, unsupported backends, or unavailable budget.
    pub fn immutable_snapshot(&self, sequence: u64, coverage: u64) -> Result<ImmutableTreeSnapshot, CtError> {
        let mut raw = std::ptr::null_mut();
        check(unsafe { sys::ct_immutable_snapshot_capture(self.as_ptr(), sequence, coverage, &mut raw) })?;
        Ok(ImmutableTreeSnapshot {
            handle: NonNull::new(raw).ok_or(CtError::Internal)?,
            sequence,
            coverage,
        })
    }
}

impl ImmutableTreeSnapshot {
    #[must_use]
    pub const fn checkpoint_state(&self) -> (u64, u64) {
        (self.sequence, self.coverage)
    }

    /// Read a key from the fixed view. The copying result is application-owned.
    /// # Errors
    /// Returns a typed read or admission failure.
    pub fn get(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let mut raw = std::ptr::null_mut();
        check(unsafe {
            sys::ct_immutable_snapshot_get(self.handle.as_ptr(), key.as_ptr(), key.len(), &mut raw)
        })?;
        let result =
            try_poll_ct_future(&mut FutureGuard(raw), FutureKind::Get).ok_or(CtError::Internal)??;
        Ok(result.found.then_some((result.slot, result.value)))
    }

    /// Read one bounded window with independently owned result bytes.
    /// # Errors
    /// Returns invalid boundaries, read failures, or insufficient budget.
    pub fn scan(&self, options: &ScanOptions) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let start = options.start.as_deref().unwrap_or_default();
        let raw = sys::ct_scan_options_v2 {
            struct_size: std::mem::size_of::<sys::ct_scan_options_v2>(),
            prefix: options.prefix.as_ptr(),
            prefix_len: options.prefix.len(),
            start: start.as_ptr(),
            start_len: start.len(),
            end: options.end.as_ptr(),
            end_len: options.end.len(),
            begin: options.begin.as_ptr(),
            begin_len: options.begin.len(),
            limit: options.limit,
            byte_budget: options.byte_budget,
            deadline_ms: options.deadline_ms,
            reverse: i32::from(options.direction == ScanDirection::Reverse),
            keys_only: i32::from(options.keys_only),
            include_tombstones: i32::from(options.include_tombstones),
            has_start_bound: i32::from(options.start.is_some()),
            start_inclusive: i32::from(options.start_inclusive),
            strict_byte_budget: i32::from(options.strict_byte_budget),
        };
        let mut result = std::ptr::null_mut();
        check(unsafe { sys::ct_immutable_snapshot_scan(self.handle.as_ptr(), &raw, &mut result) })?;
        let (packed, count, truncated) = crate::scan::owner::take(result)?;
        Ok((crate::scan::decode_scan_bytes(packed, count)?, truncated))
    }

    /// Creates a separate runtime destination/account and rebuilds only its
    /// configured range. Captured pages remain valid after the source closes.
    /// # Errors
    /// Returns destination admission, read, import, or checkpoint failures.
    pub fn rebuild_range(&self, options: &Config) -> Result<(Crowdbtree, RangeRebuildStats), CtError> {
        if options.page_store.is_none() {
            return Err(CtError::InvalidArgument);
        }
        let destination = Crowdbtree::open(options)?;
        let mut stats = sys::ct_range_rebuild_stats::default();
        check(unsafe {
            sys::ct_immutable_snapshot_rebuild(self.handle.as_ptr(), destination.as_ptr(), &mut stats)
        })?;
        Ok((
            destination,
            RangeRebuildStats {
                entries_examined: stats.entries_examined,
                entries_emitted: stats.entries_emitted,
                entries_filtered: stats.entries_filtered,
                pages_reused: stats.pages_reused,
                pages_rebuilt: stats.pages_rebuilt,
                subtrees_skipped: stats.subtrees_skipped,
            },
        ))
    }
}
