// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::CtError;
use crate::reactor::{
    drive_ct_future, try_poll_ct_future, try_poll_ct_future_pinned, FutureGuard, FutureKind, PinnedValue,
};
use crate::scan::{ScanDirection, ScanEntry, ScanOptions};
use crate::sys;
use crate::tree::Crowdbtree;
use crate::Config;

mod compaction;
pub use compaction::{CompactionOptions, CompactionReport};
mod chunk_collection;
mod maintenance;
mod owned_get;
pub use chunk_collection::ChunkCollectionReport;
pub use owned_get::OwnedGetOutcome;
mod scan;

/// Async facade over native operation futures. Runtime Trees execute CPU work
/// on their shared native pool; [`drive_ct_future`] polls completion and the
/// backend reactor without blocking the Rust executor. Clones share one Tree.
#[derive(Clone, Debug)]
pub struct AsyncCrowdbtree {
    inner: Arc<Crowdbtree>,
}

impl AsyncCrowdbtree {
    pub fn open(opt: &Config) -> Result<Self, CtError> {
        Ok(Self {
            inner: Arc::new(Crowdbtree::open(opt)?),
        })
    }

    /// Share an existing Tree without opening another writer or storage domain.
    #[must_use]
    pub fn from_handle(inner: Arc<Crowdbtree>) -> Self {
        Self { inner }
    }

    pub fn from_sync(tree: Crowdbtree) -> Self {
        Self {
            inner: Arc::new(tree),
        }
    }

    pub fn from_shared(tree: Arc<Crowdbtree>) -> Self {
        Self { inner: tree }
    }

    pub fn handle(&self) -> Arc<Crowdbtree> {
        Arc::clone(&self.inner)
    }

    /// Await the native flush completion. The native worker waits for the
    /// captured MemTable writers without blocking this executor thread.
    pub async fn flush(&self) -> Result<(), CtError> {
        self.flush_report().await?.status
    }

    /// Awaits the captured checkpoint, including its backend completion.
    pub async fn snapshot(&self) -> Result<u64, CtError> {
        let report = self.checkpoint_report().await?;
        report.status?;
        Ok(report.durable_coverage)
    }

    /// Awaits a native point read dispatched through the shared CPU pool.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let out = drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Get).await?;
        Ok(out.found.then_some((out.slot, out.value)))
    }

    /// Returns a result immediately if the first native poll completes;
    /// otherwise boxes a pending future. Native operation and result storage
    /// still allocate; runtime operations charge their admitted storage.
    pub fn try_get(&self, key: &[u8]) -> GetOutcome {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let mut guard = FutureGuard(fut);
        if let Some(result) = try_poll_ct_future(&mut guard, FutureKind::Get) {
            return GetOutcome::Ready(result.map(|out| out.found.then_some((out.slot, out.value))));
        }
        let tree = self.inner.clone();
        GetOutcome::Pending(Box::pin(async move {
            let out = drive_ct_future(guard, &tree, FutureKind::Get).await?;
            Ok(out.found.then_some((out.slot, out.value)))
        }))
    }

    /// Like [`Self::try_get`] but the fast path returns a [`PinnedValue`]
    /// borrowing directly from the C++ engine's internal buffer (no
    /// `copy_buf` allocation). The slow path is identical to
    /// [`Self::try_get`]'s (`PinnedGetOutcome::Pending` resolves to an
    /// owned `Vec<u8>`).
    pub fn try_get_pinned(&self, key: &[u8]) -> PinnedGetOutcome {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let mut guard = FutureGuard(fut);
        if let Some(result) = try_poll_ct_future_pinned(&mut guard) {
            return PinnedGetOutcome::Ready(result);
        }
        let tree = self.inner.clone();
        PinnedGetOutcome::Pending(Box::pin(async move {
            let out = drive_ct_future(guard, &tree, FutureKind::Get).await?;
            Ok(out.found.then_some((out.slot, out.value)))
        }))
    }

    /// Async twin of [`Crowdbtree::scan`]. Runtime scans retain one captured
    /// publication across internal windows and keep the native allocation
    /// owner in every returned `Bytes` slice. Resume truncated output from
    /// its last key, even when the requested limit is zero. Use
    /// [`Self::scan_with_options`] to resume from an empty last key.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan(
        &self,
        prefix: Vec<u8>,
        start_after: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        self.scan_directional(
            prefix,
            start_after,
            end_key,
            limit,
            byte_budget,
            keys_only,
            deadline_ms,
            ScanDirection::Forward,
        )
        .await
    }

    /// Directional asynchronous scan. Reverse mode interprets the legacy
    /// continuation bytes as an exclusive upper cursor.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_directional(
        &self,
        prefix: Vec<u8>,
        start_after: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
        direction: ScanDirection,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        self.scan_with_options(ScanOptions {
            prefix,
            start: (!start_after.is_empty()).then_some(start_after),
            end: end_key,
            limit,
            byte_budget,
            keys_only,
            deadline_ms,
            direction,
            ..Default::default()
        })
        .await
    }

    /// Like [`Self::try_get`], returns ready scan output on the first native
    /// completion poll and boxes a future only when that poll is pending.
    #[allow(clippy::too_many_arguments)]
    pub fn try_scan(
        &self,
        prefix: Vec<u8>,
        start_after: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
    ) -> ScanOutcome {
        self.try_scan_directional(
            prefix,
            start_after,
            end_key,
            limit,
            byte_budget,
            keys_only,
            deadline_ms,
            ScanDirection::Forward,
        )
    }

    /// Directional scan with an immediate first completion poll.
    #[allow(clippy::too_many_arguments)]
    pub fn try_scan_directional(
        &self,
        prefix: Vec<u8>,
        start_after: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
        direction: ScanDirection,
    ) -> ScanOutcome {
        self.try_scan_with_options(ScanOptions {
            prefix,
            start: (!start_after.is_empty()).then_some(start_after),
            end: end_key,
            limit,
            byte_budget,
            keys_only,
            deadline_ms,
            direction,
            ..Default::default()
        })
    }
}

/// Result of [`AsyncCrowdbtree::try_get`] -- see its doc comment for why this
/// exists instead of a single uniform `async fn`.
#[allow(clippy::type_complexity)]
pub enum GetOutcome {
    /// Resolved on the first poll without boxing a Rust future.
    Ready(Result<Option<(u64, Vec<u8>)>, CtError>),
    /// Native CPU or I/O work is pending; await its completion.
    Pending(Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, CtError>> + Send>>),
}

/// Result of [`AsyncCrowdbtree::try_scan`] -- see its doc comment for why
/// this exists instead of a single uniform `async fn`.
#[allow(clippy::type_complexity)]
pub enum ScanOutcome {
    /// Resolved on the first poll without boxing a Rust future.
    Ready(Result<(Vec<ScanEntry>, bool), CtError>),
    /// Native CPU or I/O work is pending; await its completion.
    Pending(Pin<Box<dyn Future<Output = Result<(Vec<ScanEntry>, bool), CtError>> + Send>>),
}

/// Result of [`AsyncCrowdbtree::try_get_pinned`] -- like [`GetOutcome`] but
/// the fast path returns a [`PinnedValue`] (zero-copy borrow from the C++
/// frame) instead of an owned `Vec<u8>`. The slow path is identical to
/// [`GetOutcome::Pending`]: the value is always owned (copied by
/// `materialize_owned` on the reactor thread).
#[allow(clippy::type_complexity)]
pub enum PinnedGetOutcome {
    /// Fast path (resident hit/miss) -- zero-copy borrow, no `copy_buf`.
    Ready(Result<Option<(u64, PinnedValue)>, CtError>),
    /// Slow path (demand-load miss) -- resolves to an owned `Vec<u8>`,
    /// same as [`GetOutcome::Pending`].
    Pending(Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, CtError>> + Send>>),
}
