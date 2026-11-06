// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{error::check, sys, Crowdbtree, CtError};
use std::{ptr::NonNull, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub struct SplitOverlayLimits {
    pub max_records: u64,
    pub max_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstalledSplit {
    pub generation: u64,
    pub frontier: u64,
}

/// An owned native installation capability; dropping it cancels an unused lease.
pub struct PreparedSplitOverlay {
    raw: NonNull<sys::ct_prepared_split_overlay>,
    start_frontier: u64,
    _source: Arc<Crowdbtree>,
    _destination: Arc<Crowdbtree>,
}

// SAFETY: the native capability serializes state transitions atomically and
// supports concurrent read-only queries. Its Tree owners outlive every call.
unsafe impl Send for PreparedSplitOverlay {}
unsafe impl Sync for PreparedSplitOverlay {}

impl Crowdbtree {
    /// Prepare all source, destination, and allocation work on a host thread.
    /// The destination's durable checkpoint stays unchanged.
    ///
    /// # Errors
    /// Returns an incompatible destination, exhausted budget, or closed source.
    pub fn arm_split_overlay(
        self: &Arc<Self>,
        destination: Arc<Self>,
        limits: SplitOverlayLimits,
    ) -> Result<PreparedSplitOverlay, CtError> {
        let limits = sys::ct_split_overlay_limits_v1 {
            version: 1,
            struct_size: u32::try_from(std::mem::size_of::<sys::ct_split_overlay_limits_v1>())
                .map_err(|_| CtError::Internal)?,
            max_records: limits.max_records,
            max_bytes: limits.max_bytes,
        };
        let mut raw = std::ptr::null_mut();
        let mut start_frontier = 0;
        check(unsafe {
            sys::ct_arm_split_overlay(
                self.as_ptr(),
                destination.as_ptr(),
                &raw const limits,
                &raw mut raw,
                &raw mut start_frontier,
            )
        })?;
        Ok(PreparedSplitOverlay {
            raw: NonNull::new(raw).ok_or(CtError::Internal)?,
            start_frontier,
            _source: self.clone(),
            _destination: destination,
        })
    }
}

impl PreparedSplitOverlay {
    #[must_use]
    pub const fn start_frontier(&self) -> u64 {
        self.start_frontier
    }

    /// Finish destination preparation after replay reaches the observer start.
    ///
    /// # Errors
    /// Returns an expired observer or changed destination.
    pub fn prepare(&self, replayed_prefix: u64) -> Result<(), CtError> {
        check(unsafe { sys::ct_prepared_split_overlay_prepare(self.raw.as_ptr(), replayed_prefix) })
    }

    /// Atomically commit one exact frontier without I/O, allocation, or waiting.
    ///
    /// # Errors
    /// Returns a busy, expired, or conflicting installation without publishing it.
    pub fn try_install(&self, frontier: u64) -> Result<InstalledSplit, CtError> {
        let mut result = sys::ct_installed_split_overlay_v1 {
            version: 1,
            struct_size: u32::try_from(std::mem::size_of::<sys::ct_installed_split_overlay_v1>())
                .map_err(|_| CtError::Internal)?,
            ..Default::default()
        };
        check(unsafe {
            sys::ct_prepared_split_overlay_try_install(self.raw.as_ptr(), frontier, &raw mut result)
        })?;
        Ok(InstalledSplit {
            generation: result.generation,
            frontier: result.frontier,
        })
    }

    #[must_use]
    pub fn committed_frontier(&self) -> Option<u64> {
        let mut frontier = 0;
        (unsafe { sys::ct_prepared_split_overlay_committed_frontier(self.raw.as_ptr(), &raw mut frontier) }
            != 0)
            .then_some(frontier)
    }
}

impl Drop for PreparedSplitOverlay {
    fn drop(&mut self) {
        unsafe { sys::ct_prepared_split_overlay_destroy(self.raw.as_ptr()) };
    }
}
