// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Parent-local split request dispatch.

use super::Partition;
use crate::{PartitionLifecycle, PartitionRange, Result, SplitPlan};
use arc_swap::{ArcSwap, ArcSwapOption};
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Arc;

/// Lock-free request routing installed once both split writers own live WALs
/// and memtables at the ordered route frontier.
/// The original parent handle dispatches left keys to the unchanged parent
/// worker and right keys to the new child.
#[derive(Clone)]
pub struct SplitIngress {
    pub(super) split_key: Arc<[u8]>,
    pub(super) retained_parent: Partition,
    pub(super) child: Partition,
}

impl Partition {
    fn strong_writer_clone(&self) -> Self {
        let mut writer = self.clone();
        writer.sender = self.sender.strong_clone();
        writer
    }

    /// Installs child dispatch while retaining the parent's tree and worker.
    ///
    /// # Errors
    /// Rejects a child that does not match the active split plan.
    pub async fn install_child_split_ingress(&self, plan: &SplitPlan, child: Partition) -> Result<()> {
        let mut retained = self.clone();
        retained.range = Arc::new(ArcSwap::from_pointee(PartitionRange {
            start: plan.parent_range.start.clone(),
            end: Some(plan.split_key.clone()),
        }));
        retained.ownership_epoch = Arc::new(AtomicU64::new(plan.parent_next_epoch));
        retained.lifecycle = Arc::new(AtomicU8::new(super::lifecycle_code(PartitionLifecycle::Serving)));
        retained.split_ingress = Arc::new(ArcSwapOption::empty());
        // The worker owns this ingress. A strong sender here would keep its
        // channel alive after every external partition handle was dropped.
        retained.sender = self.sender.borrowed();
        self.install_split_ingress(retained, child).await
    }
}

impl SplitIngress {
    #[must_use]
    pub fn retained_parent(&self) -> Partition {
        self.retained_parent.strong_writer_clone()
    }

    #[must_use]
    pub fn child(&self) -> Partition {
        self.child.clone()
    }

    #[must_use]
    pub fn writer_for_key(&self, key: &[u8]) -> Partition {
        self.writer_for(key).clone()
    }

    #[must_use]
    pub fn split_key(&self) -> &[u8] {
        &self.split_key
    }

    pub(super) fn writer_for(&self, key: &[u8]) -> &Partition {
        if key < self.split_key.as_ref() {
            &self.retained_parent
        } else {
            &self.child
        }
    }
}
