// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Test backend preparation follows writes before its constant-size cutover.

use super::MemoryPartitionTree;
use crate::{
    ChunkKvError, InstalledSplit, PartitionRange, PartitionTree, PreparedSplitOverlay, Result,
    SplitOverlayLimits, ValueRevision,
};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
};
use tokio::sync::RwLock;

type Values = BTreeMap<Vec<u8>, ValueRevision>;
const UNCOMMITTED: u64 = u64::MAX;

#[derive(Debug)]
pub(super) struct MemoryValues {
    current: Arc<Values>,
    prepared: BTreeMap<u64, PreparedView>,
}

#[derive(Debug)]
struct PreparedView {
    values: Arc<Values>,
    previous: Option<Arc<Values>>,
    range: PartitionRange,
    committed: Arc<AtomicU64>,
    alive: Weak<()>,
    changed_bytes: u64,
    changed_records: usize,
    limits: SplitOverlayLimits,
    expired: bool,
}

impl MemoryValues {
    pub(super) fn new(values: Values) -> Self {
        Self {
            current: Arc::new(values),
            prepared: BTreeMap::new(),
        }
    }

    pub(super) fn apply(&mut self, key: &[u8], value: Option<ValueRevision>) {
        for view in self.prepared.values_mut() {
            if view.expired
                || view.committed.load(Ordering::Acquire) != UNCOMMITTED
                || !view.range.contains(key)
            {
                continue;
            }
            let bytes =
                (key.len() as u64).saturating_add(value.as_ref().map_or(0, |value| value.value.len() as u64));
            if view.changed_records == view.limits.max_records
                || bytes > view.limits.max_bytes.saturating_sub(view.changed_bytes)
            {
                view.expired = true;
                continue;
            }
            view.changed_records += 1;
            view.changed_bytes += bytes;
            update(Arc::make_mut(&mut view.values), key, value.clone());
        }
        update(Arc::make_mut(&mut self.current), key, value);
    }

    pub(super) fn prepared_view(&self, generation: u64) -> Result<Values> {
        self.prepared
            .get(&generation)
            .filter(|view| view.committed.load(Ordering::Acquire) != UNCOMMITTED)
            .map(|view| (*view.values).clone())
            .ok_or_else(|| ChunkKvError::SplitRetry("prepared memory capture is stale".into()))
    }

    pub(super) fn prepared_footprint(&self) -> (usize, u64, bool) {
        self.prepared.values().next().map_or((0, 0, false), |view| {
            (view.changed_records, view.changed_bytes, view.expired)
        })
    }

    pub(super) fn release_prepared(&mut self, generation: u64) -> bool {
        self.prepared.remove(&generation).is_some()
    }
}

impl Deref for MemoryValues {
    type Target = Values;
    fn deref(&self) -> &Self::Target {
        &self.current
    }
}
impl DerefMut for MemoryValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.current)
    }
}

fn update(values: &mut Values, key: &[u8], value: Option<ValueRevision>) {
    if let Some(value) = value {
        values.insert(key.to_vec(), value);
    } else {
        values.remove(key);
    }
}

struct MemoryPreparedOverlay {
    source: Arc<RwLock<MemoryValues>>,
    destination: Arc<RwLock<MemoryValues>>,
    source_frontier: Arc<AtomicU64>,
    destination_frontier: Arc<AtomicU64>,
    committed: Arc<AtomicU64>,
    _alive: Arc<()>,
    generation: u64,
    start_frontier: u64,
    replayed_prefix: AtomicU64,
    limits: SplitOverlayLimits,
}

pub(super) async fn arm(
    source: &MemoryPartitionTree,
    destination: &dyn PartitionTree,
    limits: SplitOverlayLimits,
) -> Result<(Arc<dyn PreparedSplitOverlay>, u64)> {
    let destination = destination
        .as_any()
        .downcast_ref::<MemoryPartitionTree>()
        .ok_or_else(|| {
            ChunkKvError::InvalidRequest("memory preparation requires a memory destination".into())
        })?;
    if Arc::ptr_eq(&source.values, &destination.values) || limits.max_records == 0 || limits.max_bytes == 0 {
        return Err(ChunkKvError::SplitRetry(
            "memory preparation has no private replayed prefix".into(),
        ));
    }
    let generation = source
        .next_split_view
        .fetch_add(1, Ordering::AcqRel)
        .checked_add(1)
        .ok_or_else(|| ChunkKvError::Faulted("split view generation exhausted".into()))?;
    let committed = Arc::new(AtomicU64::new(UNCOMMITTED));
    let alive = Arc::new(());
    if !source.split_views.lock().await.is_empty() {
        return Err(ChunkKvError::SplitRetry(
            "another memory split capture is active".into(),
        ));
    }
    let mut values = source.values.write().await;
    values.prepared.retain(|_, view| {
        view.alive.strong_count() != 0 || view.committed.load(Ordering::Acquire) != UNCOMMITTED
    });
    if !values.prepared.is_empty() {
        return Err(ChunkKvError::SplitRetry(
            "another memory split capture is active".into(),
        ));
    }
    let inherited = values
        .iter()
        .filter(|(key, _)| destination.range.contains(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    values.prepared.insert(
        generation,
        PreparedView {
            values: Arc::new(inherited),
            previous: None,
            range: destination.range.clone(),
            committed: committed.clone(),
            alive: Arc::downgrade(&alive),
            changed_bytes: 0,
            changed_records: 0,
            limits,
            expired: false,
        },
    );
    let start_frontier = source.last_applied.load(Ordering::Acquire);
    Ok((
        Arc::new(MemoryPreparedOverlay {
            source: source.values.clone(),
            destination: destination.values.clone(),
            source_frontier: source.last_applied.clone(),
            destination_frontier: destination.last_applied.clone(),
            committed,
            _alive: alive,
            generation,
            start_frontier,
            replayed_prefix: AtomicU64::new(UNCOMMITTED),
            limits,
        }),
        start_frontier,
    ))
}

#[async_trait]
impl PreparedSplitOverlay for MemoryPreparedOverlay {
    async fn prepare(&self, replayed_prefix: u64) -> Result<()> {
        if replayed_prefix < self.start_frontier
            || self.destination_frontier.load(Ordering::Acquire) != replayed_prefix
        {
            return Err(ChunkKvError::SplitRetry(
                "memory replay does not cover observer start".into(),
            ));
        }
        match self.replayed_prefix.compare_exchange(
            UNCOMMITTED,
            replayed_prefix,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(previous) if previous == replayed_prefix => Ok(()),
            Err(_) => Err(ChunkKvError::SplitRetry("memory prepared prefix is fixed".into())),
        }
    }

    fn try_install(&self, frontier: u64) -> Result<InstalledSplit> {
        if let Some(previous) = self.committed_frontier() {
            return if previous == frontier {
                Ok(InstalledSplit {
                    generation: self.generation,
                    frontier,
                })
            } else {
                Err(ChunkKvError::SplitRetry(
                    "prepared memory frontier is already fixed".into(),
                ))
            };
        }
        let mut source = self
            .source
            .try_write()
            .map_err(|_| ChunkKvError::SplitRetry("memory source preparation is busy".into()))?;
        let mut destination = self
            .destination
            .try_write()
            .map_err(|_| ChunkKvError::SplitRetry("memory destination preparation is busy".into()))?;
        // A concurrent installer may have committed after the optimistic
        // query and released these locks. Never extend its frozen prefix.
        if let Some(previous) = self.committed_frontier() {
            return if previous == frontier {
                Ok(InstalledSplit {
                    generation: self.generation,
                    frontier,
                })
            } else {
                Err(ChunkKvError::SplitRetry(
                    "prepared memory frontier is already fixed".into(),
                ))
            };
        }
        let view = source
            .prepared
            .get_mut(&self.generation)
            .ok_or_else(|| ChunkKvError::SplitRetry("prepared memory capture is absent".into()))?;
        let replayed_prefix = self.replayed_prefix.load(Ordering::Acquire);
        if view.expired
            || replayed_prefix == UNCOMMITTED
            || frontier == UNCOMMITTED
            || frontier < replayed_prefix
            || frontier.saturating_sub(self.start_frontier) > self.limits.max_records as u64
            || view.changed_bytes > self.limits.max_bytes
            || self.source_frontier.load(Ordering::Acquire) != frontier
            || self.destination_frontier.load(Ordering::Acquire) != replayed_prefix
        {
            return Err(ChunkKvError::SplitRetry(
                "prepared memory tail no longer fits its admission".into(),
            ));
        }
        // Retain the old map for background release. Neither replacing the
        // route's map nor acknowledging its frontier copies or frees entries.
        view.previous = Some(std::mem::replace(&mut destination.current, view.values.clone()));
        self.destination_frontier.store(frontier, Ordering::Release);
        self.committed.store(frontier, Ordering::Release);
        Ok(InstalledSplit {
            generation: self.generation,
            frontier,
        })
    }

    fn committed_frontier(&self) -> Option<u64> {
        let value = self.committed.load(Ordering::Acquire);
        (value != UNCOMMITTED).then_some(value)
    }
}
