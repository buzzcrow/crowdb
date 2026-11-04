// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use crowdb_tree_ffi::{CtError, RootCatalogObject, RootCatalogStore};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct TestRootCatalog {
    objects: ArcSwap<BTreeMap<(u64, u8, u64), Vec<u8>>>,
    next_reference: AtomicU64,
    pub armed: AtomicBool,
    pub entered: tokio::sync::Notify,
    pub async_progress: AtomicBool,
    pub observed_progress: AtomicBool,
}

impl TestRootCatalog {
    fn key(tree_id: u64, object: RootCatalogObject) -> (u64, u8, u64) {
        match object {
            RootCatalogObject::CurrentManifest => (tree_id, 0, 0),
            RootCatalogObject::Manifest(generation) => (tree_id, 1, generation),
            RootCatalogObject::ReferenceSegment(id) => (tree_id, 2, id),
        }
    }

    fn wait_for_async_progress(&self) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        self.entered.notify_one();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !self.async_progress.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        self.observed_progress
            .store(self.async_progress.load(Ordering::Acquire), Ordering::Release);
    }
}

impl RootCatalogStore for TestRootCatalog {
    fn load(&self, tree_id: u64, object: RootCatalogObject) -> Result<Option<Vec<u8>>, CtError> {
        self.wait_for_async_progress();
        Ok(self.objects.load().get(&Self::key(tree_id, object)).cloned())
    }

    fn store(&self, tree_id: u64, object: RootCatalogObject, data: &[u8]) -> Result<(), CtError> {
        self.objects.rcu(|previous| {
            let mut next = (**previous).clone();
            next.insert(Self::key(tree_id, object), data.to_vec());
            next
        });
        Ok(())
    }

    fn publish(&self, tree_id: u64, _: u64, _: u64, generation: u64, manifest: &[u8]) -> Result<(), CtError> {
        self.wait_for_async_progress();
        self.store(tree_id, RootCatalogObject::Manifest(generation), manifest)?;
        self.store(tree_id, RootCatalogObject::CurrentManifest, manifest)
    }

    fn allocate_reference_segment_id(&self, _: u64) -> Result<u64, CtError> {
        Ok(self.next_reference.fetch_add(1, Ordering::AcqRel) + 1)
    }
}
