// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{Arc, Weak};
use std::time::Instant;

use crowdb_common::metrics::MetricsSampler;
use crowdb_tree_ffi::runtime::{ResourceUsageLog, ResourceUsageSnapshot};

use crate::{ChunkKvService, ChunkKvStorage};

pub struct TreeResources {
    runtime: Arc<crowdb_tree_ffi::TreeRuntime>,
    service: Weak<ChunkKvService>,
    storage: Weak<ChunkKvStorage>,
    windows: ResourceUsageLog,
}

impl TreeResources {
    #[must_use]
    pub fn new(runtime: Arc<crowdb_tree_ffi::TreeRuntime>, service: &Arc<ChunkKvService>) -> Self {
        Self {
            runtime,
            service: Arc::downgrade(service),
            storage: Weak::new(),
            windows: ResourceUsageLog::default(),
        }
    }

    #[must_use]
    pub fn with_storage(mut self, storage: &Arc<ChunkKvStorage>) -> Self {
        self.storage = Arc::downgrade(storage);
        self
    }

    fn host_usage(&self) -> Result<ResourceUsageSnapshot, crowdb_tree_ffi::CtError> {
        let mut snapshot = self.runtime.resource_usage()?;
        if let Some(storage) = self.storage.upgrade() {
            snapshot.resources[20] = storage.pin_release_resource_usage()?;
            snapshot.resource_mask |= 1 << 20;
        }
        Ok(snapshot)
    }
}

impl MetricsSampler for TreeResources {
    fn sample(&mut self) {
        self.windows.begin_sample();
        match self.host_usage() {
            Ok(snapshot) => self.windows.observe("host.tree", snapshot, Instant::now()),
            Err(_) => self.windows.sample_failed("host.tree"),
        }
        if let Some(service) = self.service.upgrade() {
            service.sample_tree_resources(|name, result| match result {
                Ok(Some(snapshot)) => self.windows.observe(name, snapshot, Instant::now()),
                Ok(None) => {}
                Err(_) => self.windows.sample_failed(name),
            });
        }
        self.windows.end_sample(Instant::now());
    }

    fn flush(&mut self, writer: &mut dyn std::io::Write, timestamp: &str) -> std::io::Result<()> {
        self.windows.flush(writer, timestamp, Instant::now())
    }
}
