// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use crowdb_common::metrics::MetricsSampler;
use crowdb_paxos_kv::kv::CrowdbTreeEngine;
use crowdb_tree_ffi::runtime::{ResourceUsageLog, ResourceUsageSnapshot};

use crate::store_registry::PaxosKvStoreRegistry;

pub(super) struct Resources {
    stores: Arc<PaxosKvStoreRegistry>,
    windows: ResourceUsageLog,
}

impl Resources {
    pub fn new(stores: Arc<PaxosKvStoreRegistry>) -> Self {
        Self {
            stores,
            windows: ResourceUsageLog::default(),
        }
    }

    fn observe(&mut self, name: &str, value: &Result<ResourceUsageSnapshot, crowdb_tree_ffi::CtError>) {
        match value {
            Ok(value) => self.windows.observe(name, *value, Instant::now()),
            Err(_) => self.windows.sample_failed(name),
        }
    }
}

impl MetricsSampler for Resources {
    fn sample(&mut self) {
        self.windows.begin_sample();
        self.observe("host.tree", &self.stores.tree_runtime.resource_usage());
        let stores = self.stores.stores_snapshot();
        for (store_id, store) in stores.iter() {
            store.for_each_group(|group| {
                let replica = group.local_replica();
                let engine = replica.learner.engine();
                if let Some(engine) = engine.as_any().downcast_ref::<CrowdbTreeEngine>() {
                    let name = format!("s.{store_id}.g.{}.tree", group.group_id());
                    self.observe(&name, &engine.handle().resource_usage());
                }
            });
        }
        self.windows.end_sample(Instant::now());
    }

    fn flush(&mut self, writer: &mut dyn std::io::Write, timestamp: &str) -> std::io::Result<()> {
        self.windows.flush(writer, timestamp, Instant::now())
    }
}
