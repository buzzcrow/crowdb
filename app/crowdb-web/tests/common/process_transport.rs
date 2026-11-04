// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::fixture::TestSource;
use crowdb_console_shared::{
    cluster::NodeHealth,
    config::{NodeEntry, ServerEntry},
    ConsoleConfig,
};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceIntent};
use crowdb_web::{AppState, ProcessObservationForTests, ServiceObservationSourceForTests};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
pub struct TestCounts {
    pub created: [AtomicUsize; 3],
    pub dropped: [AtomicUsize; 3],
    pub reads: [AtomicUsize; 3],
}

pub struct TestTransportSource {
    pub source: Arc<TestSource>,
    pub counts: Arc<TestCounts>,
}

struct TestProcessTransport {
    source: Arc<TestSource>,
    counts: Arc<TestCounts>,
    state: AppState,
    binding: NodeBinding,
    node: NodeEntry,
}

impl Drop for TestProcessTransport {
    fn drop(&mut self) {
        self.counts.dropped[usize::try_from(self.node.id).unwrap()].fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl ProcessObservationForTests for TestProcessTransport {
    async fn read(&mut self) -> Result<Value, String> {
        self.counts.reads[usize::try_from(self.node.id).unwrap()].fetch_add(1, Ordering::Relaxed);
        self.source
            .processes(&self.state, &self.binding, &self.node)
            .await
    }
}

#[async_trait::async_trait]
impl ServiceObservationSourceForTests for TestTransportSource {
    fn process_observer(
        &self,
        state: &AppState,
        binding: &NodeBinding,
        node: &NodeEntry,
    ) -> Result<Option<Box<dyn ProcessObservationForTests>>, String> {
        self.counts.created[usize::try_from(node.id).unwrap()].fetch_add(1, Ordering::Relaxed);
        Ok(Some(Box::new(TestProcessTransport {
            source: self.source.clone(),
            counts: self.counts.clone(),
            state: state.clone(),
            binding: binding.clone(),
            node: node.clone(),
        })))
    }

    async fn intents(&self, state: &AppState) -> Result<Vec<NodeServiceIntent>, String> {
        self.source.intents(state).await
    }

    async fn processes(&self, _: &AppState, _: &NodeBinding, _: &NodeEntry) -> Result<Value, String> {
        panic!("the owned process transport must handle process sampling")
    }

    async fn health(
        &self,
        state: &AppState,
        config: &ConsoleConfig,
        entry: &ServerEntry,
        pid: Option<u32>,
        intents: &[NodeServiceIntent],
    ) -> NodeHealth {
        self.source.health(state, config, entry, pid, intents).await
    }
}
