// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crowdb_chunkdb::task::executor::TaskFuture;
use crowdb_chunkdb::task::{TaskHandler, TaskOutcome};
use crowdb_protocol::chunk_task::ChunkTaskValue;
use tokio::sync::Notify;

pub struct TestTaskHandler {
    pub calls: AtomicUsize,
    pub started: Notify,
    pub release: Option<Arc<Notify>>,
}

impl TestTaskHandler {
    pub fn new(release: Option<Arc<Notify>>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release,
        })
    }
}

impl TaskHandler for TestTaskHandler {
    fn kind(&self) -> u16 {
        42
    }
    fn supports_version(&self, version: u16) -> bool {
        version == 1
    }
    fn execute<'a>(&'a self, _task: &'a ChunkTaskValue) -> TaskFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            if let Some(release) = &self.release {
                release.notified().await;
            }
            TaskOutcome::Complete
        })
    }
}
