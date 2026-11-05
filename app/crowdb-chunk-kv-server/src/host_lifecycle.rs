// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One explicit host exit path also covers partially initialized services.

use crowdb_chunk_kv_server::serving::process::ProcessLeaseSession;
use crowdb_chunk_kv_server::{ChunkKvRpcService, ChunkKvService, ChunkKvStorage};
use crowdb_rpc_ffi::RpcServer;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{error, warn};

pub(super) struct HostLifecycle {
    pub storage: Arc<ChunkKvStorage>,
    pub process: Option<Arc<ProcessLeaseSession>>,
    pub service: Option<Arc<ChunkKvService>>,
    pub rpc: Option<Arc<ChunkKvRpcService>>,
    pub server: Option<Arc<RpcServer>>,
    pub tasks: [Option<JoinHandle<()>>; 4],
    pub stop: Arc<HostStop>,
}

impl HostLifecycle {
    pub fn new(storage: Arc<ChunkKvStorage>) -> Self {
        Self {
            storage,
            process: None,
            service: None,
            rpc: None,
            server: None,
            tasks: std::array::from_fn(|_| None),
            stop: Arc::default(),
        }
    }

    pub async fn close(&mut self) {
        if let Some(service) = &self.service {
            service.begin_drain();
        }
        if let Some(server) = &self.server {
            server.stop();
        }
        self.stop.close();
        for task in &mut self.tasks {
            if let Some(task) = task.take() {
                let _ = task.await;
            }
        }
        if let Some(service) = &self.service {
            if let Err(error) = service.drain_writers().await {
                warn!(%error, "WAL writer drain incomplete; restart must retain lease exclusion");
            }
        }
        if let Some(rpc) = &self.rpc {
            rpc.close_and_drain().await;
        }
        if let Some(service) = &self.service {
            if let Err(error) = service.release_hosted_trees().await {
                error!(%error, "native Tree close failed during host shutdown");
            }
        }
        self.storage.drain_pin_releases().await;
        if let Some(process) = &self.process {
            if let Err(error) = process.withdraw_observation().await {
                warn!(%error, "failed to unregister chunk KV instance");
            }
        }
    }
}

#[derive(Default)]
pub(super) struct HostStop {
    closed: AtomicBool,
    wake: Notify,
}
impl HostStop {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }
    pub async fn wait(&self) {
        loop {
            let wake = self.wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            if self.closed.load(Ordering::Acquire) {
                return;
            }
            wake.await;
        }
    }
}
