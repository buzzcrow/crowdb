// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{body::Body, http::Request};
use crowdb_chunkdb::{range_guard::RangeGuard, readiness::RangeReadiness};
use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};
use tower::ServiceExt;

pub struct TestReadiness {
    pub guard: Arc<RangeGuard>,
    pub readiness: Arc<RangeReadiness>,
    pub mode: Arc<AtomicUsize>,
    pub calls: Arc<AtomicUsize>,
    pub stop: watch::Sender<bool>,
    pub worker: JoinHandle<()>,
}

impl TestReadiness {
    pub fn new(initialized: bool, instance: u64) -> Self {
        let guard = Arc::new(RangeGuard::new());
        if initialized {
            guard.install(&layout().service_map().unwrap(), instance).unwrap();
        }
        let readiness = Arc::new(RangeReadiness::new(Arc::clone(&guard)));
        let mode = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let (stop, receiver) = watch::channel(false);
        let worker = Arc::clone(&readiness).start_source_for_tests(receiver, {
            let mode = Arc::clone(&mode);
            let calls = Arc::clone(&calls);
            move || {
                let mode = mode.load(Ordering::Relaxed);
                calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    match mode {
                        1 => tokio::time::sleep(Duration::from_millis(1500)).await,
                        2 => std::future::pending::<()>().await,
                        3 => return Err("authority unavailable".into()),
                        _ => {}
                    }
                    Ok(())
                }
            }
        });
        Self {
            guard,
            readiness,
            mode,
            calls,
            stop,
            worker,
        }
    }

    pub async fn read(&self) -> u16 {
        read(Arc::clone(&self.readiness)).await
    }

    pub async fn settle() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }
}

pub fn layout() -> ChunkSlotBootstrap {
    ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    }
}

pub async fn read(readiness: Arc<RangeReadiness>) -> u16 {
    tokio::time::timeout(
        Duration::from_secs(1),
        readiness
            .router()
            .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()),
    )
    .await
    .expect("HTTP readiness waited for authority I/O")
    .unwrap()
    .status()
    .as_u16()
}
