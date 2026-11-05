// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct TestCatalogLoad {
    pub armed: AtomicBool,
    pub entered: tokio::sync::Notify,
    pub async_progress: AtomicBool,
    pub observed_progress: AtomicBool,
}

impl TestCatalogLoad {
    pub fn wait_for_async_progress(&self) {
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
