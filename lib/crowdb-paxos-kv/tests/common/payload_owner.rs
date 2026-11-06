// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct TestPayload {
    pub bytes: Vec<u8>,
    pub dropped: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for TestPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for TestPayload {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}
