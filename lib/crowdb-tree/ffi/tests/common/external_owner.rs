// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;

struct TestOwner {
    data: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for TestOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for TestOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn payload(length: usize, drops: &Arc<AtomicUsize>) -> Bytes {
    Bytes::from_owner(TestOwner {
        data: vec![0xA7; length],
        drops: Arc::clone(drops),
    })
}
