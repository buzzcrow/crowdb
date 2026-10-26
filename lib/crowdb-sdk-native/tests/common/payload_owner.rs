// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct TestPayloadOwner {
    pub bytes: Bytes,
    pub released: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for TestPayloadOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for TestPayloadOwner {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::Release);
    }
}
