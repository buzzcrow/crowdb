// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_access_dataset::{PayloadStream, UploadError};
use crowdb_rpc_ffi::OwnedPayload;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub struct TestPayloadCollector {
    chunks: std::vec::IntoIter<Vec<u8>>,
    pub active: Arc<AtomicUsize>,
    pub pulls: usize,
    pub first_pointer: usize,
    pub fail_eof: bool,
    pub enforce_credit: bool,
}
impl TestPayloadCollector {
    pub fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            chunks: chunks.into_iter(),
            active: Arc::new(AtomicUsize::new(0)),
            pulls: 0,
            first_pointer: 0,
            fail_eof: false,
            enforce_credit: true,
        }
    }
}
struct TestOwner {
    data: Vec<u8>,
    active: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for TestOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl Drop for TestOwner {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}
#[async_trait::async_trait]
impl PayloadStream for TestPayloadCollector {
    async fn next(&mut self) -> Result<Option<OwnedPayload>, UploadError> {
        self.pulls += 1;
        let Some(data) = self.chunks.next() else {
            return if self.fail_eof {
                Err(UploadError::Storage("failed final read".into()))
            } else {
                Ok(None)
            };
        };
        if self.enforce_credit {
            assert_eq!(
                self.active.load(Ordering::Relaxed),
                0,
                "next IO needs the previous buffer's credit"
            );
        }
        if self.first_pointer == 0 {
            self.first_pointer = data.as_ptr() as usize;
        }
        self.active.fetch_add(1, Ordering::Relaxed);
        Ok(Some(OwnedPayload::from(Bytes::from_owner(TestOwner {
            data,
            active: Arc::clone(&self.active),
        }))))
    }
}
