// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::MemoryStreamStore;
use crate::{Result, StreamError, StreamName, StreamOwnerAuthority, StreamWriterSession};
use async_trait::async_trait;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

struct TestMemoryAuthority;

#[async_trait]
impl StreamOwnerAuthority for TestMemoryAuthority {
    fn deadline(&self) -> Result<Instant> {
        Ok(Instant::now() + Duration::from_secs(3600))
    }
    async fn release(&self) -> Result<()> {
        Ok(())
    }
}

impl MemoryStreamStore {
    pub(super) async fn claim_memory_writer(
        &self,
        name: StreamName,
        epoch: u64,
    ) -> Result<Arc<StreamWriterSession>> {
        let mut state = self.state.lock().await;
        if let Some((current_epoch, current)) = state.owners.get(&name) {
            if *current_epoch > epoch {
                return Err(StreamError::StaleWriter);
            }
            current.fence();
        }
        let session = StreamWriterSession::new(Arc::new(TestMemoryAuthority));
        state.owners.insert(name, (epoch, session.clone()));
        Ok(session)
    }
}
