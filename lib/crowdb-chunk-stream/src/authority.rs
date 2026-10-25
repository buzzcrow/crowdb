// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Captured writer authority shared by admission, physical IO and acknowledgment.

use crate::{Result, StreamError};
use async_trait::async_trait;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

#[async_trait]
pub trait StreamOwnerAuthority: Send + Sync {
    /// Conservative local deadline of the currently live owner scope.
    /// # Errors
    /// Rejects expired or replaced authority.
    fn deadline(&self) -> Result<Instant>;
    /// Called only after stopping admission, draining IO and checkpointing.
    /// # Errors
    /// Returns an authority or durable release error.
    async fn release(&self) -> Result<()>;
}

pub struct StreamWriterSession {
    authority: Arc<dyn StreamOwnerAuthority>,
    fenced: AtomicBool,
    released: AtomicBool,
}

#[derive(Clone)]
pub struct StreamWriteContext {
    session: Arc<StreamWriterSession>,
    deadline: Instant,
}

impl StreamWriterSession {
    #[must_use]
    pub fn new(authority: Arc<dyn StreamOwnerAuthority>) -> Arc<Self> {
        Arc::new(Self {
            authority,
            fenced: AtomicBool::new(false),
            released: AtomicBool::new(false),
        })
    }

    /// # Errors
    /// Rejects fenced or expired ownership.
    pub fn capture(self: &Arc<Self>) -> Result<StreamWriteContext> {
        let deadline = self.check()?;
        Ok(StreamWriteContext {
            session: Arc::clone(self),
            deadline,
        })
    }

    /// # Errors
    /// Rejects fenced or expired ownership.
    pub fn check(&self) -> Result<Instant> {
        if self.fenced.load(Ordering::Acquire) {
            return Err(StreamError::StaleWriter);
        }
        let deadline = self.authority.deadline()?;
        if Instant::now() >= deadline {
            self.fence();
            return Err(StreamError::StaleWriter);
        }
        Ok(deadline)
    }

    pub fn fence(&self) {
        self.fenced.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }

    /// # Errors
    /// Returns a durable owner-release error; admission stays fenced.
    pub async fn release(&self) -> Result<()> {
        self.fence();
        self.authority.release().await?;
        self.released.store(true, Ordering::Release);
        Ok(())
    }
}

impl StreamWriteContext {
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }
    /// IO retains its original deadline even when the process lease renews.
    /// # Errors
    /// Rejects revoked authority or an elapsed submission deadline.
    pub fn check_io(&self) -> Result<()> {
        self.session.check()?;
        if Instant::now() >= self.deadline {
            return Err(StreamError::StaleWriter);
        }
        Ok(())
    }
    /// An uninterrupted renewal may authorize acknowledgment after old IO drained.
    /// # Errors
    /// Rejects revoked or expired authority.
    pub fn check_ack(&self) -> Result<()> {
        self.session.check().map(|_| ())
    }
}

impl std::fmt::Debug for StreamWriterSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamWriterSession")
            .field("fenced", &self.fenced.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
