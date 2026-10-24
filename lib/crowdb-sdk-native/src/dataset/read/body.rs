// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{field::FieldReader, BatchHeader};
use crate::{buffer::BufferLease, runtime::RequestLease, Error, Result, RuntimeResources};
use crowdb_access_dataset::{DatasetUploads, FieldLocator};
use crowdb_rpc_ffi::OwnedPayload;
use std::{collections::VecDeque, sync::Arc};

pub struct FieldBuffer {
    pub field: usize,
    pub buffer: BufferLease,
}

/// One bounded pull ahead; caller-held allocations stay charged until released.
/// Dropping an individual pull future preserves pending data for the next pull.
pub struct BatchBody {
    header: Arc<BatchHeader>,
    locators: VecDeque<(usize, FieldLocator)>,
    field: Option<FieldReader>,
    pending: Option<OwnedPayload>,
    request: Option<RequestLease>,
    resources: Arc<RuntimeResources>,
    uploads: Arc<DatasetUploads>,
    sequence: u64,
    complete: bool,
    failure: Option<Error>,
}

impl BatchBody {
    pub(super) fn new(
        header: Arc<BatchHeader>,
        locators: Vec<(usize, FieldLocator)>,
        request: RequestLease,
        resources: Arc<RuntimeResources>,
        uploads: Arc<DatasetUploads>,
    ) -> Self {
        Self {
            header,
            locators: locators.into(),
            field: None,
            pending: None,
            request: Some(request),
            resources,
            uploads,
            sequence: 0,
            complete: false,
            failure: None,
        }
    }

    #[must_use]
    pub fn header(&self) -> &BatchHeader {
        &self.header
    }
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Successful EOF certifies all field and part digests, never processing ack.
    /// EOF releases local request admission without metadata writes.
    /// # Errors
    /// Returns sticky integrity/transport/cancellation failures or expired admission.
    pub async fn next(&mut self) -> Result<Option<FieldBuffer>> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.complete {
            return Ok(None);
        }
        match self.pull().await {
            Ok(Some(data)) => Ok(Some(data)),
            Ok(None) => {
                self.clear();
                self.complete = true;
                Ok(None)
            }
            Err(error) => {
                self.clear();
                self.failure = Some(error.clone());
                Err(error)
            }
        }
    }

    pub fn cancel(&mut self) {
        if !self.complete && self.failure.is_none() {
            self.clear();
            self.failure = Some(Error::Cancelled);
        }
    }

    fn clear(&mut self) {
        self.field = None;
        self.pending = None;
        self.locators.clear();
        self.request = None;
    }

    async fn pull(&mut self) -> Result<Option<FieldBuffer>> {
        let request = self.request.as_ref().ok_or(Error::Cancelled)?;
        request.run(async { Ok(()) }).await?;
        loop {
            if self.field.is_none() {
                let Some((index, locator)) = self.locators.pop_front() else {
                    return Ok(None);
                };
                self.field = Some(FieldReader::new(index, locator)?);
            }
            let field = self.field.as_mut().ok_or(Error::Integrity)?;
            if self.pending.is_none() {
                self.pending = field.next(&self.uploads, request).await?;
                if self.pending.is_none() {
                    self.field = None;
                    continue;
                }
            }
            let payload = self.pending.as_ref().ok_or(Error::Integrity)?.clone();
            let id = request.context().request_id.get();
            let buffer = request
                .run(
                    self.resources
                        .retain_payload(payload, id, id, self.sequence, field.offset),
                )
                .await?;
            field.delivered(buffer.as_slice())?;
            self.pending = None;
            self.sequence = self.sequence.checked_add(1).ok_or(Error::BufferRange)?;
            return Ok(Some(FieldBuffer {
                field: field.index,
                buffer,
            }));
        }
    }
}
