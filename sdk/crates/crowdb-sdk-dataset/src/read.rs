// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    wire::{BatchDecoder, BatchEvent, MAX_HEADER_FRAME},
    BatchRequest, DatasetClient, Error,
};
use bytes::BytesMut;
use crowdb_sdk_core::ResponseStream;

/// Incremental verified delivery. A fetched batch becomes complete only at EOF;
/// obtaining these events never acknowledges application consumption.
pub struct BatchStream {
    transport: ResponseStream,
    decoder: BatchDecoder,
    buffer: BytesMut,
    ended: bool,
}
impl BatchStream {
    pub(crate) fn new_v2(transport: ResponseStream) -> Self {
        Self {
            decoder: BatchDecoder::for_control_v2(),
            ..Self::new(transport)
        }
    }
    pub(crate) fn new(transport: ResponseStream) -> Self {
        Self {
            transport,
            decoder: BatchDecoder::new(),
            buffer: BytesMut::new(),
            ended: false,
        }
    }
    /// # Errors
    /// Reports frame/field integrity, version, transport and deadline failures.
    pub async fn next_event(&mut self) -> Result<Option<BatchEvent>, Error> {
        if self.ended {
            return Ok(None);
        }
        loop {
            if let Some(event) = self.decoder.decode(&mut self.buffer)? {
                match event {
                    BatchEvent::Data(bytes) => {
                        return Ok(Some(BatchEvent::Data(self.transport.retain_frame(bytes).await?)))
                    }
                    BatchEvent::End => {
                        if self.transport.next_chunk().await?.is_some() {
                            return Err(Error::Integrity);
                        }
                        self.decoder.finish()?;
                        self.ended = true;
                        return Ok(Some(BatchEvent::End));
                    }
                    other => return Ok(Some(other)),
                }
            }
            let Some(bytes) = self.transport.next_chunk().await? else {
                self.decoder.finish()?;
                self.ended = true;
                return Ok(None);
            };
            if self.buffer.len().saturating_add(bytes.len()) > 2 * MAX_HEADER_FRAME + 32 {
                return Err(Error::Limit);
            }
            self.buffer.extend_from_slice(&bytes);
        }
    }
}
impl DatasetClient {
    /// # Errors
    /// Rejects invalid scope, selection, capacity, service or transport failures.
    pub async fn stream_batch(&self, request: &BatchRequest) -> Result<BatchStream, Error> {
        Ok(BatchStream::new(
            self.http
                .stream("POST", &self.path(&["reads"]), Some(request))
                .await?,
        ))
    }
}
