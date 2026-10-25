// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fair progress between private control and background node work.

use tokio::{
    net::{UnixListener, UnixStream},
    time::Interval,
};

#[derive(Debug, Default)]
pub struct NodeDispatch {
    health_first: bool,
}

impl NodeDispatch {
    /// Select the next accepted control stream or a background health pass.
    /// # Errors
    /// Propagates Unix listener failures.
    pub async fn next(
        &mut self,
        listener: &UnixListener,
        heartbeat: &mut Interval,
    ) -> std::io::Result<Option<UnixStream>> {
        let stream = if self.health_first {
            tokio::select! {
                biased;
                _ = heartbeat.tick() => None,
                connection = listener.accept() => Some(connection?.0),
            }
        } else {
            tokio::select! {
                biased;
                connection = listener.accept() => Some(connection?.0),
                _ = heartbeat.tick() => None,
            }
        };
        // When both are ready, alternate priority so neither accepted
        // commands nor service reconciliation can starve the other.
        self.health_first = stream.is_some();
        Ok(stream)
    }
}
