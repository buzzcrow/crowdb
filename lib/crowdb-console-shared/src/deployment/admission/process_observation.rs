// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A single sampler owns its authenticated, read-only monitor connection.

use crate::{
    config::NodeEntry,
    error::{Error, Result},
    ssh::Session,
};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeControl};
use serde_json::Value;
use std::time::Duration;
use tokio::time::Instant;

/// Bound process observations reuse SSH without sharing sessions with mutations.
/// Dropping a read future discards its connection; the next cadence reconnects.
pub struct RemoteProcessObservation {
    node: NodeEntry,
    binding: NodeBinding,
    connection: Option<(Session, Instant)>,
}

impl RemoteProcessObservation {
    /// Construct a reader for one accepted node using its provisioned local key.
    ///
    /// # Errors
    /// Rejects password fallback or a missing explicit key.
    pub fn new(node: NodeEntry, binding: NodeBinding) -> Result<Self> {
        if node.ssh_password.is_some() || node.ssh_key.is_none() {
            return Err(Error::Config(
                "process observations require an explicit node key".into(),
            ));
        }
        Ok(Self {
            node,
            binding,
            connection: None,
        })
    }

    /// Observe the same node and bootstrap, with no transport retry on failure.
    ///
    /// # Errors
    /// Propagates authentication, command and identity failures.
    pub async fn read(&mut self) -> Result<Value> {
        // Keep no stored handle across an await: timeout cancellation must not
        // retain a connection with an unfinished command or an unread reply.
        let (mut session, connected) = match self.connection.take() {
            Some((session, connected)) if connected.elapsed() < Duration::from_secs(60) => {
                (session, connected)
            }
            previous => {
                if let Some((session, _)) = previous {
                    session.close().await;
                }
                (super::connect(&self.node).await?, Instant::now())
            }
        };
        let command = NodeControl::Processes {
            cluster_id: self.binding.bootstrap.cluster_id.clone(),
            node_id: self.node.id,
        };
        let result = super::control(&mut session, &command).await.and_then(|value| {
            if value["node_id"] != self.node.id
                || value["cluster_id"] != self.binding.bootstrap.cluster_id
                || value["bootstrap"]
                    != serde_json::to_value(&self.binding.bootstrap).map_err(super::config_error)?
            {
                return Err(Error::Config(
                    "process observation identity differs from accepted node".into(),
                ));
            }
            Ok(value)
        });
        if result.is_ok() {
            self.connection = Some((session, connected));
        } else {
            session.close().await;
        }
        result
    }
}
