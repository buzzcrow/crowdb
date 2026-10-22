// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_rpc_ffi::RdmaConfig;
use serde::{Deserialize, Serialize};

use crate::{ClientError, Result};

/// Fixed metadata and aggregate connection groups sharing one RPC caller.
/// Missing overrides retain environment policy with 64 KiB/256 KiB defaults.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChunkKvRpcTransportConfig {
    pub metadata_rdma: Option<RdmaConfig>,
    pub aggregate_rdma: Option<RdmaConfig>,
}

impl ChunkKvRpcTransportConfig {
    /// Validate the layouts required by encoded-control streams.
    ///
    /// # Errors
    /// Enabled RDMA groups require a supported control/data class.
    pub fn validate(&self) -> Result<()> {
        for (name, config) in [
            ("metadata_rdma", &self.metadata_rdma),
            ("aggregate_rdma", &self.aggregate_rdma),
        ] {
            if let Some(config) = config {
                if config.enabled && config.control_data_capacity().is_none() {
                    return Err(ClientError::InvalidRequest(format!(
                        "{name} requires control_data with 64 KiB, 256 KiB or 1 MiB data capacity"
                    )));
                }
            }
        }
        Ok(())
    }
}
