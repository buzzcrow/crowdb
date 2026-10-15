// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Server capability discovery before versioned control operations.

use crate::{DatasetClient, Error};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DatasetCapabilities {
    pub api_version: u16,
    pub capabilities: Vec<String>,
}

impl DatasetCapabilities {
    /// # Errors
    /// Rejects an unsupported protocol version or a missing required capability.
    pub fn require(&self, required: &[&str]) -> Result<(), Error> {
        if self.api_version != 2 {
            return Err(Error::Invalid("unsupported Dataset control API version".into()));
        }
        for capability in required {
            if !self.capabilities.iter().any(|value| value == capability) {
                return Err(Error::Invalid(format!(
                    "Dataset capability unavailable: {capability}"
                )));
            }
        }
        Ok(())
    }
}

impl DatasetClient {
    /// Discovers capabilities without mutating a Dataset. A v1-only server returns
    /// an explicit service error; the SDK never retries a v2 write through v1.
    /// # Errors
    /// Returns unsupported-version, service, decoding or transport errors.
    pub async fn capabilities(&self) -> Result<DatasetCapabilities, Error> {
        let capabilities: DatasetCapabilities = self
            .http
            .json_idempotent::<(), _>("GET", &["api", "dataset", "v2", "capabilities"], None)
            .await?;
        capabilities.require(&[])?;
        Ok(capabilities)
    }
}
