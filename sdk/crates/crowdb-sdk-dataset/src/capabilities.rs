// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Server capability discovery before versioned control operations.

use crate::{DatasetClient, Error};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

const KNOWN_CAPABILITIES: [&str; 10] = [
    "head-inspection",
    "bounded-inspection",
    "inspection-preparation",
    "dataset-management",
    "split-revisions",
    "split-head-update",
    "stateless-reads",
    "uploads",
    "composition",
    "snapshot-reclaim",
];

fn capability_bit(capability: &str) -> u64 {
    KNOWN_CAPABILITIES
        .iter()
        .position(|known| *known == capability)
        .map_or(0, |index| 1 << index)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DatasetCapabilities {
    pub api_version: u16,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DatasetHead {
    #[serde(deserialize_with = "required_option")]
    pub latest: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub stable: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub generation: Option<String>,
}

fn required_option<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
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
    /// Reads mutable names and their opaque generation in one server observation.
    /// # Errors
    /// Rejects unsupported capability, missing Dataset or invalid server responses.
    pub async fn head(&self) -> Result<DatasetHead, Error> {
        self.require_capability("head-inspection").await?;
        let mut path = self.path(&["head"]);
        path[2] = "v2";
        self.http.json_idempotent::<(), _>("GET", &path, None).await
    }

    /// Always refreshes capabilities and replaces the positive hints shared by clones.
    /// Hints never bypass server authentication, resource admission or mutation conditions.
    /// A v1-only server returns an error; mutations never fall back to v1.
    /// # Errors
    /// Returns unsupported-version, service, decoding or transport errors.
    pub async fn capabilities(&self) -> Result<DatasetCapabilities, Error> {
        let capabilities: DatasetCapabilities = self
            .http
            .json_idempotent::<(), _>("GET", &["api", "dataset", "v2", "capabilities"], None)
            .await?;
        let valid = capabilities.require(&[]);
        let bits = if valid.is_ok() {
            capabilities
                .capabilities
                .iter()
                .fold(0, |bits, capability| bits | capability_bit(capability))
        } else {
            0
        };
        self.capability_hints.store(bits, Ordering::Release);
        valid?;
        Ok(capabilities)
    }

    pub(crate) async fn require_capability(&self, capability: &str) -> Result<(), Error> {
        let bit = capability_bit(capability);
        if bit != 0 && self.capability_hints.load(Ordering::Acquire) & bit != 0 {
            return Ok(());
        }
        self.capabilities().await?.require(&[capability])
    }
}
