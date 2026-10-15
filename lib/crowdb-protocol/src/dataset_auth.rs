// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Stable Dataset service credential derivation for existing cluster deployments.

use sha2::{Digest, Sha256};

/// Derive a protocol-specific bearer without exposing the cluster master secret.
/// The input is the deployment's private random master key, never user input.
#[must_use]
pub fn deployment_bearer(master: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"crowdb-dataset-bearer-v1\0");
    digest.update(master.as_bytes());
    format!("{:x}", digest.finalize())
}
