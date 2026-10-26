// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::TenantId;
use crowdb_chunk_client::SmallWritePolicy;

pub struct S3ClientConfig {
    pub tenant: TenantId,
    /// None reuses the runtime's current compatible S3 writer group.
    pub small_write: Option<SmallWritePolicy>,
    /// None shares a runtime-local key. Supply a stable key for cross-runtime cursors.
    pub continuation_key: Option<Vec<u8>>,
    /// None uses the current S3 small writer and the standard large-write policy.
    pub write_policy: Option<crowdb_access_s3::execution::S3WritePolicy>,
}

impl From<TenantId> for S3ClientConfig {
    fn from(tenant: TenantId) -> Self {
        Self {
            tenant,
            small_write: None,
            continuation_key: None,
            write_policy: None,
        }
    }
}
