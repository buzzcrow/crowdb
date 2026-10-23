// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::TenantId;
use crowdb_chunk_client::SmallWritePolicy;

pub struct S3ClientConfig {
    pub tenant: TenantId,
    /// None reuses the runtime's current compatible S3 writer group.
    pub small_write: Option<SmallWritePolicy>,
}

impl From<TenantId> for S3ClientConfig {
    fn from(tenant: TenantId) -> Self {
        Self {
            tenant,
            small_write: None,
        }
    }
}
