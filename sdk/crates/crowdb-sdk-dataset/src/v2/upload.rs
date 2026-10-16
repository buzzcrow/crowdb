// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{Decimal, OpaqueId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateUpload {
    pub id: OpaqueId,
    pub length: Decimal,
    pub part_size: u32,
    pub md5: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadPart {
    pub index: u32,
    pub length: Decimal,
    pub md5: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadStatus {
    pub id: OpaqueId,
    pub length: Decimal,
    pub part_size: u32,
    pub md5: String,
    pub state: crate::UploadState,
    pub expires_at: Decimal,
    pub cleanup_complete: bool,
    pub parts: Vec<UploadPart>,
}
