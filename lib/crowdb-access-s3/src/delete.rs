// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Whole-request validation followed by bounded independent logical deletions.

mod integrity;
mod selection;

pub use integrity::validate_integrity;
pub use selection::DeleteSelection;

pub const MAX_DELETE_BODY: usize = 2 * 1024 * 1024;
pub type DeleteResults = Vec<(Vec<u8>, Result<(), crate::error::S3ErrorCode>)>;
