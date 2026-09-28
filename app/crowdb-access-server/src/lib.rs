// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Independent listener lifecycle for external access protocols.

pub mod config;
mod http_receive;
pub mod iceberg;
mod multipart_complete;

#[cfg(feature = "s3")]
pub mod credentials;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "s3")]
pub mod storage;
