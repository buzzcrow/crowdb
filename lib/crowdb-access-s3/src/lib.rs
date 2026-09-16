// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! S3 request compatibility boundary and service components.

pub mod auth;
pub mod bucket;
pub mod condition;
pub mod continuation;
pub mod error;
pub mod integrity;
pub mod metadata;
pub mod metrics;
pub mod native_buffer;
pub mod object;
pub mod publication;
pub mod range;
pub mod retrieval;
pub mod route;
pub mod streaming;
pub mod wire;

pub use error::{S3Error, S3ErrorCode};
