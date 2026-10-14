// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared, bounded HTTP transport for `CrowDB` SDKs.

mod admission;
mod config;
mod error;
mod http;

pub use config::HttpConfig;
pub use error::{Error, ServiceError, TransportError};
pub use http::HttpClient;
