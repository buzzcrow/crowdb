// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Native S3 transport and signing; deployment belongs to the node lifecycle.

mod client;
mod exchange;
mod signing;

pub use client::{S3Credentials, S3HttpClient};
pub use exchange::{S3HttpExchange, S3HttpRequest};
pub use signing::sign_request;
