// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Capability-gated control API. Unsupported operations never fall back to v1.

use super::{json, read_body, ApiError, ApiResult, DatasetHttpService};
use hyper::{body::Incoming, Method, Request};
use serde::Serialize;

#[derive(Serialize)]
struct Capabilities {
    api_version: u16,
    capabilities: &'static [&'static str],
}

impl DatasetHttpService {
    pub(super) async fn route_v2(&self, parts: &[String], request: Request<Incoming>) -> ApiResult {
        if parts == ["api", "dataset", "v2", "capabilities"] && request.method() == Method::GET {
            read_body(request, false).await?;
            return json(&Capabilities {
                api_version: 2,
                // Advertise each operation family only after its authority contract is implemented.
                capabilities: &[],
            });
        }
        Err(ApiError::not_found())
    }
}
