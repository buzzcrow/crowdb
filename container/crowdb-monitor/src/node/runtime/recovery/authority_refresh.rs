// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared deadline for recovered-route publication and optional contact refresh.

use super::super::Result;
use std::future::Future;
use std::time::Duration;

pub(super) async fn run(
    publication: impl Future<Output = Result<()>>,
    confirmation: impl Future<Output = Result<()>>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    // Recovered routes must reach current authority before optional reads
    // through retained contacts can consume the shared deadline.
    match tokio::time::timeout_at(deadline, publication).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "recovered endpoint publication awaits authority"),
        Err(_) => tracing::debug!("recovered endpoint publication yielded at its deadline"),
    }
    match tokio::time::timeout_at(deadline, confirmation).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "confirmed recovery contacts await current authority"),
        Err(_) => tracing::debug!("confirmed recovery contacts yielded at their deadline"),
    }
}

#[cfg(feature = "test-util")]
pub async fn recovery_authority_work_for_tests(
    publication: impl Future<Output = Result<()>>,
    confirmation: impl Future<Output = Result<()>>,
) {
    run(publication, confirmation).await;
}
