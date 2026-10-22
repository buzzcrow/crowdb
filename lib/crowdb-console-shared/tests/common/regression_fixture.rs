// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit test-owned native environment for regression measurement scripts.

mod regression;

#[tokio::main]
async fn main() -> regression::TestResult<()> {
    regression::run().await
}
