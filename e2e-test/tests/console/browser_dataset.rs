// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

async fn run() {
    let context = super::native_context::TestNativeContext::connect().await;
    super::native_storage::start(&context).await;
    let result = crate::common::control::native_browser().await;
    assert_eq!(result["passed"], true);
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn browse() {
    run().await;
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn import() {
    run().await;
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn split_revisions() {
    run().await;
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn recovery() {
    run().await;
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn capabilities() {
    run().await;
}
