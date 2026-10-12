// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::native_context::TestNativeContext;

#[tokio::test]
#[ignore = "requires three formal nodes and an installed host browser"]
async fn multipart_upload_and_physical_navigation() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start(&context).await;
    let result = crate::common::control::native_browser().await;
    assert_eq!(result["passed"], true);
}

async fn browse_storage() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start(&context).await;
    let result = crate::common::control::native_browser().await;
    assert_eq!(result["passed"], true);
}

#[tokio::test]
#[ignore = "requires three formal nodes and an installed host browser"]
async fn chunk_ownership_scope() {
    browse_storage().await;
}

#[tokio::test]
#[ignore = "requires normal storage and a host browser for owner recovery"]
async fn journal_identity_and_owner_recovery() {
    browse_storage().await;
}

#[tokio::test]
#[ignore = "requires normal storage, production balance and an installed host browser"]
async fn page_layout_and_iceberg_inspection() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start(&context).await;
    super::native_balance::settle(&context).await;
    let chunks = super::native_chunks::seed(&context).await;
    let result = crate::common::control::native_inspection(chunks).await;
    assert_eq!(result["passed"], true);
}
