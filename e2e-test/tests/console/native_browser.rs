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
