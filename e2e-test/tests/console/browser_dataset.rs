// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

async fn run(browser_phase: &str) {
    let phase = std::env::var("CROWDB_E2E_PHASE").expect("owned Dataset client phase");
    assert!(
        phase == "prepare" || phase == browser_phase,
        "unexpected Dataset phase: {phase}"
    );
    let context = super::native_context::TestNativeContext::connect().await;
    if phase == "prepare" {
        super::native_storage::start(&context).await;
        // Finish the full production placement policy in the preparation phase.
        super::native_balance::settle(&context).await;
        return;
    }
    super::native_balance::settle_existing(&context).await;
    let result = crate::common::control::native_browser().await;
    assert_eq!(result["passed"], true);
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn browse() {
    run("browse").await;
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn import() {
    run("import").await;
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn split_revisions() {
    run("split_revisions").await;
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn recovery() {
    run("recovery").await;
}
#[tokio::test]
#[ignore = "requires native Dataset and installed browser"]
async fn capabilities() {
    run("capabilities").await;
}
