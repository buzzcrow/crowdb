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

#[tokio::test]
#[ignore = "requires actual stream extent directories and a host browser"]
async fn large_journal_pagination_and_restart() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start_with_geometry(&context, Some((1024 * 1024, 1))).await;
    super::native_journal::TestNativeJournal::seed(&context).await;
    let result = crate::common::control::native_browser().await;
    assert_eq!(result["passed"], true);
}

#[tokio::test]
#[ignore = "requires normal storage and actual scanner/recalc observations"]
async fn scanner_and_scoped_recalc() {
    browse_storage().await;
}

#[tokio::test]
#[ignore = "requires normal DiskDB metadata and a host capacity browser"]
async fn hardware_capacity_and_dashed_status() {
    browse_diskdb().await;
}

#[tokio::test]
#[ignore = "requires actual DiskDB owners and host maintenance menus"]
async fn disk_maintenance_and_status_badges() {
    browse_diskdb().await;
}

async fn browse_diskdb() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start_diskdb(&context, [128 * 1024; 3]).await;
    let result = crate::common::control::native_browser().await;
    assert_eq!(result["passed"], true);
}

#[tokio::test]
#[ignore = "requires normal storage and owned container PID observations"]
async fn auxiliary_process_menus() {
    browse_storage().await;
}

#[tokio::test]
#[ignore = "requires normal storage and host deployment dialogs"]
async fn auxiliary_dialog_defaults() {
    browse_storage().await;
}

#[tokio::test]
#[ignore = "requires normal storage and durable plan reconciliation"]
async fn interrupted_plan_reconciliation() {
    browse_storage().await;
}

#[tokio::test]
#[ignore = "requires a partial normal deployment and arriving DiskIO prerequisites"]
async fn waiting_plan_prerequisites() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start_prerequisites(&context).await;
    let result = crate::common::control::native_browser().await;
    assert_eq!(result["passed"], true);
}

#[tokio::test]
#[ignore = "requires production count balancing and an actual split overlay browser"]
async fn production_split_and_transfer_journals() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start(&context).await;
    let ((), browser) = tokio::join!(
        super::native_balance::settle(&context),
        crate::common::control::native_transition(),
    );
    assert_eq!(browser["passed"], true);
}
