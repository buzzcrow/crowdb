// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/node_setup.rs"]
mod node_setup;
#[path = "common/service_receipt.rs"]
mod service_receipt;

use service_receipt::TestStoppedKv;
use std::time::Duration;

#[tokio::test]
async fn completed_kv_stop_can_report_its_receipt_after_authority_loss() {
    let mut test = TestStoppedKv::new().await;
    let before = std::fs::read(test.state()).unwrap();
    let receipt = tokio::time::timeout(
        Duration::from_millis(200),
        test.worker.execute_for_tests(test.intent.clone()),
    )
    .await
    .expect("reporting a completed stop must not wait for the authority it stopped")
    .unwrap();
    assert_eq!(receipt["operation_id"], test.intent.operation_id);
    assert!(receipt["pid"].is_null());
    assert_eq!(
        std::fs::read(test.state()).unwrap(),
        before,
        "a receipt must not repeat execution"
    );
}

#[tokio::test]
async fn incomplete_foreign_and_legacy_receipts_cannot_authorize_an_offline_operation() {
    let mut test = TestStoppedKv::new().await;
    let original: serde_json::Value = serde_json::from_slice(&std::fs::read(test.state()).unwrap()).unwrap();
    for fault in [
        "legacy",
        "foreign-bootstrap",
        "incomplete",
        "pid",
        "newer-operation",
    ] {
        let mut record = original.clone();
        match fault {
            "legacy" => {
                record.as_object_mut().unwrap().remove("bootstrap");
            }
            "foreign-bootstrap" => {
                record["bootstrap"]["operation_id"] = uuid::Uuid::new_v4().to_string().into();
            }
            "incomplete" => record["complete"] = false.into(),
            "pid" => record["pid"] = 123.into(),
            "newer-operation" => record["intent"]["operation_id"] = uuid::Uuid::new_v4().to_string().into(),
            _ => unreachable!(),
        }
        let bytes = serde_json::to_vec(&record).unwrap();
        std::fs::write(test.state(), &bytes).unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            test.worker.execute_for_tests(test.intent.clone()),
        )
        .await;
        assert!(
            !matches!(result, Ok(Ok(_))),
            "invalid {fault} receipt acknowledged without authority"
        );
        assert_eq!(std::fs::read(test.state()).unwrap(), bytes);
    }
}

#[tokio::test]
async fn a_completed_receipt_cannot_stop_a_live_replacement_without_authority() {
    let mut test = TestStoppedKv::new().await;
    let pid = test.worker.start_kv_child_for_tests().await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        test.worker.execute_for_tests(test.intent.clone()),
    )
    .await;
    assert!(
        !matches!(result, Ok(Ok(_))),
        "a live replacement requires current authorization"
    );
    assert!(std::path::Path::new(&format!("/proc/{pid}")).exists());
    test.worker.stop_kv_for_tests().await.unwrap();
}

#[tokio::test]
async fn retired_bootstrap_does_not_report_a_completed_receipt() {
    let mut test = TestStoppedKv::new().await;
    let record: serde_json::Value = serde_json::from_slice(&std::fs::read(test.state()).unwrap()).unwrap();
    let operation = record["bootstrap"]["operation_id"].as_str().unwrap();
    std::fs::write(
        test.directory
            .path()
            .join(format!("retired-bootstrap-{operation}.json")),
        b"{}",
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        test.worker.execute_for_tests(test.intent.clone()),
    )
    .await;
    assert!(
        !matches!(result, Ok(Ok(_))),
        "retired local authority cannot serve the receipt"
    );
}
