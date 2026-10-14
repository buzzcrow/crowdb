// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/iceberg_file_recovery/allocator.rs"]
mod allocator;
#[path = "common/iceberg_file_recovery/blocks.rs"]
mod blocks;
#[path = "common/iceberg_commit_recovery/listener.rs"]
mod child;
#[path = "common/iceberg_commit_recovery/fixture.rs"]
mod common;
#[path = "common/iceberg_file_recovery/disk.rs"]
mod disk;
#[path = "common/iceberg_commit_fault.rs"]
mod fault;
#[path = "common/iceberg_store.rs"]
mod memory;
#[path = "common/iceberg_file_recovery/component.rs"]
mod recovery;
#[path = "common/iceberg_commit_recovery/store.rs"]
mod store;

use common::{now_ms, TestCommitFixture};
use crowdb_access_iceberg::catalog::{CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::file::{FileGrant, FileGrantIssuer, FileOperation, FileOperations, TableLocation};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use reqwest::Client;

#[path = "common/iceberg_signed_file.rs"]
mod signed;
use signed::TestFileClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_publication_recovers_across_listeners_at_every_durable_write() {
    recovery::run().await;
}

async fn setup_with_bounds(
    bounds: ClearBounds,
) -> (
    TestCommitFixture,
    child::TestCommitListener,
    TestFileClient,
    TableLocation,
) {
    setup_with_bounds_and_file_limit(bounds, 16 * 1024 * 1024, 64 * 1024 * 1024).await
}

async fn setup_with_bounds_and_file_limit(
    bounds: ClearBounds,
    max_request_bytes: u64,
    max_file_bytes: u64,
) -> (
    TestCommitFixture,
    child::TestCommitListener,
    TestFileClient,
    TableLocation,
) {
    let stack = TestCommitFixture::new();
    let repository = CatalogRepository::new(stack.store(), bounds).unwrap();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now_ms(),
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "file-http".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now_ms(),
        )
        .await
        .unwrap();
    common::activate(&repository).await;
    let context = repository.status().await.unwrap().0.context;
    let process = child::TestCommitListener::normal(&stack).await;
    let client = Client::new();
    let endpoint = format!("http://{}", process.address);
    let namespace = client
        .post(format!("{endpoint}/v1/namespaces"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"namespace": ["analytics"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(namespace.status(), 200, "{}", namespace.text().await.unwrap());
    let draft = client
        .post(format!("{endpoint}/v1/namespaces/analytics/tables"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"name": "files", "stage-create": true,
            "schema": {"type": "struct", "fields": []}}))
        .send()
        .await
        .unwrap();
    assert_eq!(draft.status(), 200, "{}", draft.text().await.unwrap());
    let draft: serde_json::Value = draft.json().await.unwrap();
    assert_eq!(draft["config"]["s3.endpoint"], endpoint);
    let table: TableLocation = format!("{}/", draft["metadata"]["location"].as_str().unwrap())
        .parse()
        .unwrap();
    let authenticator =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(authenticator.namespace_token_key(), 15 * 60 * 1000).unwrap();
    let started = now_ms();
    let credentials = issuer
        .issue(FileGrant {
            storage_namespace: None,
            context,
            table: table.table,
            principal: [7; 32],
            nonce: OperationId::random(),
            issued_ms: started - 1_000,
            expires_ms: started + 10 * 60 * 1000,
            operations: FileOperations::new(&[
                FileOperation::Head,
                FileOperation::Get,
                FileOperation::ListObjects,
                FileOperation::Put,
                FileOperation::CreateMultipart,
                FileOperation::UploadPart,
                FileOperation::ListParts,
                FileOperation::CompleteMultipart,
                FileOperation::AbortMultipart,
            ])
            .unwrap(),
            max_request_bytes,
            max_file_bytes,
        })
        .unwrap();
    let client = TestFileClient {
        client,
        credentials,
        address: process.address,
    };
    (stack, process, client, table)
}

fn path(table: TableLocation, key: &str) -> String {
    format!("/{}/{}", table.bucket(), table.file(key).unwrap().object_key())
}
