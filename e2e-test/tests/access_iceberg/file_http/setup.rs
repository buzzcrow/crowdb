// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    frontend::TestFrontend,
    native::{activate, TestNativeStorage},
    now_ms,
    signed_file::TestFileClient,
};
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    file::{FileGrant, FileGrantIssuer, FileOperation, FileOperations, TableLocation},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
    wire::BearerAuthenticator,
};
use reqwest::Client;
pub(super) async fn setup() -> (TestNativeStorage, TestFrontend, TestFileClient, TableLocation) {
    setup_with_bounds(ClearBounds {
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    })
    .await
}

pub(super) async fn setup_with_bounds(
    bounds: ClearBounds,
) -> (TestNativeStorage, TestFrontend, TestFileClient, TableLocation) {
    setup_with_bounds_and_file_limit(bounds, 16 * 1024 * 1024, 64 * 1024 * 1024).await
}

pub(super) async fn setup_with_bounds_and_file_limit(
    bounds: ClearBounds,
    max_request_bytes: u64,
    max_file_bytes: u64,
) -> (TestNativeStorage, TestFrontend, TestFileClient, TableLocation) {
    let stack = TestNativeStorage::new();
    let repository = CatalogRepository::new(stack.store().await, bounds).unwrap();
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
    activate(&repository).await;
    let context = repository.status().await.unwrap().0.context;
    let process = TestFrontend::start(12).await;
    let client = Client::new();
    let endpoint = process.origin.clone();
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
    assert_eq!(process.address().port(), 7502);
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
        address: process.address(),
    };
    (stack, process, client, table)
}

pub(super) fn path(table: TableLocation, key: &str) -> String {
    format!("/{}/{}", table.bucket(), table.file(key).unwrap().object_key())
}

pub(super) async fn cleanup_client(
    stack: &TestNativeStorage,
    client: &TestFileClient,
    table: TableLocation,
) -> TestFileClient {
    let context = CatalogRepository::new(
        stack.store().await,
        ClearBounds {
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap()
    .status()
    .await
    .unwrap()
    .0
    .context;
    let authenticator =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(authenticator.namespace_token_key(), 15 * 60 * 1000).unwrap();
    let started = now_ms();
    TestFileClient {
        client: client.client.clone(),
        credentials: issuer
            .issue(FileGrant {
                storage_namespace: None,
                context,
                table: table.table,
                principal: [9; 32],
                nonce: OperationId::random(),
                issued_ms: started - 1_000,
                expires_ms: started + 10 * 60 * 1000,
                operations: FileOperations::new(&[FileOperation::DeleteObject, FileOperation::DeleteObjects])
                    .unwrap(),
                max_request_bytes: 1024 * 1024,
                max_file_bytes: 1024 * 1024,
            })
            .unwrap(),
        address: client.address,
    }
}

pub(super) fn fixture_config() -> crowdb_access_server::config::AccessConfig {
    crowdb_common::config::load_from_file(
        &std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap()).join("iceberg.toml"),
    )
    .unwrap()
}
