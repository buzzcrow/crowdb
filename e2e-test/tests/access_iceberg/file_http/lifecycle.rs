// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::{CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege, RootState},
    file::{FileGrantIssuer, TableLocation},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
    wire::BearerAuthenticator,
};
use reqwest::{Client, Method};
use serde_json::json;

use super::super::{frontend::TestFrontend, now_ms, signed_file::TestFileClient};
use super::setup::{path, setup_with_bounds};
mod grants;
use grants::{create, expire, location, refresh, rest, value};

const NAME: &str = "/v1/namespaces/analytics/tables/grants";
const RENAMED: &str = "/v1/namespaces/analytics/tables/renamed";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn native_file_credentials_refresh_expire_and_follow_lifecycle_fences() {
    let bounds = ClearBounds {
        request_ms: 300_000,
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    };
    let (stack, first, bootstrap, _) = setup_with_bounds(bounds).await;
    let second = TestFrontend::start(13).await;
    let endpoint = format!("http://{}", first.address());
    let other = format!("http://{}", second.address());
    let context = bootstrap.credentials.grant().context;
    let authentication =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(authentication.namespace_token_key(), 900_000).unwrap();
    let created = create(&endpoint).await;
    let table = location(&created);
    let first_grant = refresh(&endpoint, NAME, "w", &issuer, context).await;
    let next_grant = refresh(&other, NAME, "w", &issuer, context).await;
    assert_ne!(first_grant.access_key_id(), next_grant.access_key_id());
    let writer = TestFileClient {
        client: Client::new(),
        credentials: next_grant,
        address: second.address(),
    };
    let object = path(table, "metadata/delegated.json");
    let bytes = br#"{"delegated":true}"#;
    assert_eq!(
        writer.send(Method::PUT, &object, "", bytes, false).await.status(),
        200
    );
    let previous = TestFileClient {
        client: Client::new(),
        credentials: first_grant,
        address: first.address(),
    };
    assert_eq!(
        previous
            .send(Method::GET, &object, "", b"", false)
            .await
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        bytes
    );
    let reader = TestFileClient {
        client: Client::new(),
        credentials: refresh(&other, NAME, "r", &issuer, context).await,
        address: second.address(),
    };
    assert_eq!(
        reader.send(Method::GET, &object, "", b"", false).await.status(),
        200
    );
    assert_eq!(
        reader
            .send(
                Method::PUT,
                &path(table, "metadata/forbidden.json"),
                "",
                bytes,
                false
            )
            .await
            .status(),
        403
    );
    expire(&writer, &issuer, &object).await;
    rename_drop(&endpoint, &other, &writer, &issuer, table, &object).await;
    let repository = CatalogRepository::new(stack.store().await, bounds).unwrap();
    clear(&repository, &writer, &previous, &other, &object).await;
}

async fn rename_drop(
    endpoint: &str,
    other: &str,
    writer: &TestFileClient,
    issuer: &FileGrantIssuer,
    table: TableLocation,
    object: &str,
) {
    let context = writer.credentials.grant().context;
    let bytes = br#"{"delegated":true}"#;
    value(
        rest(
            endpoint,
            Method::POST,
            "/v1/tables/rename",
            "w",
            Some(json!({
                "source":{"namespace":["analytics"],"name":"grants"},
                "destination":{"namespace":["analytics"],"name":"renamed"}
            })),
        )
        .await,
        204,
    )
    .await;
    assert_eq!(
        rest(other, Method::GET, &format!("{NAME}/credentials"), "w", None)
            .await
            .status(),
        404
    );
    let renamed = refresh(other, RENAMED, "w", issuer, context).await;
    assert_eq!(renamed.grant().table, writer.credentials.grant().table);
    assert_eq!(
        writer.send(Method::GET, object, "", b"", false).await.status(),
        200
    );
    value(rest(endpoint, Method::DELETE, RENAMED, "w", None).await, 204).await;
    assert_eq!(
        rest(other, Method::GET, &format!("{RENAMED}/credentials"), "w", None)
            .await
            .status(),
        404
    );
    assert_eq!(
        writer.send(Method::GET, object, "", b"", false).await.status(),
        200
    );
    let recreated = create(other).await;
    let replacement = location(&recreated);
    assert_ne!(replacement.table, table.table);
    assert_eq!(
        writer
            .send(
                Method::PUT,
                &path(replacement, "metadata/foreign.json"),
                "",
                bytes,
                false
            )
            .await
            .status(),
        403
    );
    let fresh = refresh(other, NAME, "w", issuer, context).await;
    let fresh = TestFileClient {
        client: Client::new(),
        credentials: fresh,
        address: writer.address,
    };
    assert_eq!(
        fresh.send(Method::GET, object, "", b"", false).await.status(),
        403
    );
}

async fn clear(
    repository: &CatalogRepository,
    writer: &TestFileClient,
    previous: &TestFileClient,
    other: &str,
    object: &str,
) {
    let context = writer.credentials.grant().context;
    let request = ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: now_ms(),
        },
        principal: "clearer".into(),
        action: ManagementAction::Clear,
        expected_epoch: context.activation_epoch,
        display_name: "after-clear".into(),
        confirmation: Some(context.catalog),
        capabilities: None,
    };
    assert!(matches!(
        repository
            .execute(request, ManagementPrivilege::Clear, now_ms())
            .await,
        Err(CatalogError::Busy)
    ));
    assert_ne!(repository.status().await.unwrap().0.state, RootState::Ready);
    assert_eq!(
        writer.send(Method::GET, object, "", b"", false).await.status(),
        503
    );
    assert_eq!(
        previous.send(Method::GET, object, "", b"", false).await.status(),
        503
    );
    assert_eq!(
        rest(other, Method::GET, &format!("{NAME}/credentials"), "w", None)
            .await
            .status(),
        503
    );
}
