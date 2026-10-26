// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

mod background;
mod creation;
mod dropping;
mod fault;
use super::frontend;
mod journal;
mod namespace;
mod property;
mod scan;
use super::{
    native::{self, TestNativeStorage},
    now_ms,
};
use crowdb_access_iceberg::catalog::{
    CatalogAuthority, CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege,
};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};

fn request(
    action: ManagementAction,
    name: &str,
    previous: Option<(u64, &CatalogAuthority)>,
) -> ManagementRequest {
    ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(
                &(u128::from(previous.map_or(0, |(epoch, _)| epoch)) * 8 + u128::from(action as u8) + 1)
                    .to_be_bytes(),
            )
            .unwrap(),
            issued_ms: now_ms(),
        },
        principal: "clearer".into(),
        action,
        display_name: name.into(),
        expected_epoch: previous.map_or(0, |(epoch, _)| epoch),
        confirmation: previous
            .filter(|_| action == ManagementAction::Clear)
            .map(|(_, authority)| authority.catalog),
        capabilities: None,
    }
}

async fn execute(repository: &CatalogRepository, request: ManagementRequest) -> CatalogAuthority {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match repository
                .execute(request.clone(), ManagementPrivilege::Clear, now_ms())
                .await
            {
                Ok(authority) => return authority,
                Err(CatalogError::Busy) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(error) => panic!("management failed: {error:?}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release native catalog"]
async fn catalog_recovery_survives_real_chunk_kv_restart() {
    let mut stack = TestNativeStorage::new();
    let bounds = ClearBounds {
        request_ms: 500,
        root_lease_ms: 0,
        delegated_access_ms: 0,
        clock_skew_ms: 10,
    };
    let repository = Arc::new(CatalogRepository::new(stack.store().await, bounds).unwrap());
    let initialize = request(ManagementAction::Initialize, "original", None);
    let original = execute(&repository, initialize.clone()).await;
    native::activate(&repository).await;
    let frontend = frontend::TestFrontend::start(12).await;
    let second_frontend = frontend::TestFrontend::start(13).await;
    frontend.check_official_reads();
    second_frontend.check_official_reads();
    let denied = frontend::command(&stack.seeds)
        .env("CROWDB_ICEBERG_TOKEN", "r".repeat(32))
        .arg("status")
        .output()
        .unwrap();
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stderr).contains("management privilege"));
    for action in ["status", "initialize", "rename", "clear"] {
        let denied = frontend::command(&stack.seeds)
            .env("CROWDB_ICEBERG_TOKEN", "w".repeat(32))
            .arg(action)
            .output()
            .unwrap();
        assert!(!denied.status.success());
        assert!(String::from_utf8_lossy(&denied.stderr).contains("management privilege"));
    }
    let rename = request(ManagementAction::Rename, "renamed", Some((1, &original)));
    let renamed = execute(&repository, rename).await;
    assert_eq!(renamed.catalog, original.catalog);
    let clear = request(ManagementAction::Clear, "replacement", Some((1, &renamed)));
    assert!(matches!(
        repository
            .execute(clear.clone(), ManagementPrivilege::Clear, now_ms())
            .await,
        Err(CatalogError::Busy)
    ));
    drop(repository);
    stack.restart_catalog().await;
    let repository = CatalogRepository::new(stack.store().await, bounds).unwrap();
    let (recovering, _) = repository.status().await.unwrap();
    if let crowdb_access_iceberg::catalog::RootState::Published(transition) = recovering.state {
        let remaining = transition.complete_after_ms.saturating_sub(now_ms());
        tokio::time::sleep(Duration::from_millis(remaining)).await;
    }
    repository.recover(now_ms()).await.unwrap();
    let replacement = execute(&repository, clear.clone()).await;
    assert_ne!(replacement.catalog, original.catalog);
    let second = request(ManagementAction::Clear, "second", Some((2, &replacement)));
    let latest = execute(&repository, second).await;
    assert_ne!(latest.catalog, replacement.catalog);
    assert_eq!(execute(&repository, clear).await, replacement);
    assert_eq!(execute(&repository, initialize).await, original);
    assert_eq!(repository.status().await.unwrap().0.context.activation_epoch, 3);
    native::activate(&repository).await;
    scan::verify_legacy_records(&stack, &repository).await;
    frontend.stop().await;
    second_frontend.stop().await;
    namespace::verify_name_index(&stack, latest.catalog).await;
    let frontend = frontend::TestFrontend::start(12).await;
    let second_frontend = frontend::TestFrontend::start(13).await;
    background::verify(stack.store().await, repository.status().await.unwrap().0.context).await;
    frontend.check_official_reads();
    second_frontend.check_official_reads();
    frontend.stop().await;
    second_frontend.stop().await;
    journal::verify_recovery(&mut stack, repository.status().await.unwrap().0.context).await;
    verify_interrupted_clear(&stack, &repository).await;
    native::activate(&repository).await;
    let frontend = frontend::TestFrontend::start(12).await;
    let second_frontend = frontend::TestFrontend::start(13).await;
    frontend.check_official_reads();
    second_frontend.check_official_reads();
}

async fn verify_interrupted_clear(stack: &TestNativeStorage, repository: &CatalogRepository) {
    for mode in [1, 2] {
        let (root, authority) = repository.status().await.unwrap();
        let clear = request(
            ManagementAction::Clear,
            "recovered",
            Some((root.context.activation_epoch, &authority)),
        );
        let faulty = CatalogRepository::new(
            Arc::new(fault::TestFaultStore {
                inner: stack.store().await,
                mode: std::sync::atomic::AtomicU8::new(mode),
            }),
            authority.admission_bounds,
        )
        .unwrap();
        assert!(matches!(
            faulty
                .execute(clear.clone(), ManagementPrivilege::Clear, now_ms())
                .await,
            Err(CatalogError::Store(_))
        ));
        drop(faulty);
        let recovered = execute(repository, clear.clone()).await;
        assert_ne!(recovered.catalog, authority.catalog);
        assert_eq!(
            repository.status().await.unwrap().0.context.activation_epoch,
            root.context.activation_epoch + 1
        );
        assert_eq!(execute(repository, clear).await, recovered);
    }
}
