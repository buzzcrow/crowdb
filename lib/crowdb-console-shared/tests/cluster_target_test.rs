// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/cluster_target.rs"]
mod common;

use common::{identity, TestTargetServer};
use crowdb_console_shared::deployment::target::{
    resolve, ClusterTargetBinding, RuntimeOwnership, RuntimeSelection, TargetError,
};
use std::sync::atomic::Ordering;

fn binding(server: &TestTargetServer, id: u128) -> ClusterTargetBinding {
    ClusterTargetBinding {
        version: 1,
        name: "demo".into(),
        cluster_id: uuid::Uuid::from_u128(id).to_string(),
        management_endpoints: vec![server.origin.clone()],
        runtime: None,
    }
}

#[tokio::test]
async fn endpoint_only_attach_observes_identity_without_runtime_ownership() {
    let server = TestTargetServer::start(identity(1)).await;
    let selected = resolve(None, &[server.origin.clone(), server.origin.clone()], None)
        .await
        .unwrap();
    assert_eq!(selected.identity, identity(1));
    assert_eq!(selected.management_endpoints, vec![server.origin.clone()]);
    assert!(selected.runtime.is_none());
    assert_eq!(server.requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn named_binding_and_explicit_addresses_must_observe_the_same_uuid() {
    let original = TestTargetServer::start(identity(1)).await;
    let replacement = TestTargetServer::start(identity(2)).await;
    let binding = binding(&original, 1);
    assert!(resolve(Some(&binding), &[], None).await.is_ok());
    assert!(matches!(
        resolve(Some(&binding), std::slice::from_ref(&replacement.origin), None).await,
        Err(TargetError::IdentityConflict)
    ));
    // Explicit addresses replace the saved address list, but never its UUID.
    assert_eq!(original.requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn mixed_uuid_endpoints_fail_even_without_a_named_binding() {
    let left = TestTargetServer::start(identity(1)).await;
    let right = TestTargetServer::start(identity(2)).await;
    assert!(matches!(
        resolve(None, &[left.origin.clone(), right.origin.clone()], None).await,
        Err(TargetError::IdentityConflict)
    ));
    assert_eq!(left.requests.load(Ordering::Relaxed), 1);
    assert_eq!(right.requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn runtime_endpoint_or_context_change_fails_before_contacting_application() {
    let server = TestTargetServer::start(identity(1)).await;
    let mut binding = binding(&server, 1);
    let selection = RuntimeSelection {
        runtime: "docker".into(),
        endpoint: "unix:///var/run/docker.sock".into(),
        context: "default".into(),
    };
    binding.runtime = Some(RuntimeOwnership {
        selection: selection.clone(),
        owner: uuid::Uuid::new_v4().to_string(),
    });
    for replacement in [
        None,
        Some(RuntimeSelection {
            context: "remote".into(),
            ..selection.clone()
        }),
        Some(RuntimeSelection {
            endpoint: "ssh://remote".into(),
            ..selection.clone()
        }),
    ] {
        assert!(matches!(
            resolve(Some(&binding), &[], replacement.as_ref()).await,
            Err(TargetError::RuntimeConflict)
        ));
    }
    assert_eq!(server.requests.load(Ordering::Relaxed), 0);
    let selected = resolve(Some(&binding), &[], Some(&selection)).await.unwrap();
    assert_eq!(selected.runtime, binding.runtime);
}

#[tokio::test]
async fn missing_and_malformed_targets_never_fall_back_to_localhost() {
    assert!(matches!(
        resolve(None, &[], None).await,
        Err(TargetError::Missing)
    ));
    for origin in [
        "http://user:secret@127.0.0.1:9090",
        "http://127.0.0.1:9090/api",
        "file:///tmp/target",
        "http://127.0.0.1:9090?token=secret",
        "http://127.0.0.1:0",
    ] {
        let error = resolve(None, &[origin.into()], None).await.unwrap_err();
        assert!(matches!(error, TargetError::Invalid));
        assert!(!error.to_string().contains("secret"));
    }
}

#[tokio::test]
async fn unreachable_explicit_target_never_uses_a_reachable_saved_address() {
    let server = TestTargetServer::start(identity(1)).await;
    let binding = binding(&server, 1);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let closed = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    assert!(matches!(
        resolve(Some(&binding), &[closed], None).await,
        Err(TargetError::Unavailable)
    ));
    assert_eq!(server.requests.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn malformed_identity_or_native_address_is_not_accepted() {
    for case in 0..5 {
        let mut value = identity(1);
        match case {
            0 => value.version = 2,
            1 => value.cluster_id = uuid::Uuid::nil().to_string(),
            2 => value.group0_management_endpoints = vec![],
            3 => value.group0_rpc_endpoints = vec!["http://127.0.0.1:10100".into()],
            _ => value.group0_rpc_endpoints = vec!["127.0.0.1:10100/path".into()],
        }
        let server = TestTargetServer::start(value).await;
        assert!(
            matches!(
                resolve(None, std::slice::from_ref(&server.origin), None).await,
                Err(TargetError::Invalid)
            ),
            "case {case}"
        );
    }
}

#[tokio::test]
async fn http_attach_and_native_transport_reachability_are_separate() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut value = identity(1);
    value.group0_rpc_endpoints = vec![listener.local_addr().unwrap().to_string()];
    let server = TestTargetServer::start(value).await;
    let selected = resolve(None, std::slice::from_ref(&server.origin), None)
        .await
        .unwrap();
    selected.require_native_transport().await.unwrap();
    // Drain the prior connection so its close cannot affect this assertion.
    drop(listener.accept().await.unwrap());
    drop(listener);
    assert!(matches!(
        selected.require_native_transport().await,
        Err(TargetError::RpcUnreachable)
    ));
    assert!(resolve(None, std::slice::from_ref(&server.origin), None)
        .await
        .is_ok());
}
