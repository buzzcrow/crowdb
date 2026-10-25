// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::mgmt::node::{
    deployment::{DeploymentMode, NodeDeploymentOverrides, NodeDeploymentPolicy, PublicAccessUris},
    setup::{SetupServiceKind, SetupStorage},
};

#[test]
fn fresh_local_policy_has_one_voter_and_all_public_access_listeners() {
    let policy = NodeDeploymentOverrides::default()
        .resolve(&NodeDeploymentOverrides::default(), None)
        .unwrap();
    policy.validate().unwrap();
    assert_eq!(policy.initial_voters, 1);
    assert_eq!(policy.mode, DeploymentMode::Local);
    assert!(matches!(
        policy.setup.storage,
        SetupStorage::Automatic { sparse: true, .. }
    ));
    assert!(policy.setup.services.iter().all(|service| service.enabled));
    let access = policy
        .setup
        .services
        .iter()
        .find(|service| service.kind == SetupServiceKind::AccessServer)
        .unwrap();
    assert_eq!(access.listeners.s3_port, Some(9091));
    assert_eq!(access.listeners.http_port, Some(9092));
    assert_eq!(access.listeners.dataset_port, Some(9093));
}

#[test]
fn public_origins_resolve_per_field_and_survive_omitted_restart_inputs() {
    let file = NodeDeploymentOverrides {
        public_access: PublicAccessUris {
            s3: Some("http://127.0.0.1:19091".into()),
            iceberg: Some("https://catalog.example.test".into()),
            dataset: None,
        },
        ..Default::default()
    };
    let explicit = NodeDeploymentOverrides {
        public_access: PublicAccessUris {
            s3: Some("http://[::1]:29091".into()),
            dataset: Some("http://127.0.0.1:19093".into()),
            iceberg: None,
        },
        ..Default::default()
    };
    let accepted = explicit.resolve(&file, None).unwrap();
    assert_eq!(accepted.public_access.s3, explicit.public_access.s3);
    assert_eq!(accepted.public_access.iceberg, file.public_access.iceberg);
    let omitted = NodeDeploymentOverrides::default();
    assert_eq!(omitted.resolve(&omitted, Some(&accepted)).unwrap(), accepted);
    assert!(file.resolve(&omitted, Some(&accepted)).is_err());
    let environment = accepted.public_access.environment();
    assert_eq!(environment["CROWDB_DATASET_PUBLIC_URI"], "http://127.0.0.1:19093");
    assert_eq!(environment.len(), 3);
    assert_eq!(accepted.setup, NodeDeploymentPolicy::default().setup);
}

#[test]
fn malformed_or_secret_bearing_public_origins_never_resolve() {
    for uri in [
        "",
        "http:example.test",
        "http://",
        "ftp://example.test",
        "http://user:password@example.test",
        "http://example.test/path",
        "http://example.test?token=secret",
        "http://example.test#fragment",
        "http://example.test\n",
        "http://example.test:99999",
        "http://example.test\\path",
    ] {
        let inputs = NodeDeploymentOverrides {
            public_access: PublicAccessUris {
                s3: Some(uri.into()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            inputs.resolve(&NodeDeploymentOverrides::default(), None).is_err(),
            "{uri:?}"
        );
    }
}

#[test]
fn explicit_inputs_override_file_fields_without_erasing_other_file_choices() {
    let file = NodeDeploymentOverrides {
        cluster: Some("file-scope".into()),
        initial_voters: Some(3),
        ..Default::default()
    };
    let explicit = NodeDeploymentOverrides {
        cluster: Some("explicit-scope".into()),
        ..Default::default()
    };
    let policy = explicit.resolve(&file, None).unwrap();
    assert_eq!(policy.cluster, "explicit-scope");
    assert_eq!(policy.initial_voters, 3);
}

#[test]
fn omitted_restart_inputs_preserve_accepted_values_instead_of_new_defaults() {
    let mut accepted = NodeDeploymentPolicy {
        cluster: "retained-cluster".into(),
        initial_voters: 3,
        ..Default::default()
    };
    accepted.setup.services[5].listeners.dataset_port = Some(19093);
    let omitted = NodeDeploymentOverrides::default();
    assert_eq!(omitted.resolve(&omitted, Some(&accepted)).unwrap(), accepted);
    let changed = NodeDeploymentOverrides {
        initial_voters: Some(1),
        ..Default::default()
    };
    assert!(changed.resolve(&omitted, Some(&accepted)).is_err());
    assert!(omitted.resolve(&changed, Some(&accepted)).is_err());
    let unchanged = NodeDeploymentOverrides {
        initial_voters: Some(3),
        ..Default::default()
    };
    assert_eq!(unchanged.resolve(&changed, Some(&accepted)).unwrap(), accepted);
}

#[test]
fn production_uses_preallocated_storage_and_never_inherits_local_sparse_files() {
    let production = NodeDeploymentOverrides {
        mode: Some(DeploymentMode::Production),
        ..Default::default()
    };
    let policy = production
        .resolve(&NodeDeploymentOverrides::default(), None)
        .unwrap();
    assert!(matches!(
        policy.setup.storage,
        SetupStorage::Automatic { sparse: false, .. }
    ));
    let sparse = NodeDeploymentOverrides {
        storage: Some(NodeDeploymentPolicy::default().setup.storage),
        ..production
    };
    assert!(sparse.resolve(&NodeDeploymentOverrides::default(), None).is_err());
}

#[test]
fn invalid_policies_are_rejected_before_any_resolution_can_replace_them() {
    let valid = NodeDeploymentPolicy::default();
    for invalid in [
        NodeDeploymentPolicy {
            version: 2,
            ..valid.clone()
        },
        NodeDeploymentPolicy {
            initial_voters: 2,
            ..valid.clone()
        },
        NodeDeploymentPolicy {
            cluster: "../foreign".into(),
            ..valid.clone()
        },
        NodeDeploymentPolicy {
            cluster: String::new(),
            ..valid.clone()
        },
    ] {
        assert!(invalid.validate().is_err());
        assert!(NodeDeploymentOverrides::default()
            .resolve(&NodeDeploymentOverrides::default(), Some(&invalid))
            .is_err());
    }
    let mut duplicate_port = valid;
    duplicate_port.setup.services[5].listeners.dataset_port = Some(9092);
    assert!(duplicate_port.validate().is_err());
    duplicate_port.setup.services[5].listeners.dataset_port = None;
    assert!(duplicate_port.validate().is_err());
    for reserved in [2222, 9090, 9095, 10101] {
        let mut policy = NodeDeploymentPolicy::default();
        policy.setup.services[5].listeners.dataset_port = Some(reserved);
        assert!(policy.validate().is_err());
    }
    for (http, rpc) in [(10001, 10100), (10000, 10102)] {
        let mut policy = NodeDeploymentPolicy::default();
        let kv = policy
            .setup
            .services
            .iter_mut()
            .find(|service| service.kind == SetupServiceKind::PaxosKv)
            .unwrap();
        kv.listeners.http_port = Some(http);
        kv.listeners.rpc_port = Some(rpc);
        assert!(policy.validate().unwrap_err().contains("bootstrap KV"));
    }
}

#[test]
fn accepted_serialization_requires_explicit_version_and_complete_policy() {
    let policy = NodeDeploymentPolicy::default();
    let bytes = serde_json::to_vec(&policy).unwrap();
    assert_eq!(
        serde_json::from_slice::<NodeDeploymentPolicy>(&bytes).unwrap(),
        policy
    );
    assert!(serde_json::from_str::<NodeDeploymentPolicy>(r#"{"cluster":"default"}"#).is_err());
    let mut value = serde_json::to_value(policy).unwrap();
    value["password"] = serde_json::json!("unexpected");
    assert!(serde_json::from_value::<NodeDeploymentPolicy>(value).is_err());
    let current = NodeDeploymentOverrides {
        version: Some(1),
        ..Default::default()
    };
    let future = NodeDeploymentOverrides {
        version: Some(99),
        ..Default::default()
    };
    assert!(current.resolve(&future, None).is_err());
}
