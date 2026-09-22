use crowdb_access_iceberg::catalog::ManagementPrivilege;
use crowdb_access_iceberg::wire::{BearerAuthenticator, CatalogConfig};

#[test]
fn config_advertises_only_landed_support_and_rejects_nonempty_warehouse() {
    for warehouse in [None, Some("")] {
        let value = serde_json::to_value(CatalogConfig::foundation(warehouse).unwrap()).unwrap();
        assert_eq!(value["endpoints"], serde_json::json!([]));
        assert!(value.get("idempotency-key-lifetime").is_none());
        assert!(value["overrides"]
            .as_object()
            .unwrap()
            .values()
            .all(|value| value == "false"));
    }
    let error = CatalogConfig::foundation(Some("warehouse")).unwrap_err();
    let value = serde_json::to_value(error).unwrap();
    assert_eq!(value["error"]["code"], 404);
    assert_eq!(value["error"]["type"], "NoSuchWarehouseException");
}

#[test]
fn bearer_tokens_separate_namespace_writes_from_management() {
    let reader = "r".repeat(32);
    let writer = "w".repeat(32);
    let manager = "m".repeat(32);
    let clearer = "c".repeat(32);
    let auth = BearerAuthenticator::new(&reader, &writer, &manager, &clearer).unwrap();
    for (token, name, privilege, namespace_write) in [
        (reader.as_str(), "reader", ManagementPrivilege::None, false),
        (writer.as_str(), "writer", ManagementPrivilege::None, true),
        (manager.as_str(), "manager", ManagementPrivilege::Manage, false),
        (clearer.as_str(), "clearer", ManagementPrivilege::Clear, false),
    ] {
        let principal = auth.authenticate(&format!("Bearer {token}")).unwrap();
        assert_eq!(principal.name, name);
        assert_eq!(principal.management, privilege);
        assert_eq!(principal.namespace_write, namespace_write);
    }
    assert!(auth.authenticate("Bearer wrong").is_none());
    assert!(auth.authenticate(&format!("Basic {manager}")).is_none());
    assert!(BearerAuthenticator::new(&reader, &writer, &reader, &clearer).is_err());
    assert!(BearerAuthenticator::new("short", &writer, &manager, &clearer).is_err());
}

#[test]
fn writer_credentials_reject_duplicates_and_invalid_tokens() {
    let reader = "r".repeat(32);
    let manager = "m".repeat(32);
    let clearer = "c".repeat(32);
    for writer in [
        &reader,
        &manager,
        &clearer,
        "",
        "short",
        &"w".repeat(257),
        &format!("{}\0", "w".repeat(32)),
    ] {
        assert!(BearerAuthenticator::new(&reader, writer, &manager, &clearer).is_err());
    }
}
#[test]
fn absent_idempotency_keys_allocate_distinct_internal_recovery_identities() {
    use crowdb_access_iceberg::wire::RequestKey;
    let first = RequestKey::parse(None, 100).unwrap();
    let second = RequestKey::parse(None, 100).unwrap();
    assert!(matches!(first, RequestKey::Internal(_)));
    assert_ne!(first.identity(), second.identity());
    let header = "00000000-0064-7000-8000-000000000001";
    assert_eq!(
        RequestKey::parse(Some(header), 100).unwrap(),
        RequestKey::parse(Some(header), 101).unwrap()
    );
    assert!(RequestKey::parse(Some(""), 100).is_err());
}
