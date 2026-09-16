// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::auth::{Credential, CredentialCache, CredentialProvider};

fn credential(secret: &[u8], enabled: bool) -> Credential {
    Credential {
        secret_key: secret.to_vec(),
        session_token: None,
        enabled,
    }
}

#[test]
fn snapshot_replacement_is_atomic_and_staleness_fails_closed() {
    let cache = CredentialCache::new(10);
    assert!(cache.lookup_at("old", 100).is_none());
    cache
        .install([("old".into(), credential(b"first", true))], 100)
        .unwrap();
    assert_eq!(cache.lookup_at("old", 110).unwrap().secret_key, b"first");
    assert!(cache.lookup_at("old", 111).is_none());

    cache
        .install([("new".into(), credential(b"second", true))], 112)
        .unwrap();
    assert!(cache.lookup_at("old", 112).is_none());
    assert_eq!(cache.lookup_at("new", 112).unwrap().secret_key, b"second");
}

#[test]
fn invalid_snapshot_does_not_replace_the_current_authority() {
    let cache = CredentialCache::new(10);
    cache
        .install([("stable".into(), credential(b"secret", true))], 100)
        .unwrap();
    assert!(cache
        .install([("broken".into(), credential(b"", true))], 101)
        .is_err());
    assert!(cache.lookup_at("stable", 101).is_some());
}

#[test]
fn provider_lookup_observes_enabled_state() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let cache = CredentialCache::new(10);
    cache
        .install([("disabled".into(), credential(b"secret", false))], now)
        .unwrap();
    assert!(!cache.lookup("disabled").unwrap().enabled);
}
