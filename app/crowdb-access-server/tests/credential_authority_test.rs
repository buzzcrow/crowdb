// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/credentials.rs"]
mod credentials;
use credentials::TestCredentials;
use crowdb_access_server::credentials::CredentialAuthorityError;
use crowdb_kv_client::{GetOutcome, ReadMode};

const USER: &[u8] = b"console-root";
const PREPARED: &[u8] = b"crowdb/s3/user-bootstrap/";
const CREDENTIALS: &[u8] = b"crowdb/s3/credential/";

#[tokio::test]
async fn concurrent_bootstrap_callers_publish_one_encrypted_credential_and_reuse_it() {
    let test = TestCredentials::start().await;
    let calls = (0..16).map(|_| async { test.authority().ensure_user(USER).await.unwrap() });
    let tokens = futures::future::join_all(calls).await;
    assert!(tokens
        .iter()
        .all(|token| token.access_key_id == tokens[0].access_key_id));
    assert!(tokens
        .iter()
        .all(|token| token.secret_key == tokens[0].secret_key));
    let durable = test
        .control
        .scan(
            0,
            0,
            CREDENTIALS,
            b"",
            b"",
            100,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(!durable.truncated && !durable.timed_out);
    assert_eq!(durable.items.len(), 1);
    let prepared = test
        .control
        .scan(
            0,
            0,
            PREPARED,
            b"",
            b"",
            100,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(prepared.items.len(), 1);
    let state: serde_json::Value = serde_json::from_slice(&prepared.items[0].1).unwrap();
    let encrypted: Vec<u8> = serde_json::from_value(state["record"].clone()).unwrap();
    assert!(!encrypted
        .windows(tokens[0].secret_key.len())
        .any(|bytes| bytes == tokens[0].secret_key.as_bytes()));
    let replay = test.authority().ensure_user(USER).await.unwrap();
    assert_eq!(replay.access_key_id, tokens[0].access_key_id);
    assert_eq!(replay.secret_key, tokens[0].secret_key);
    let loaded = test.authority().load_credentials().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].0, replay.access_key_id);
}

#[tokio::test]
async fn interrupted_projection_resumes_the_original_encrypted_candidate_after_authority_restart() {
    let test = TestCredentials::start().await;
    let original = test.authority().ensure_user(USER).await.unwrap();
    let key = format!("crowdb/s3/credential/{}", original.access_key_id);
    let before = test
        .control
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap();
    // Reproduce interruption after preparation but before its credential projection.
    test.control.delete(0, 0, key.as_bytes(), None).await.unwrap();
    assert!(test.authority().lookup_user(USER).await.unwrap().is_none());
    let resumed = test.authority().ensure_user(USER).await.unwrap();
    assert_eq!(resumed.access_key_id, original.access_key_id);
    assert_eq!(resumed.secret_key, original.secret_key);
    match (
        before,
        test.control
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await
            .unwrap(),
    ) {
        (GetOutcome::Found { value: a, .. }, GetOutcome::Found { value: b, .. }) => assert_eq!(a, b),
        _ => panic!("credential projection is absent"),
    }
}

#[tokio::test]
async fn legacy_credentials_are_preserved_and_existing_duplicates_are_never_repaired_by_guessing() {
    let test = TestCredentials::start().await;
    assert!(matches!(
        test.authority().ensure_user(b"").await,
        Err(CredentialAuthorityError::EmptyUser)
    ));
    let legacy = test.authority().issue_user(USER).await.unwrap();
    let replay = test.authority().ensure_user(USER).await.unwrap();
    assert_eq!(replay.access_key_id, legacy.access_key_id);
    assert_eq!(replay.secret_key, legacy.secret_key);
    test.authority().issue_user(USER).await.unwrap();
    assert!(matches!(
        test.authority().ensure_user(USER).await,
        Err(CredentialAuthorityError::DuplicateUser)
    ));
    let rows = test
        .control
        .scan(
            0,
            0,
            CREDENTIALS,
            b"",
            b"",
            100,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(rows.items.len(), 2);
    let rows = test
        .control
        .scan(
            0,
            0,
            PREPARED,
            b"",
            b"",
            100,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(rows.items.is_empty());
}

#[tokio::test]
async fn malformed_preparation_never_creates_or_replaces_a_user_credential() {
    let test = TestCredentials::start().await;
    let original = test.authority().ensure_user(USER).await.unwrap();
    test.control
        .delete(
            0,
            0,
            format!("crowdb/s3/credential/{}", original.access_key_id).as_bytes(),
            None,
        )
        .await
        .unwrap();
    let rows = test
        .control
        .scan(
            0,
            0,
            PREPARED,
            b"",
            b"",
            100,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let key = &rows.items[0].0;
    let mut invalid: serde_json::Value = serde_json::from_slice(&rows.items[0].1).unwrap();
    invalid["version"] = serde_json::json!(2);
    let bytes = serde_json::to_vec(&invalid).unwrap();
    test.control.put(0, 0, key, &bytes, None).await.unwrap();
    assert!(matches!(
        test.authority().ensure_user(USER).await,
        Err(CredentialAuthorityError::InvalidUserPreparation)
    ));
    assert!(test.authority().lookup_user(USER).await.unwrap().is_none());
    let GetOutcome::Found { value, .. } = test
        .control
        .get(0, 0, key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("preparation disappeared")
    };
    assert_eq!(value.as_ref(), bytes.as_slice());
}

#[tokio::test]
async fn incomplete_credential_inventories_cannot_create_users_or_install_a_partial_cache() {
    let test = TestCredentials::start().await;
    for user in 0..4097 {
        test.authority()
            .issue_user(format!("user-{user}").as_bytes())
            .await
            .unwrap();
    }
    let lookup = test.authority().ensure_user(USER).await;
    let error = lookup.err().expect("incomplete lookup must reject issuance");
    assert!(matches!(error, CredentialAuthorityError::Storage(_)), "{error}");
    let loaded = test.authority().load_credentials().await;
    assert!(matches!(loaded, Err(CredentialAuthorityError::Storage(_))));
    let prepared = test
        .control
        .scan(
            0,
            0,
            PREPARED,
            b"",
            b"",
            100,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(
        prepared.items.is_empty(),
        "incomplete inventory cannot reserve a new user"
    );
}
