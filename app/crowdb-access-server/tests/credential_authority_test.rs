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

fn legacy_key() -> Vec<u8> {
    use sha2::Digest;
    format!(
        "crowdb/s3/user-bootstrap/{}",
        hex::encode(sha2::Sha256::digest(USER))
    )
    .into_bytes()
}

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
    assert!(prepared.items.is_empty());
    let encrypted = &durable.items[0].1;
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
async fn deleted_final_credential_is_recreated_without_resurrecting_a_legacy_preparation() {
    let test = TestCredentials::start().await;
    let original = test.authority().ensure_user(USER).await.unwrap();
    let key = format!("crowdb/s3/credential/{}", original.access_key_id);
    let GetOutcome::Found { value: before, .. } = test
        .control
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!()
    };
    let legacy_key = legacy_key();
    let legacy = serde_json::to_vec(&serde_json::json!({"version": 1,
        "access_key_id": original.access_key_id, "record": before.to_vec()}))
    .unwrap();
    test.control.put(0, 0, &legacy_key, &legacy, None).await.unwrap();
    test.control.delete(0, 0, key.as_bytes(), None).await.unwrap();
    assert!(test.authority().lookup_user(USER).await.unwrap().is_none());
    let replaced = test.authority().ensure_user(USER).await.unwrap();
    assert_eq!(replaced.access_key_id, original.access_key_id);
    assert_ne!(replaced.secret_key, original.secret_key);
    let GetOutcome::Found { value: after, .. } = test
        .control
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_ne!(before, after);
    assert_eq!(
        test.authority().load_credentials().await.unwrap()[0].0,
        replaced.access_key_id
    );
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
async fn malformed_legacy_process_records_do_not_authorize_or_block_credentials() {
    let test = TestCredentials::start().await;
    let key = legacy_key();
    let bytes = b"not an encrypted credential";
    test.control.put(0, 0, &key, bytes, None).await.unwrap();
    assert!(test.authority().lookup_user(USER).await.unwrap().is_none());
    let issued = test.authority().ensure_user(USER).await.unwrap();
    assert_eq!(
        test.authority()
            .lookup_user(USER)
            .await
            .unwrap()
            .unwrap()
            .secret_key,
        issued.secret_key
    );
    assert_eq!(test.authority().load_credentials().await.unwrap().len(), 1);
    let GetOutcome::Found { value, .. } = test
        .control
        .get(0, 0, &key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(value.as_ref(), bytes);
}

#[tokio::test]
async fn disabled_canonical_credentials_are_never_overwritten_by_bootstrap() {
    let test = TestCredentials::start().await;
    let issued = test.authority().ensure_user(USER).await.unwrap();
    let key = format!("crowdb/s3/credential/{}", issued.access_key_id);
    let GetOutcome::Found { value, .. } = test
        .control
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!()
    };
    let mut record = crowdb_access_s3::auth::DurableCredentialRecord::decode(&value).unwrap();
    record.encrypted.enabled = false;
    let disabled = record.encode().unwrap();
    test.control
        .put(0, 0, key.as_bytes(), &disabled, None)
        .await
        .unwrap();
    assert!(matches!(
        test.authority().ensure_user(USER).await,
        Err(CredentialAuthorityError::InvalidUserCredential)
    ));
    let GetOutcome::Found { value, .. } = test
        .control
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(value.as_ref(), disabled);
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
