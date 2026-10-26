// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::auth::{
    CredentialCipher, DurableCredentialRecord, EncryptedCredentialRecord, MasterKey, SecretError,
};

#[test]
fn configured_master_key_issues_and_decrypts_one_time_user_token() {
    let key = MasterKey::from_hex(&"11".repeat(32)).unwrap();
    let cipher = CredentialCipher::new(&key);
    let (issued, record) = cipher.issue_user_token(b"user-1", 7).unwrap();
    let encoded = record.encode();
    let decoded = EncryptedCredentialRecord::decode(&encoded).unwrap();
    let credential = cipher
        .decrypt(b"user-1", &issued.access_key_id, &decoded)
        .unwrap();

    assert!(issued.access_key_id.starts_with("CROW"));
    assert_eq!(credential.secret_key, issued.secret_key.as_bytes());
    assert!(credential.enabled);
    assert_eq!(decoded.generation, 7);
}

#[test]
fn ciphertext_is_bound_to_user_access_key_and_generation() {
    let key = MasterKey::from_hex(&"22".repeat(32)).unwrap();
    let cipher = CredentialCipher::new(&key);
    let record = cipher
        .encrypt(b"user-1", "CROWACCESS", 3, b"secret", true)
        .unwrap();

    assert!(cipher.decrypt(b"user-2", "CROWACCESS", &record).is_err());
    assert!(cipher.decrypt(b"user-1", "CROWOTHER", &record).is_err());
    let mut changed = record;
    changed.generation = 4;
    assert!(cipher.decrypt(b"user-1", "CROWACCESS", &changed).is_err());
}

#[test]
fn master_key_also_derives_a_stable_continuation_authority() {
    let first = CredentialCipher::new(&MasterKey::from_hex(&"33".repeat(32)).unwrap());
    let second = CredentialCipher::new(&MasterKey::from_hex(&"33".repeat(32)).unwrap());
    let different = CredentialCipher::new(&MasterKey::from_hex(&"44".repeat(32)).unwrap());

    assert_eq!(first.continuation_key(), second.continuation_key());
    assert_ne!(first.continuation_key(), different.continuation_key());
}

#[test]
fn durable_record_preserves_user_binding() {
    let cipher = CredentialCipher::new(&MasterKey::from_hex(&"55".repeat(32)).unwrap());
    let (_, encrypted) = cipher.issue_user_token(b"alice", 7).unwrap();
    let record = DurableCredentialRecord {
        user: b"alice".to_vec(),
        encrypted,
    };

    let decoded = DurableCredentialRecord::decode(&record.encode().unwrap()).unwrap();
    assert_eq!(decoded, record);
    assert!(matches!(
        DurableCredentialRecord::decode(&[1, 0, 0, 0, 0]),
        Err(SecretError::InvalidRecord)
    ));
}

#[test]
fn a_selected_public_key_still_gets_random_secrets_and_bound_ciphertext() {
    let cipher = CredentialCipher::new(&MasterKey::from_hex(&"66".repeat(32)).unwrap());
    let key = format!("CROWU{}", "ab".repeat(32));
    let (first, encrypted) = cipher.issue_user_token_for_key(b"user", key.clone(), 1).unwrap();
    let (second, _) = cipher.issue_user_token_for_key(b"user", key.clone(), 1).unwrap();
    assert_eq!(first.access_key_id, second.access_key_id);
    assert_ne!(first.secret_key, second.secret_key);
    assert_eq!(
        cipher.decrypt(b"user", &key, &encrypted).unwrap().secret_key,
        first.secret_key.as_bytes()
    );
    assert!(cipher.decrypt(b"other", &key, &encrypted).is_err());
    assert!(cipher.decrypt(b"user", "CROWOTHER", &encrypted).is_err());
    for invalid in [String::new(), "a".repeat(81), "a/b".into(), "a\0b".into()] {
        assert!(cipher.issue_user_token_for_key(b"user", invalid, 1).is_err());
    }
}
