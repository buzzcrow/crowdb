// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Host credential provisioning through the real Group-0 authority.

mod benchmark;
mod policy;
mod policy_failure;
mod protocol_http;

use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;

use crowdb_access_s3::auth::{CredentialCipher, MasterKey};
use crowdb_access_server::credentials::CredentialAuthority;

#[tokio::test]
#[ignore = "requires the owned release access storage containers"]
async fn credentials() {
    let ips = crate::common::management::addresses(12);
    let kv = Arc::new(crowdb_kv_client::CrowdbKvClient::new(
        crowdb_kv_client::ClientConfig::new(
            ips[..3]
                .iter()
                .map(|ip| crate::common::management::origin(ip))
                .collect(),
        ),
    ));
    let master = MasterKey::from_hex(&"11".repeat(32)).unwrap();
    let authority = CredentialAuthority::new(kv, Arc::new(CredentialCipher::new(&master)));
    assert!(authority.lookup_user(b"boto3-e2e").await.unwrap().is_none());
    let issued = authority.issue_user(b"boto3-e2e").await.unwrap();
    for resumed in [
        authority.ensure_user(b"boto3-e2e").await.unwrap(),
        authority.lookup_user(b"boto3-e2e").await.unwrap().unwrap(),
        authority.ensure_user(b"boto3-e2e").await.unwrap(),
    ] {
        assert!(
            resumed.access_key_id == issued.access_key_id,
            "credential identity changed"
        );
        assert!(
            resumed.secret_key == issued.secret_key,
            "credential secret changed"
        );
    }
    let path =
        std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap()).join("credentials.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    serde_json::to_writer(
        file,
        &serde_json::json!({"access_key": issued.access_key_id, "secret_key": issued.secret_key}),
    )
    .unwrap();
}
