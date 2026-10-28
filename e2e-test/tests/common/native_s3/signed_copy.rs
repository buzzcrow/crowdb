// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! The HTTP parity fixture uses its ordinary credential authority for COPY.

use std::fmt::Write;
use std::sync::Arc;

use crowdb_access_s3::auth::{CredentialCipher, MasterKey};
use crowdb_access_server::credentials::CredentialAuthority;
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use hmac::{Hmac, Mac};
use reqwest::{Client, Response};
use sha2::{Digest, Sha256};

pub async fn copy(addresses: &[String], http: &Client, base: &str) -> Response {
    let control = Arc::new(PaxosKvClient::new(ClientConfig::new(
        addresses[..3]
            .iter()
            .map(|ip| super::super::management::origin(ip))
            .collect(),
    )));
    let master = MasterKey::from_hex(&"11".repeat(32)).unwrap();
    let authority = CredentialAuthority::new(control, Arc::new(CredentialCipher::new(&master)));
    let credential = authority.ensure_user(b"local").await.unwrap();
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let now = chrono::DateTime::<chrono::Utc>::from_timestamp(i64::try_from(seconds).unwrap(), 0).unwrap();
    let date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let short = now.format("%Y%m%d").to_string();
    let host = base.strip_prefix("http://").unwrap().split('/').next().unwrap();
    let path = "/native-corpus/http-copy";
    let source = "/native-corpus/native-513";
    let hash = hex(&Sha256::digest([]));
    let names = "host;x-amz-content-sha256;x-amz-copy-source;x-amz-date";
    let canonical = format!(
        "PUT\n{path}\n\nhost:{host}\nx-amz-content-sha256:{hash}\nx-amz-copy-source:{source}\nx-amz-date:{date}\n\n{names}\n{hash}"
    );
    let scope = format!("{short}/us-east-1/s3/aws4_request");
    let date_key = mac(format!("AWS4{}", credential.secret_key).as_bytes(), &short);
    let region_key = mac(&date_key, "us-east-1");
    let service_key = mac(&region_key, "s3");
    let signing_key = mac(&service_key, "aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical))
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={}",
        credential.access_key_id,
        hex(&mac(&signing_key, &to_sign))
    );
    http.put(format!("http://{host}{path}"))
        .header("host", host)
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", hash)
        .header("x-amz-copy-source", source)
        .header("authorization", authorization)
        .header("content-length", 0)
        .send()
        .await
        .unwrap()
}

fn mac(key: &[u8], input: &str) -> Vec<u8> {
    let mut signer = Hmac::<Sha256>::new_from_slice(key).unwrap();
    signer.update(input.as_bytes());
    signer.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::new();
    for byte in bytes {
        write!(output, "{byte:02x}").unwrap();
    }
    output
}
