// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::credentials::S3Credentials;
use axum::http::{HeaderMap, Method};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

fn encode(value: &str, slash: bool) -> String {
    let mut result = String::new();
    for byte in percent_encoding::percent_decode_str(value) {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) || (slash && byte == b'/') {
            result.push(char::from(byte));
        } else {
            write!(result, "%{byte:02X}").expect("String writing cannot fail");
        }
    }
    result
}

fn mac(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts arbitrary key lengths");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub(super) fn sign(
    credentials: &S3Credentials,
    method: &Method,
    url: &reqwest::Url,
    body: &[u8],
    headers: &mut HeaderMap,
) -> Result<(), String> {
    let timestamp = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
        .format("%Y%m%dT%H%M%SZ")
        .to_string();
    let date = &timestamp[..8];
    let payload = hex::encode(Sha256::digest(body));
    let origin = url.origin().ascii_serialization();
    let host = origin.split_once("://").map_or(origin.as_str(), |(_, host)| host);
    let mut signed = std::collections::BTreeMap::from([
        ("host", host.to_owned()),
        ("x-amz-content-sha256", payload.clone()),
        ("x-amz-date", timestamp.clone()),
    ]);
    if let Some(token) = &credentials.session {
        signed.insert("x-amz-security-token", token.clone());
    }
    let names = signed.keys().copied().collect::<Vec<_>>().join(";");
    let mut canonical_headers = String::new();
    for (name, value) in &signed {
        writeln!(canonical_headers, "{name}:{}", value.trim()).expect("String writing cannot fail");
    }
    let mut query: Vec<_> = url
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (encode(key, false), encode(value, false))
        })
        .collect();
    query.sort();
    let query = query
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!(
        "{method}\n{}\n{query}\n{canonical_headers}\n{names}\n{payload}",
        encode(url.path(), true)
    );
    let scope = format!("{date}/{}/s3/aws4_request", credentials.region);
    let mut key = format!("AWS4{}", credentials.secret).into_bytes();
    for part in [date, &credentials.region, "s3", "aws4_request"] {
        key = mac(&key, part);
    }
    let signature = hex::encode(mac(
        &key,
        &format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical.as_bytes()))
        ),
    ));
    signed.insert(
        "authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={signature}",
            credentials.access
        ),
    );
    for (name, value) in signed {
        headers.insert(name, value.parse().map_err(|_| "Invalid cluster S3 credential")?);
    }
    Ok(())
}
