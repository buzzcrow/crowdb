// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, Utc};
use hmac::{Hmac, Mac};
use hyper::header::AUTHORIZATION;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::ZeroizeOnDrop;

use super::{AuthError, PayloadMode, RawAuthRequest, RequestAuthenticator};

mod streaming;
pub use streaming::StreamingPayloadVerifier;

type HmacSha256 = Hmac<Sha256>;
const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const TERMINATOR: &str = "aws4_request";

#[derive(Clone, ZeroizeOnDrop)]
pub struct Credential {
    pub secret_key: Vec<u8>,
    pub session_token: Option<String>,
    pub enabled: bool,
}

pub trait CredentialProvider: Send + Sync {
    fn lookup(&self, access_key: &str) -> Option<Credential>;
}

impl<T: CredentialProvider + ?Sized> CredentialProvider for std::sync::Arc<T> {
    fn lookup(&self, access_key: &str) -> Option<Credential> {
        (**self).lookup(access_key)
    }
}

pub struct SigV4Verifier<P> {
    provider: P,
    region: String,
    max_clock_skew_seconds: u64,
}

impl<P> SigV4Verifier<P> {
    #[must_use]
    pub fn new(provider: P, region: String, max_clock_skew_seconds: u64) -> Self {
        Self {
            provider,
            region,
            max_clock_skew_seconds,
        }
    }
}

#[async_trait::async_trait]
impl<P: CredentialProvider> RequestAuthenticator for SigV4Verifier<P> {
    async fn authenticate(&self, request: RawAuthRequest<'_>) -> Result<(), AuthError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::Unavailable)?
            .as_secs();
        self.verify(request, now)
    }
}

impl<P: CredentialProvider> SigV4Verifier<P> {
    /// Verifies one header-signed `SigV4` request before routing or body polling.
    ///
    /// # Errors
    ///
    /// Rejects malformed scope, headers, timestamp, credential, or signature.
    pub fn verify(&self, request: RawAuthRequest<'_>, now: u64) -> Result<(), AuthError> {
        validate_payload_mode(request.payload_mode)?;
        if let Some(authorization) = request
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        {
            return self.verify_header(request, authorization, now, false);
        }
        self.verify_presigned(request, now)
    }

    fn verify_header(
        &self,
        request: RawAuthRequest<'_>,
        authorization: &str,
        now: u64,
        streaming: bool,
    ) -> Result<(), AuthError> {
        let parsed = ParsedAuthorization::parse(authorization)?;
        let amz_date = request
            .headers
            .get("x-amz-date")
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthError::Rejected)?;
        let timestamp = parse_timestamp(amz_date)?;
        if now.abs_diff(timestamp) > self.max_clock_skew_seconds {
            return Err(AuthError::Rejected);
        }
        let payload_hash = request
            .headers
            .get("x-amz-content-sha256")
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthError::Rejected)?;
        let query = canonical_query(request.uri.query(), None);
        self.verify_signature(
            request,
            &parsed,
            amz_date,
            payload_hash,
            &query,
            request
                .headers
                .get("x-amz-security-token")
                .and_then(|value| value.to_str().ok()),
            streaming,
        )
    }

    fn verify_presigned(&self, request: RawAuthRequest<'_>, now: u64) -> Result<(), AuthError> {
        let query = request.uri.query().ok_or(AuthError::Rejected)?;
        if query_value(query, "X-Amz-Algorithm")?.as_str() != ALGORITHM {
            return Err(AuthError::Rejected);
        }
        let credential_scope = query_value(query, "X-Amz-Credential")?;
        let signed_headers = query_value(query, "X-Amz-SignedHeaders")?;
        let signature = query_value(query, "X-Amz-Signature")?;
        let amz_date = query_value(query, "X-Amz-Date")?;
        let expires = query_value(query, "X-Amz-Expires")?
            .parse::<u64>()
            .map_err(|_| AuthError::Rejected)?;
        if expires > 604_800 {
            return Err(AuthError::Rejected);
        }
        let timestamp = parse_timestamp(&amz_date)?;
        if now.saturating_add(self.max_clock_skew_seconds) < timestamp
            || now
                > timestamp
                    .saturating_add(expires)
                    .saturating_add(self.max_clock_skew_seconds)
        {
            return Err(AuthError::Rejected);
        }
        let parsed =
            ParsedAuthorization::from_credential_scope(&credential_scope, &signed_headers, &signature)?;
        let payload_hash = request
            .headers
            .get("x-amz-content-sha256")
            .map(|value| value.to_str().map_err(|_| AuthError::Rejected))
            .transpose()?
            .unwrap_or("UNSIGNED-PAYLOAD");
        let session_token = optional_query_value(query, "X-Amz-Security-Token")?;
        let canonical_query = canonical_query(Some(query), Some("X-Amz-Signature"));
        self.verify_signature(
            request,
            &parsed,
            &amz_date,
            payload_hash,
            &canonical_query,
            session_token.as_deref(),
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_signature(
        &self,
        request: RawAuthRequest<'_>,
        parsed: &ParsedAuthorization<'_>,
        amz_date: &str,
        payload_hash: &str,
        canonical_query: &str,
        session_token: Option<&str>,
        streaming: bool,
    ) -> Result<(), AuthError> {
        if parsed.region != self.region || parsed.service != "s3" || parsed.terminator != TERMINATOR {
            return Err(AuthError::Rejected);
        }
        let credential = self
            .provider
            .lookup(parsed.access_key)
            .filter(|credential| credential.enabled)
            .ok_or(AuthError::Rejected)?;
        if session_token != credential.session_token.as_deref() {
            return Err(AuthError::Rejected);
        }
        if !amz_date.starts_with(parsed.date) {
            return Err(AuthError::Rejected);
        }
        if !streaming || !streaming::supported(payload_hash) {
            validate_payload_hash(payload_hash)?;
        }
        let canonical_headers = canonical_headers(request.headers, parsed.signed_headers)?;
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            request.method.as_str(),
            canonical_uri(request.uri.path()),
            canonical_query,
            canonical_headers,
            parsed.signed_headers,
            payload_hash
        );
        let scope = format!("{}/{}/s3/{}", parsed.date, parsed.region, TERMINATOR);
        let string_to_sign = format!(
            "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
            hex(&Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = signing_key(&credential.secret_key, parsed.date, parsed.region, "s3")?;
        let mut mac = HmacSha256::new_from_slice(&signing_key).map_err(|_| AuthError::Rejected)?;
        mac.update(string_to_sign.as_bytes());
        let expected = mac.finalize().into_bytes();
        let supplied = decode_hex(parsed.signature)?;
        if expected.as_slice().ct_eq(&supplied).unwrap_u8() != 1 {
            return Err(AuthError::Rejected);
        }
        Ok(())
    }
}

struct ParsedAuthorization<'a> {
    access_key: &'a str,
    date: &'a str,
    region: &'a str,
    service: &'a str,
    terminator: &'a str,
    signed_headers: &'a str,
    signature: &'a str,
}

impl<'a> ParsedAuthorization<'a> {
    fn parse(value: &'a str) -> Result<Self, AuthError> {
        let fields = value.strip_prefix(ALGORITHM).ok_or(AuthError::Rejected)?.trim();
        let credential = field(fields, "Credential")?;
        let signed_headers = field(fields, "SignedHeaders")?;
        let signature = field(fields, "Signature")?;
        Self::from_credential_scope(credential, signed_headers, signature)
    }

    fn from_credential_scope(
        credential: &'a str,
        signed_headers: &'a str,
        signature: &'a str,
    ) -> Result<Self, AuthError> {
        let mut scope = credential.split('/');
        let result = Self {
            access_key: scope.next().ok_or(AuthError::Rejected)?,
            date: scope.next().ok_or(AuthError::Rejected)?,
            region: scope.next().ok_or(AuthError::Rejected)?,
            service: scope.next().ok_or(AuthError::Rejected)?,
            terminator: scope.next().ok_or(AuthError::Rejected)?,
            signed_headers,
            signature,
        };
        if scope.next().is_some()
            || result.access_key.is_empty()
            || !signed_headers.split(';').any(|h| h == "host")
        {
            return Err(AuthError::Rejected);
        }
        Ok(result)
    }
}

fn field<'a>(value: &'a str, name: &str) -> Result<&'a str, AuthError> {
    value
        .split(',')
        .map(str::trim)
        .find_map(|part| part.strip_prefix(name)?.strip_prefix('='))
        .ok_or(AuthError::Rejected)
}

fn canonical_headers(headers: &hyper::HeaderMap, names: &str) -> Result<String, AuthError> {
    let mut output = String::new();
    let mut previous = "";
    for name in names.split(';') {
        if name.is_empty() || name <= previous || name.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(AuthError::Rejected);
        }
        previous = name;
        let values = headers.get_all(name);
        if values.iter().next().is_none() {
            return Err(AuthError::Rejected);
        }
        output.push_str(name);
        output.push(':');
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                output.push(',');
            }
            let value = value.to_str().map_err(|_| AuthError::Rejected)?;
            output.push_str(&value.split_ascii_whitespace().collect::<Vec<_>>().join(" "));
        }
        output.push('\n');
    }
    Ok(output)
}

fn canonical_uri(path: &str) -> String {
    aws_encode(
        &percent_encoding::percent_decode_str(path).collect::<Vec<_>>(),
        true,
    )
}

fn canonical_query(query: Option<&str>, excluded_name: Option<&str>) -> String {
    let mut fields = query
        .unwrap_or_default()
        .split('&')
        .filter(|field| !field.is_empty())
        .filter(|field| field.split_once('=').map_or(*field, |(name, _)| name) != excluded_name.unwrap_or(""))
        .map(|field| {
            let (name, value) = field.split_once('=').unwrap_or((field, ""));
            (
                aws_encode(
                    &percent_encoding::percent_decode_str(name).collect::<Vec<_>>(),
                    false,
                ),
                aws_encode(
                    &percent_encoding::percent_decode_str(value).collect::<Vec<_>>(),
                    false,
                ),
            )
        })
        .collect::<Vec<_>>();
    fields.sort_unstable();
    fields
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn aws_encode(value: &[u8], preserve_slash: bool) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value {
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || preserve_slash && *byte == b'/'
        {
            output.push(char::from(*byte));
        } else {
            output.push('%');
            write!(&mut output, "{byte:02X}").expect("writing to String cannot fail");
        }
    }
    output
}

fn signing_key(secret: &[u8], date: &str, region: &str, service: &str) -> Result<Vec<u8>, AuthError> {
    let mut initial = b"AWS4".to_vec();
    initial.extend_from_slice(secret);
    let date_key = hmac(&initial, date.as_bytes())?;
    let region_key = hmac(&date_key, region.as_bytes())?;
    let service_key = hmac(&region_key, service.as_bytes())?;
    hmac(&service_key, TERMINATOR.as_bytes())
}

fn hmac(key: &[u8], value: &[u8]) -> Result<Vec<u8>, AuthError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| AuthError::Rejected)?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn parse_timestamp(value: &str) -> Result<u64, AuthError> {
    let naive = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ").map_err(|_| AuthError::Rejected)?;
    u64::try_from(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc).timestamp())
        .map_err(|_| AuthError::Rejected)
}

fn query_value(query: &str, name: &str) -> Result<String, AuthError> {
    optional_query_value(query, name)?.ok_or(AuthError::Rejected)
}

fn optional_query_value(query: &str, name: &str) -> Result<Option<String>, AuthError> {
    query
        .split('&')
        .find_map(|field| {
            let (candidate, value) = field.split_once('=').unwrap_or((field, ""));
            (candidate == name).then_some(value)
        })
        .map(|value| {
            percent_encoding::percent_decode_str(value)
                .decode_utf8()
                .map(std::borrow::Cow::into_owned)
                .map_err(|_| AuthError::Rejected)
        })
        .transpose()
}

fn validate_payload_mode(mode: PayloadMode<'_>) -> Result<(), AuthError> {
    match mode {
        PayloadMode::Signed(value) => validate_payload_hash(value),
        PayloadMode::Empty | PayloadMode::ContentLength(_) | PayloadMode::Chunked => Ok(()),
    }
}

fn validate_payload_hash(value: &str) -> Result<(), AuthError> {
    if value == "UNSIGNED-PAYLOAD" {
        return Ok(());
    }
    if value.starts_with("STREAMING-") {
        return Err(AuthError::Rejected);
    }
    decode_hex(value).map(|_| ())
}

fn decode_hex(value: &str) -> Result<Vec<u8>, AuthError> {
    if value.len() != 64 {
        return Err(AuthError::Rejected);
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| AuthError::Rejected)?;
            u8::from_str_radix(text, 16).map_err(|_| AuthError::Rejected)
        })
        .collect()
}

fn hex(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
