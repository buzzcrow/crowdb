use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{FileCredentials, FileGrant, FileGrantIssuer};
use crowdb_access_s3::auth::{AuthError, Credential, CredentialProvider, RawAuthRequest, SigV4Verifier};

/// Authenticates native file credentials without consulting general S3 authority.
/// Callers must freshly validate the Ready context and authorize the returned grant
/// against the routed operation, location and actual streamed byte counts.
/// # Errors
/// Rejects oversized or ambiguous authentication, stale tokens and bad signatures.
pub fn authenticate_file_request(
    issuer: &FileGrantIssuer,
    context: CatalogContext,
    request: RawAuthRequest<'_>,
    region: &str,
    now_ms: u64,
) -> Result<FileGrant, AuthError> {
    validate_bounds(request)?;
    let token = session_token(request)?;
    let credentials = issuer
        .verify_token(&token, context, now_ms)
        .map_err(|_| AuthError::Rejected)?;
    let provider = FileCredentialProvider(&credentials);
    SigV4Verifier::new(provider, region.to_owned(), 900).verify(request, now_ms / 1000)?;
    Ok(credentials.grant().clone())
}

struct FileCredentialProvider<'a>(&'a FileCredentials);

impl CredentialProvider for FileCredentialProvider<'_> {
    fn lookup(&self, access_key: &str) -> Option<Credential> {
        (access_key == self.0.access_key_id()).then(|| Credential {
            secret_key: self.0.secret_access_key().as_bytes().to_vec(),
            session_token: Some(self.0.session_token().to_owned()),
            enabled: true,
        })
    }
}

fn validate_bounds(request: RawAuthRequest<'_>) -> Result<(), AuthError> {
    let uri_bytes = request
        .uri
        .path_and_query()
        .map_or(0, |value| value.as_str().len())
        + request.uri.authority().map_or(0, |value| value.as_str().len())
        + request.uri.scheme_str().map_or(0, str::len);
    if uri_bytes > 8192 || request.headers.len() > 64 {
        return Err(AuthError::Rejected);
    }
    let total = request
        .headers
        .iter()
        .try_fold(0_usize, |size, (name, value)| {
            size.checked_add(name.as_str().len())?.checked_add(value.len())
        })
        .ok_or(AuthError::Rejected)?;
    if total > 16 * 1024 {
        return Err(AuthError::Rejected);
    }
    for name in [
        "authorization",
        "host",
        "x-amz-date",
        "x-amz-content-sha256",
        "x-amz-security-token",
    ] {
        if request.headers.get_all(name).iter().count() > 1 {
            return Err(AuthError::Rejected);
        }
    }
    Ok(())
}

fn session_token(request: RawAuthRequest<'_>) -> Result<String, AuthError> {
    let header_signed = request.headers.contains_key("authorization");
    let mut token = None;
    let mut names = std::collections::BTreeSet::new();
    for part in request.uri.query().unwrap_or_default().split('&') {
        let (name, value) = part.split_once('=').unwrap_or((part, ""));
        let decoded = percent_encoding::percent_decode_str(name)
            .decode_utf8()
            .map_err(|_| AuthError::Rejected)?;
        if !decoded.starts_with("X-Amz-") {
            continue;
        }
        if header_signed || decoded != name || !names.insert(name) {
            return Err(AuthError::Rejected);
        }
        if name == "X-Amz-Security-Token" {
            token = Some(
                percent_encoding::percent_decode_str(value)
                    .decode_utf8()
                    .map_err(|_| AuthError::Rejected)?
                    .into_owned(),
            );
        }
    }
    if header_signed {
        request
            .headers
            .get("x-amz-security-token")
            .ok_or(AuthError::Rejected)?
            .to_str()
            .map(str::to_owned)
            .map_err(|_| AuthError::Rejected)
    } else if request.headers.contains_key("x-amz-security-token") {
        Err(AuthError::Rejected)
    } else {
        token.ok_or(AuthError::Rejected)
    }
}
