use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use data_encoding::BASE32_NOPAD;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::catalog::CatalogContext;
use crate::key::{CatalogId, OperationId, TableId};

use super::{FileCredentials, FileGrant, FileGrantError, FileOperations};

const CLAIM_BYTES: usize = 123;
const TOKEN_BYTES: usize = CLAIM_BYTES + 32;
const TOKEN_CHARACTERS: usize = 207;
type Signer = Hmac<Sha256>;

pub struct FileGrantIssuer {
    key: [u8; 32],
    max_ttl_ms: u64,
}

impl FileGrantIssuer {
    /// # Errors
    /// Rejects an empty delegation window or an uninitialized signing key.
    pub fn new(key: [u8; 32], max_ttl_ms: u64) -> Result<Self, FileGrantError> {
        if key == [0; 32] || max_ttl_ms == 0 {
            return Err(FileGrantError::Invalid);
        }
        Ok(Self { key, max_ttl_ms })
    }

    /// # Errors
    /// Rejects invalid scopes, byte limits and grants exceeding the catalog's delegation bound.
    pub fn issue(&self, grant: FileGrant) -> Result<FileCredentials, FileGrantError> {
        grant.validate(self.max_ttl_ms)?;
        let mut bytes = encode(&grant);
        let signature = self
            .signer(b"iceberg-file-grant-v1", &bytes)
            .finalize()
            .into_bytes();
        bytes.extend_from_slice(&signature);
        Ok(self.credentials(grant, &bytes))
    }

    /// Requires the caller's freshly validated Ready catalog context.
    /// # Errors
    /// Rejects altered tokens, stale catalogs, expired grants and mismatched access keys.
    pub fn verify(
        &self,
        access_key_id: &str,
        session_token: &str,
        context: CatalogContext,
        now_ms: u64,
    ) -> Result<FileCredentials, FileGrantError> {
        if access_key_id.len() != 20 {
            return Err(FileGrantError::Invalid);
        }
        let credentials = self.verify_token(session_token, context, now_ms)?;
        if !bool::from(
            credentials
                .access_key_id
                .as_bytes()
                .ct_eq(access_key_id.as_bytes()),
        ) {
            return Err(FileGrantError::Invalid);
        }
        Ok(credentials)
    }

    /// Reconstructs credentials for request signature verification, not bearer access.
    /// Requires the caller's freshly validated Ready catalog context.
    /// # Errors
    /// Rejects altered tokens, stale catalogs and grants outside their validity window.
    pub fn verify_token(
        &self,
        session_token: &str,
        context: CatalogContext,
        now_ms: u64,
    ) -> Result<FileCredentials, FileGrantError> {
        if session_token.len() != TOKEN_CHARACTERS {
            return Err(FileGrantError::Invalid);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(session_token)
            .map_err(|_| FileGrantError::Invalid)?;
        if bytes.len() != TOKEN_BYTES {
            return Err(FileGrantError::Invalid);
        }
        self.signer(b"iceberg-file-grant-v1", &bytes[..CLAIM_BYTES])
            .verify_slice(&bytes[CLAIM_BYTES..])
            .map_err(|_| FileGrantError::Invalid)?;
        let grant = decode(&bytes[..CLAIM_BYTES])?;
        grant.validate(self.max_ttl_ms)?;
        if grant.context != context {
            return Err(FileGrantError::Forbidden);
        }
        if now_ms < grant.issued_ms || now_ms >= grant.expires_ms {
            return Err(FileGrantError::Expired);
        }
        Ok(self.credentials(grant, &bytes))
    }

    fn signer(&self, domain: &[u8], bytes: &[u8]) -> Signer {
        let mut signer = Signer::new_from_slice(&self.key).expect("HMAC accepts fixed-width keys");
        signer.update(domain);
        signer.update(bytes);
        signer
    }

    fn credentials(&self, grant: FileGrant, bytes: &[u8]) -> FileCredentials {
        let secret = self
            .signer(b"iceberg-file-secret-v1", bytes)
            .finalize()
            .into_bytes();
        let access = self
            .signer(b"iceberg-file-access-v1", bytes)
            .finalize()
            .into_bytes();
        FileCredentials {
            grant,
            access_key_id: format!("CICE{}", BASE32_NOPAD.encode(&access[..10])),
            secret_access_key: URL_SAFE_NO_PAD.encode(secret),
            session_token: URL_SAFE_NO_PAD.encode(bytes),
        }
    }
}

fn encode(grant: &FileGrant) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(TOKEN_BYTES);
    bytes.push(1);
    bytes.extend_from_slice(grant.context.catalog.as_bytes());
    bytes.extend_from_slice(&grant.context.activation_epoch.to_be_bytes());
    bytes.extend_from_slice(grant.table.as_bytes());
    bytes.extend_from_slice(&grant.principal);
    bytes.extend_from_slice(grant.nonce.as_bytes());
    bytes.extend_from_slice(&grant.issued_ms.to_be_bytes());
    bytes.extend_from_slice(&grant.expires_ms.to_be_bytes());
    bytes.extend_from_slice(&grant.operations.0.to_be_bytes());
    bytes.extend_from_slice(&grant.max_request_bytes.to_be_bytes());
    bytes.extend_from_slice(&grant.max_file_bytes.to_be_bytes());
    bytes
}

fn decode(bytes: &[u8]) -> Result<FileGrant, FileGrantError> {
    if bytes.len() != CLAIM_BYTES || bytes[0] != 1 {
        return Err(FileGrantError::Invalid);
    }
    let array = |start, end| bytes.get(start..end).ok_or(FileGrantError::Invalid);
    let number = |start| -> Result<u64, FileGrantError> {
        Ok(u64::from_be_bytes(
            array(start, start + 8)?
                .try_into()
                .map_err(|_| FileGrantError::Invalid)?,
        ))
    };
    Ok(FileGrant {
        context: CatalogContext {
            catalog: CatalogId::from_bytes(array(1, 17)?).map_err(|_| FileGrantError::Invalid)?,
            activation_epoch: number(17)?,
        },
        table: TableId::from_bytes(array(25, 41)?).map_err(|_| FileGrantError::Invalid)?,
        principal: array(41, 73)?.try_into().map_err(|_| FileGrantError::Invalid)?,
        nonce: OperationId::from_bytes(array(73, 89)?).map_err(|_| FileGrantError::Invalid)?,
        issued_ms: number(89)?,
        expires_ms: number(97)?,
        operations: FileOperations::from_bits(u16::from_be_bytes(
            array(105, 107)?.try_into().map_err(|_| FileGrantError::Invalid)?,
        ))?,
        max_request_bytes: number(107)?,
        max_file_bytes: number(115)?,
    })
}
