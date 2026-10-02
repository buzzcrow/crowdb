use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac as _};
use sha2::{Digest as _, Sha256};

use super::{super::FileGrant, FileListRequest};
use crate::error::ValidationError;

pub struct FileListTokens([u8; 32]);

impl FileListTokens {
    /// # Errors
    /// Rejects uninitialized signing configuration.
    pub fn new(key: [u8; 32]) -> Result<Self, ValidationError> {
        if key == [0; 32] {
            return Err(ValidationError::Record);
        }
        Ok(Self(key))
    }

    pub(super) fn issue(
        &self,
        grant: &FileGrant,
        request: &FileListRequest,
        start: &[u8],
        expires: u64,
    ) -> String {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&expires.to_be_bytes());
        bytes.extend_from_slice(&binding(grant, request));
        bytes.extend_from_slice(start);
        let signature = self.signer(&bytes).finalize().into_bytes();
        bytes.extend_from_slice(&signature);
        URL_SAFE_NO_PAD.encode(bytes)
    }

    pub(super) fn verify(
        &self,
        grant: &FileGrant,
        request: &FileListRequest,
        token: &str,
        now: u64,
    ) -> Result<(Vec<u8>, u64), ValidationError> {
        if token.len() > 4_096 {
            return Err(ValidationError::Key);
        }
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| ValidationError::Key)?;
        if bytes.len() < 74 || bytes[0] != 1 {
            return Err(ValidationError::Key);
        }
        let claim = &bytes[..bytes.len() - 32];
        self.signer(claim)
            .verify_slice(&bytes[bytes.len() - 32..])
            .map_err(|_| ValidationError::Key)?;
        let expires = u64::from_be_bytes(bytes[1..9].try_into().map_err(|_| ValidationError::Key)?);
        if now >= expires || expires > grant.expires_ms || bytes[9..41] != binding(grant, request) {
            return Err(ValidationError::Key);
        }
        Ok((claim[41..].to_vec(), expires))
    }

    fn signer(&self, bytes: &[u8]) -> Hmac<Sha256> {
        let mut signer = Hmac::<Sha256>::new_from_slice(&self.0).expect("fixed HMAC key");
        signer.update(b"iceberg-file-list-v1");
        signer.update(bytes);
        signer
    }
}

fn binding(grant: &FileGrant, request: &FileListRequest) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(grant.context.catalog.as_bytes());
    hash.update(grant.context.activation_epoch.to_be_bytes());
    hash.update(grant.table.as_bytes());
    hash.update(grant.principal);
    hash.update(grant.nonce.as_bytes());
    hash.update((request.prefix.len() as u64).to_be_bytes());
    hash.update(request.prefix.as_bytes());
    hash.update([
        u8::from(request.delimiter.is_some()),
        u8::from(request.encoding_url),
    ]);
    hash.finalize().into()
}
