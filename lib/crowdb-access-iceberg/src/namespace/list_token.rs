use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::key::NamespaceId;

use super::NamespaceIdentifier;

pub(super) struct ListTokens(Hmac<Sha256>);

impl ListTokens {
    pub(super) fn new(secret: &[u8; 32]) -> Result<Self, ValidationError> {
        Ok(Self(
            Hmac::<Sha256>::new_from_slice(secret).map_err(|_| ValidationError::Identity)?,
        ))
    }

    pub(super) fn binding(
        context: CatalogContext,
        parent: Option<NamespaceId>,
        identifier: Option<&NamespaceIdentifier>,
        limit: usize,
    ) -> Result<[u8; 32], ValidationError> {
        let mut digest = Sha256::new();
        digest.update(b"crowdb-iceberg-namespace-list-v1");
        digest.update(context.catalog.as_bytes());
        digest.update(context.activation_epoch.to_be_bytes());
        digest.update(parent.as_ref().map_or(&[0; 16], NamespaceId::as_bytes));
        digest.update(
            u16::try_from(limit)
                .map_err(|_| ValidationError::Text)?
                .to_be_bytes(),
        );
        if let Some(identifier) = identifier {
            digest.update(identifier.encode()?);
        }
        Ok(digest.finalize().into())
    }

    pub(super) fn encode(&self, binding: &[u8; 32], generation: u64, last_key: &[u8]) -> String {
        let mut payload = Vec::with_capacity(73 + last_key.len());
        payload.push(1);
        payload.extend_from_slice(binding);
        payload.extend_from_slice(&generation.to_be_bytes());
        payload.extend_from_slice(last_key);
        let mut mac = self.0.clone();
        mac.update(&payload);
        payload.extend_from_slice(&mac.finalize().into_bytes());
        URL_SAFE_NO_PAD.encode(payload)
    }

    pub(super) fn decode(&self, token: &str, binding: &[u8; 32]) -> Result<(u64, Vec<u8>), ValidationError> {
        if token.len() > 8192 {
            return Err(ValidationError::KeyTooLarge);
        }
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| ValidationError::Key)?;
        if bytes.len() < 74 || bytes.len() > 73 + crate::key::MAX_KEY_BYTES {
            return Err(ValidationError::Key);
        }
        let (payload, signature) = bytes.split_at(bytes.len() - 32);
        let mut mac = self.0.clone();
        mac.update(payload);
        mac.verify_slice(signature).map_err(|_| ValidationError::Key)?;
        if payload[0] != 1 || &payload[1..33] != binding {
            return Err(ValidationError::IdentityMismatch);
        }
        let generation = u64::from_be_bytes(payload[33..41].try_into().map_err(|_| ValidationError::Key)?);
        if generation == 0 {
            return Err(ValidationError::Key);
        }
        Ok((generation, payload[41..].to_vec()))
    }
}
