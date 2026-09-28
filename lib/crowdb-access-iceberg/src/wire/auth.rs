use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::catalog::ManagementPrivilege;
use crate::error::ValidationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Principal {
    pub name: &'static str,
    pub management: ManagementPrivilege,
    pub namespace_write: bool,
}

pub struct BearerAuthenticator {
    tokens: [[u8; 32]; 4],
}

impl BearerAuthenticator {
    #[must_use]
    pub fn namespace_token_key(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"crowdb-iceberg-namespace-token-key-v1");
        for token in self.tokens {
            digest.update(token);
        }
        digest.finalize().into()
    }
    /// # Errors
    /// Rejects weak, oversized, duplicate or syntactically invalid bearer tokens.
    pub fn new(reader: &str, writer: &str, manager: &str, clearer: &str) -> Result<Self, ValidationError> {
        let tokens = [reader, writer, manager, clearer];
        if tokens.iter().any(|token| {
            token.len() < 32
                || token.len() > 256
                || !token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/=".contains(&byte))
        }) || tokens
            .iter()
            .enumerate()
            .any(|(index, token)| tokens[..index].contains(token))
        {
            return Err(ValidationError::Text);
        }
        Ok(Self {
            tokens: tokens.map(|token| Sha256::digest(token.as_bytes()).into()),
        })
    }

    #[must_use]
    pub fn authenticate(&self, authorization: &str) -> Option<Principal> {
        let (scheme, token) = authorization.split_once(' ')?;
        if !scheme.eq_ignore_ascii_case("bearer") || token.len() > 256 {
            return None;
        }
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let matches = self.tokens.map(|expected| bool::from(expected.ct_eq(&digest)));
        match matches {
            [true, false, false, false] => Some(Principal {
                name: "reader",
                management: ManagementPrivilege::None,
                namespace_write: false,
            }),
            [false, true, false, false] => Some(Principal {
                name: "writer",
                management: ManagementPrivilege::None,
                namespace_write: true,
            }),
            [false, false, true, false] => Some(Principal {
                name: "manager",
                management: ManagementPrivilege::Manage,
                namespace_write: false,
            }),
            [false, false, false, true] => Some(Principal {
                name: "clearer",
                management: ManagementPrivilege::Clear,
                namespace_write: false,
            }),
            _ => None,
        }
    }
}
