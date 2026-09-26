// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 persistence for S3 user credentials.

use std::sync::Arc;

use crowdb_access_s3::auth::{
    Credential, CredentialCipher, DurableCredentialRecord, IssuedUserToken, SecretError,
};
use crowdb_kv_client::{CrowdbKvClient, Error as KvError, ReadMode};

const CREDENTIAL_PREFIX: &[u8] = b"\0crowdb/s3/credential/";
const CREATE_ATTEMPTS: usize = 8;

pub struct CredentialAuthority {
    control: Arc<CrowdbKvClient>,
    cipher: Arc<CredentialCipher>,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialAuthorityError {
    #[error("S3 user identity cannot be empty")]
    EmptyUser,
    #[error("S3 credential issuance exhausted collision retries")]
    Collision,
    #[error("S3 credential storage failed: {0}")]
    Storage(#[from] KvError),
    #[error("S3 credential record failed validation: {0}")]
    Record(#[from] SecretError),
    #[error("S3 credential key is malformed")]
    InvalidKey,
    #[error("S3 user has multiple credential records")]
    DuplicateUser,
    #[error("S3 user credential is disabled or malformed")]
    InvalidUserCredential,
}

impl CredentialAuthority {
    #[must_use]
    pub fn new(control: Arc<CrowdbKvClient>, cipher: Arc<CredentialCipher>) -> Self {
        Self { control, cipher }
    }

    /// Issues a one-time user token and persists only its encrypted secret.
    ///
    /// # Errors
    ///
    /// Returns storage, encoding, or bounded collision errors.
    pub async fn issue_user(&self, user: &[u8]) -> Result<IssuedUserToken, CredentialAuthorityError> {
        if user.is_empty() {
            return Err(CredentialAuthorityError::EmptyUser);
        }
        for generation in 1..=CREATE_ATTEMPTS as u64 {
            let (issued, encrypted) = self.cipher.issue_user_token(user, generation)?;
            let key = credential_key(&issued.access_key_id);
            let value = DurableCredentialRecord {
                user: user.to_vec(),
                encrypted,
            }
            .encode()?;
            match self.control.put_cas(0, 0, &key, &value, 0).await {
                Ok(_) => return Ok(issued),
                Err(KvError::CasFailed { .. } | KvError::CasBusy) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(CredentialAuthorityError::Collision)
    }

    /// Reuses an existing credential for a single-writer bootstrap, including
    /// after an issuance response is lost. Never creates a second credential
    /// when the user's durable record is already present.
    ///
    /// # Errors
    ///
    /// Fails closed on duplicate or invalid user records and storage errors.
    pub async fn ensure_user(&self, user: &[u8]) -> Result<IssuedUserToken, CredentialAuthorityError> {
        match self.lookup_user(user).await? {
            Some(token) => Ok(token),
            None => self.issue_user(user).await,
        }
    }

    /// Reads one user's credential without creating durable state.
    ///
    /// # Errors
    ///
    /// Fails closed on duplicate or invalid user records and storage errors.
    pub async fn lookup_user(
        &self,
        user: &[u8],
    ) -> Result<Option<IssuedUserToken>, CredentialAuthorityError> {
        if user.is_empty() {
            return Err(CredentialAuthorityError::EmptyUser);
        }
        let outcome = self
            .control
            .scan(
                0,
                0,
                CREDENTIAL_PREFIX,
                b"",
                b"",
                u32::MAX,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        let mut found = None;
        for (key, value) in outcome.items {
            let access_key = key
                .strip_prefix(CREDENTIAL_PREFIX)
                .and_then(|value| std::str::from_utf8(value).ok())
                .filter(|value| !value.is_empty())
                .ok_or(CredentialAuthorityError::InvalidKey)?;
            let record = DurableCredentialRecord::decode(&value)?;
            if record.user != user {
                continue;
            }
            if found.is_some() {
                return Err(CredentialAuthorityError::DuplicateUser);
            }
            let credential = self.cipher.decrypt(user, access_key, &record.encrypted)?;
            if !credential.enabled {
                return Err(CredentialAuthorityError::InvalidUserCredential);
            }
            let secret_key = String::from_utf8(credential.secret_key.clone())
                .map_err(|_| CredentialAuthorityError::InvalidUserCredential)?;
            found = Some(IssuedUserToken {
                access_key_id: access_key.to_owned(),
                secret_key,
            });
        }
        Ok(found)
    }

    /// Loads and decrypts the complete credential snapshot from group 0.
    ///
    /// # Errors
    ///
    /// Fails closed if any stored credential is malformed or cannot be decrypted.
    pub async fn load_credentials(&self) -> Result<Vec<(String, Credential)>, CredentialAuthorityError> {
        let outcome = self
            .control
            .scan(
                0,
                0,
                CREDENTIAL_PREFIX,
                b"",
                b"",
                u32::MAX,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        let mut credentials = Vec::with_capacity(outcome.items.len());
        for (key, value) in outcome.items {
            let access_key = key
                .strip_prefix(CREDENTIAL_PREFIX)
                .and_then(|value| std::str::from_utf8(value).ok())
                .filter(|value| !value.is_empty())
                .ok_or(CredentialAuthorityError::InvalidKey)?;
            let record = DurableCredentialRecord::decode(&value)?;
            let credential = self.cipher.decrypt(&record.user, access_key, &record.encrypted)?;
            credentials.push((access_key.to_owned(), credential));
        }
        Ok(credentials)
    }
}

fn credential_key(access_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(CREDENTIAL_PREFIX.len() + access_key.len());
    key.extend_from_slice(CREDENTIAL_PREFIX);
    key.extend_from_slice(access_key.as_bytes());
    key
}
