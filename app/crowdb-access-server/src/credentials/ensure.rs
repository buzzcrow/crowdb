// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bootstrap publishes one final encrypted credential without preparation state.

use super::{credential_key, CredentialAuthority, CredentialAuthorityError};
use crowdb_access_s3::auth::{DurableCredentialRecord, IssuedUserToken};
use crowdb_kv_client::{Error as KvError, GetOutcome, ReadMode};
use sha2::{Digest, Sha256};

impl CredentialAuthority {
    /// Reuses an existing token or creates one final resource at a stable public
    /// identifier. Concurrent bootstrap callers converge through create-if-absent;
    /// each candidate secret is random. No upload/workflow recovery record exists.
    /// # Errors
    /// Fails closed on disabled/duplicate users, conflicting user bindings,
    /// malformed ciphertext and unavailable or uncertain Group-0 storage.
    pub async fn ensure_user(&self, user: &[u8]) -> Result<IssuedUserToken, CredentialAuthorityError> {
        if let Some(token) = self.lookup_user(user).await? {
            return Ok(token);
        }
        let mut digest = Sha256::new();
        digest.update(b"crowdb/s3/user-credential/v1\0");
        digest.update(user);
        let access_key_id = format!("CROWU{}", hex::encode(digest.finalize()));
        let (issued, encrypted) = self.cipher.issue_user_token_for_key(user, access_key_id, 1)?;
        let key = credential_key(&issued.access_key_id);
        let bytes = DurableCredentialRecord {
            user: user.to_vec(),
            encrypted,
        }
        .encode()?;
        match self.control.put_cas(0, 0, &key, &bytes, 0).await {
            Ok(_) => Ok(issued),
            Err(KvError::CasFailed {
                observation: Some(observation),
                ..
            }) if observation.key.as_ref() == key.as_slice() && observation.revision != 0 => {
                self.token(user, &issued.access_key_id, &observation.value)
            }
            // Older peers and uncertain/busy outcomes have no comparison value.
            // Reconcile only the final resource, never a preparation record.
            Err(error @ (KvError::CasFailed { .. } | KvError::CasBusy | KvError::OutcomeUnknown)) => {
                match self.control.get(0, 0, &key, ReadMode::Linearizable, None).await? {
                    GetOutcome::Found { value, .. } => self.token(user, &issued.access_key_id, &value),
                    GetOutcome::NotFound => Err(error.into()),
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn token(
        &self,
        user: &[u8],
        access_key: &str,
        value: &[u8],
    ) -> Result<IssuedUserToken, CredentialAuthorityError> {
        let record = DurableCredentialRecord::decode(value)?;
        if record.user != user {
            return Err(CredentialAuthorityError::Collision);
        }
        let mut credential = self.cipher.decrypt(user, access_key, &record.encrypted)?;
        if !credential.enabled {
            return Err(CredentialAuthorityError::InvalidUserCredential);
        }
        Ok(IssuedUserToken {
            access_key_id: access_key.to_owned(),
            secret_key: String::from_utf8(std::mem::take(&mut credential.secret_key))
                .map_err(|_| CredentialAuthorityError::InvalidUserCredential)?,
        })
    }
}
