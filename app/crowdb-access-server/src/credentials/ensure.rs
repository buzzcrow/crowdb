// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Concurrent bootstrap reserves one encrypted candidate before credential publication.

use super::{credential_key, CredentialAuthority, CredentialAuthorityError, CREATE_ATTEMPTS};
use crowdb_access_s3::auth::{DurableCredentialRecord, IssuedUserToken};
use crowdb_kv_client::{Error as KvError, GetOutcome, ReadMode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedUser {
    version: u32,
    access_key_id: String,
    record: Vec<u8>,
}

impl CredentialAuthority {
    /// Reuse one credential across concurrent bootstrap callers and restarts.
    /// The user-scoped preparation contains only the existing encrypted format;
    /// publication never replaces another access key's durable record.
    ///
    /// # Errors
    /// Fails closed on duplicate/disabled users, malformed preparation, collisions
    /// and uncertain Group-0 writes. A retry reconciles the same encrypted candidate.
    pub async fn ensure_user(&self, user: &[u8]) -> Result<IssuedUserToken, CredentialAuthorityError> {
        if let Some(token) = self.lookup_user(user).await? {
            return Ok(token);
        }
        let prepared = self.prepare_user(user).await?;
        let key = credential_key(&prepared.access_key_id);
        match self.control.put_cas(0, 0, &key, &prepared.record, 0).await {
            Ok(_) => {}
            Err(KvError::CasFailed { .. } | KvError::CasBusy | KvError::OutcomeUnknown) => {
                match self.control.get(0, 0, &key, ReadMode::Linearizable, None).await? {
                    GetOutcome::Found { value, .. } if value.as_ref() == prepared.record.as_slice() => {}
                    GetOutcome::Found { .. } => return Err(CredentialAuthorityError::Collision),
                    GetOutcome::NotFound => return Err(KvError::OutcomeUnknown.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.lookup_user(user)
            .await?
            .ok_or(CredentialAuthorityError::InvalidUserCredential)
    }

    async fn prepare_user(&self, user: &[u8]) -> Result<PreparedUser, CredentialAuthorityError> {
        let key = format!("crowdb/s3/user-bootstrap/{}", hex::encode(Sha256::digest(user)));
        let mut candidate = None;
        for _ in 0..CREATE_ATTEMPTS {
            match self
                .control
                .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                GetOutcome::Found { value, .. } => {
                    if value.len() > 65536 {
                        return Err(CredentialAuthorityError::InvalidUserPreparation);
                    }
                    let prepared: PreparedUser = serde_json::from_slice(&value)
                        .map_err(|_| CredentialAuthorityError::InvalidUserPreparation)?;
                    self.validate_prepared(user, &prepared)?;
                    return Ok(prepared);
                }
                GetOutcome::NotFound => {}
            }
            let prepared = match candidate.as_ref() {
                Some(prepared) => prepared,
                None => candidate.insert(self.new_prepared(user)?),
            };
            let value =
                serde_json::to_vec(prepared).map_err(|_| CredentialAuthorityError::InvalidUserPreparation)?;
            if value.len() > 65536 {
                return Err(CredentialAuthorityError::InvalidUserPreparation);
            }
            match self.control.put_cas(0, 0, key.as_bytes(), &value, 0).await {
                Ok(_) => return Ok(candidate.take().expect("candidate is present")),
                Err(KvError::CasFailed { .. } | KvError::CasBusy | KvError::OutcomeUnknown) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(KvError::OutcomeUnknown.into())
    }

    fn new_prepared(&self, user: &[u8]) -> Result<PreparedUser, CredentialAuthorityError> {
        let (issued, encrypted) = self.cipher.issue_user_token(user, 1)?;
        let record = DurableCredentialRecord {
            user: user.to_vec(),
            encrypted,
        }
        .encode()?;
        Ok(PreparedUser {
            version: 1,
            access_key_id: issued.access_key_id.clone(),
            record,
        })
    }

    fn validate_prepared(
        &self,
        user: &[u8],
        prepared: &PreparedUser,
    ) -> Result<(), CredentialAuthorityError> {
        if prepared.version != 1 || prepared.access_key_id.is_empty() || prepared.access_key_id.len() > 80 {
            return Err(CredentialAuthorityError::InvalidUserPreparation);
        }
        let record = DurableCredentialRecord::decode(&prepared.record)?;
        if record.user != user {
            return Err(CredentialAuthorityError::InvalidUserPreparation);
        }
        let credential = self
            .cipher
            .decrypt(user, &prepared.access_key_id, &record.encrypted)?;
        if !credential.enabled {
            return Err(CredentialAuthorityError::InvalidUserCredential);
        }
        Ok(())
    }
}
