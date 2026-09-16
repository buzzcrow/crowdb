// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use rand::rngs::OsRng;
use rand::RngCore as _;
use sha2::Sha256;
use zeroize::{Zeroize, ZeroizeOnDrop};

use super::Credential;

const RECORD_VERSION: u8 = 1;
const NONCE_LENGTH: usize = 12;
const ACCESS_RANDOM_LENGTH: usize = 10;
const SECRET_LENGTH: usize = 32;
const CONTINUATION_CONTEXT: &[u8] = b"crowdb/s3/continuation/v1";
const DURABLE_RECORD_VERSION: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("S3 master key must contain exactly 64 hexadecimal characters")]
    InvalidMasterKey,
    #[error("encrypted S3 credential record is malformed")]
    InvalidRecord,
    #[error("S3 credential encryption failed")]
    Encrypt,
    #[error("S3 credential decryption failed")]
    Decrypt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableCredentialRecord {
    pub user: Vec<u8>,
    pub encrypted: EncryptedCredentialRecord,
}

impl DurableCredentialRecord {
    /// Encodes the user binding and encrypted secret for group-0 storage.
    ///
    /// # Errors
    ///
    /// Rejects user identities that cannot be represented by the durable format.
    pub fn encode(&self) -> Result<Vec<u8>, SecretError> {
        let user_length = u32::try_from(self.user.len()).map_err(|_| SecretError::InvalidRecord)?;
        let encrypted = self.encrypted.encode();
        let mut output = Vec::with_capacity(5 + self.user.len() + encrypted.len());
        output.push(DURABLE_RECORD_VERSION);
        output.extend_from_slice(&user_length.to_be_bytes());
        output.extend_from_slice(&self.user);
        output.extend_from_slice(&encrypted);
        Ok(output)
    }

    /// Decodes one group-0 credential value.
    ///
    /// # Errors
    ///
    /// Rejects malformed framing or encrypted records.
    pub fn decode(value: &[u8]) -> Result<Self, SecretError> {
        if value.len() < 5 || value[0] != DURABLE_RECORD_VERSION {
            return Err(SecretError::InvalidRecord);
        }
        let user_length =
            u32::from_be_bytes(value[1..5].try_into().map_err(|_| SecretError::InvalidRecord)?) as usize;
        let encrypted_start = 5_usize
            .checked_add(user_length)
            .filter(|offset| *offset <= value.len())
            .ok_or(SecretError::InvalidRecord)?;
        let user = value[5..encrypted_start].to_vec();
        if user.is_empty() {
            return Err(SecretError::InvalidRecord);
        }
        let encrypted = EncryptedCredentialRecord::decode(&value[encrypted_start..])?;
        Ok(Self { user, encrypted })
    }
}

#[derive(ZeroizeOnDrop)]
pub struct MasterKey([u8; 32]);

impl MasterKey {
    /// Decodes the initial configured 256-bit key.
    ///
    /// # Errors
    ///
    /// Rejects any value other than 64 hexadecimal characters.
    pub fn from_hex(value: &str) -> Result<Self, SecretError> {
        if value.len() != 64 {
            return Err(SecretError::InvalidMasterKey);
        }
        let mut key = [0_u8; 32];
        for (output, pair) in key.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
            let text = std::str::from_utf8(pair).map_err(|_| SecretError::InvalidMasterKey)?;
            *output = u8::from_str_radix(text, 16).map_err(|_| SecretError::InvalidMasterKey)?;
        }
        Ok(Self(key))
    }
}

pub struct CredentialCipher {
    cipher: Aes256Gcm,
    continuation_key: [u8; 32],
}

impl CredentialCipher {
    /// Builds the credential and continuation-key authority.
    ///
    /// # Panics
    ///
    /// Cannot panic for a validated [`MasterKey`]: AES-256 requires exactly
    /// 32 bytes and HMAC-SHA256 accepts keys of that length.
    #[must_use]
    pub fn new(master_key: &MasterKey) -> Self {
        let cipher = Aes256Gcm::new_from_slice(&master_key.0).expect("AES-256 accepts a 32-byte key");
        let mut mac =
            <Hmac<Sha256> as Mac>::new_from_slice(&master_key.0).expect("HMAC accepts keys of every size");
        mac.update(CONTINUATION_CONTEXT);
        let mut continuation_key = [0_u8; 32];
        continuation_key.copy_from_slice(&mac.finalize().into_bytes());
        Self {
            cipher,
            continuation_key,
        }
    }

    #[must_use]
    pub fn continuation_key(&self) -> &[u8; 32] {
        &self.continuation_key
    }

    /// Creates one user token and its encrypted durable record.
    ///
    /// # Errors
    ///
    /// Returns an encryption error without exposing generated secret bytes.
    pub fn issue_user_token(
        &self,
        user: &[u8],
        generation: u64,
    ) -> Result<(IssuedUserToken, EncryptedCredentialRecord), SecretError> {
        let mut access_random = [0_u8; ACCESS_RANDOM_LENGTH];
        let mut secret = [0_u8; SECRET_LENGTH];
        OsRng.fill_bytes(&mut access_random);
        OsRng.fill_bytes(&mut secret);
        let access_key_id = format!("CROW{}", encode_hex(&access_random).to_ascii_uppercase());
        let secret_key = URL_SAFE_NO_PAD.encode(secret);
        secret.zeroize();
        let issued = IssuedUserToken {
            access_key_id,
            secret_key,
        };
        let record = self.encrypt(
            user,
            &issued.access_key_id,
            generation,
            issued.secret_key.as_bytes(),
            true,
        )?;
        Ok((issued, record))
    }

    /// Encrypts one credential for group-0 persistence.
    ///
    /// # Errors
    ///
    /// Returns an encryption error if the AEAD operation fails.
    pub fn encrypt(
        &self,
        user: &[u8],
        access_key_id: &str,
        generation: u64,
        secret: &[u8],
        enabled: bool,
    ) -> Result<EncryptedCredentialRecord, SecretError> {
        let mut nonce = [0_u8; NONCE_LENGTH];
        OsRng.fill_bytes(&mut nonce);
        let aad = associated_data(user, access_key_id, generation);
        let ciphertext = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: secret,
                    aad: &aad,
                },
            )
            .map_err(|_| SecretError::Encrypt)?;
        Ok(EncryptedCredentialRecord {
            generation,
            enabled,
            nonce,
            ciphertext,
        })
    }

    /// Decrypts one group-0 credential record for a cache snapshot.
    ///
    /// # Errors
    ///
    /// Rejects authentication failure without returning partial plaintext.
    pub fn decrypt(
        &self,
        user: &[u8],
        access_key_id: &str,
        record: &EncryptedCredentialRecord,
    ) -> Result<Credential, SecretError> {
        let aad = associated_data(user, access_key_id, record.generation);
        let secret_key = self
            .cipher
            .decrypt(
                Nonce::from_slice(&record.nonce),
                Payload {
                    msg: &record.ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| SecretError::Decrypt)?;
        Ok(Credential {
            secret_key,
            session_token: None,
            enabled: record.enabled,
        })
    }
}

impl Drop for CredentialCipher {
    fn drop(&mut self) {
        self.continuation_key.zeroize();
    }
}

#[derive(ZeroizeOnDrop)]
pub struct IssuedUserToken {
    pub access_key_id: String,
    pub secret_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncryptedCredentialRecord {
    pub generation: u64,
    pub enabled: bool,
    pub nonce: [u8; NONCE_LENGTH],
    pub ciphertext: Vec<u8>,
}

impl EncryptedCredentialRecord {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(22 + self.ciphertext.len());
        output.push(RECORD_VERSION);
        output.extend_from_slice(&self.generation.to_be_bytes());
        output.push(u8::from(self.enabled));
        output.extend_from_slice(&self.nonce);
        output.extend_from_slice(&self.ciphertext);
        output
    }

    /// Decodes a durable encrypted credential record.
    ///
    /// # Errors
    ///
    /// Rejects an unsupported version, invalid enabled flag, or short record.
    pub fn decode(value: &[u8]) -> Result<Self, SecretError> {
        if value.len() < 22 || value[0] != RECORD_VERSION || value[9] > 1 {
            return Err(SecretError::InvalidRecord);
        }
        let generation = u64::from_be_bytes(value[1..9].try_into().map_err(|_| SecretError::InvalidRecord)?);
        let nonce = value[10..22].try_into().map_err(|_| SecretError::InvalidRecord)?;
        let ciphertext = value[22..].to_vec();
        if ciphertext.is_empty() {
            return Err(SecretError::InvalidRecord);
        }
        Ok(Self {
            generation,
            enabled: value[9] == 1,
            nonce,
            ciphertext,
        })
    }
}

fn associated_data(user: &[u8], access_key_id: &str, generation: u64) -> Vec<u8> {
    let mut output = Vec::with_capacity(user.len() + access_key_id.len() + 18);
    output.extend_from_slice(b"crowdb-s3\0");
    output.extend_from_slice(&u32::try_from(user.len()).unwrap_or(u32::MAX).to_be_bytes());
    output.extend_from_slice(user);
    output.extend_from_slice(access_key_id.as_bytes());
    output.extend_from_slice(&generation.to_be_bytes());
    output
}

fn encode_hex(value: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
