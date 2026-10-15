use std::collections::BTreeMap;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::{error::ValidationError, key::FileId};

use super::{ChunkRoot, ContentFormat, FileContent, FileKind, FileLocation, FileRecord, TableLocation};

const PAYLOAD_BYTES: usize = 142;
const TOKEN_BYTES: usize = PAYLOAD_BYTES + 32;

/// Persistent file locators use a key ring independent of expiring HTTP grants.
#[derive(Clone)]
pub struct DirectFileLocations {
    active_version: u32,
    keys: BTreeMap<u32, [u8; 32]>,
}

impl DirectFileLocations {
    /// # Errors
    /// Rejects a configured replacement of the catalog's reserved key version.
    pub fn for_catalog(
        &self,
        key: crate::catalog::CatalogLocatorKey,
        strict: bool,
    ) -> Result<Self, ValidationError> {
        let key = key.bytes();
        if strict && self.keys.get(&1).is_some_and(|configured| *configured != key) {
            return Err(ValidationError::IdentityMismatch);
        }
        let mut keys = self.keys.clone();
        keys.insert(1, key);
        Self::new(self.active_version, keys)
    }
    /// # Errors
    /// Rejects stems which cannot fit the final authenticated object key.
    pub fn validate_stem(stem: &str) -> Result<(), ValidationError> {
        const LOCATOR_KEY_BYTES: usize = 99 + 5 + 1 + 232 + 8;
        super::validate_relative_key(stem)?;
        if stem.contains('/') || stem.len() > super::MAX_OBJECT_KEY_BYTES - LOCATOR_KEY_BYTES {
            return Err(ValidationError::Key);
        }
        Ok(())
    }
    /// # Errors
    /// Rejects missing active keys, reserved versions and uninitialized keys.
    pub fn new(active_version: u32, keys: BTreeMap<u32, [u8; 32]>) -> Result<Self, ValidationError> {
        if !keys.contains_key(&active_version)
            || keys.iter().any(|(version, key)| *version == 0 || *key == [0; 32])
        {
            return Err(ValidationError::Record);
        }
        Ok(Self { active_version, keys })
    }

    /// Called only after the supplied native tree is durable.
    ///
    /// # Errors
    /// Rejects legacy tables, invalid stems and inconsistent native file descriptors.
    pub fn parquet(
        &self,
        table: TableLocation,
        stem: &str,
        file: FileId,
        length: u64,
        digest: [u8; 32],
        root: ChunkRoot,
    ) -> Result<FileRecord, ValidationError> {
        if table.storage_namespace.is_none() || root.height == 0 {
            return Err(ValidationError::Key);
        }
        Self::validate_stem(stem)?;
        let mut payload = Vec::with_capacity(TOKEN_BYTES);
        payload.push(1);
        payload.extend_from_slice(&self.active_version.to_be_bytes());
        payload.extend_from_slice(file.as_bytes());
        payload.extend_from_slice(&length.to_be_bytes());
        payload.extend_from_slice(&digest);
        encode_root(&mut payload, &root);
        let signature = self
            .signer(self.active_version, table, stem, &payload)?
            .finalize()
            .into_bytes();
        payload.extend_from_slice(&signature);
        let location = table.file(&format!(
            "f/v1/{stem}.{}.parquet",
            URL_SAFE_NO_PAD.encode(payload)
        ))?;
        let record = FileRecord {
            file,
            location,
            kind: FileKind::Unbound,
            format: ContentFormat::Parquet,
            length,
            digest,
            content: FileContent::Chunks { root: Some(root) },
            hint: None,
        };
        record.validate()?;
        Ok(record)
    }

    /// Reconstructs a read descriptor without consulting any file record or state.
    /// HTTP permission validation remains the caller's separate responsibility.
    ///
    /// # Errors
    /// Rejects reserved-path corruption, altered identities and unknown signing keys.
    pub fn resolve(&self, location: &FileLocation) -> Result<Option<FileRecord>, ValidationError> {
        if location.table().storage_namespace.is_some() && location.relative_key() == "f" {
            return Err(ValidationError::Key);
        }
        if location.table().storage_namespace.is_none() || !location.relative_key().starts_with("f/") {
            return Ok(None);
        }
        let name = location
            .relative_key()
            .strip_prefix("f/v1/")
            .ok_or(ValidationError::Key)?;
        let name = name.strip_suffix(".parquet").ok_or(ValidationError::Key)?;
        let (stem, token) = name.rsplit_once('.').ok_or(ValidationError::Key)?;
        if stem.contains('/') || token.len() != TOKEN_BYTES.div_ceil(3) * 4 {
            return Err(ValidationError::Key);
        }
        super::validate_relative_key(stem)?;
        let payload = URL_SAFE_NO_PAD.decode(token).map_err(|_| ValidationError::Key)?;
        if payload.len() != TOKEN_BYTES || payload[0] != 1 {
            return Err(ValidationError::Key);
        }
        let version = u32::from_be_bytes(payload[1..5].try_into().map_err(|_| ValidationError::Key)?);
        self.signer(version, location.table(), stem, &payload[..PAYLOAD_BYTES])?
            .verify_slice(&payload[PAYLOAD_BYTES..])
            .map_err(|_| ValidationError::IdentityMismatch)?;
        let mut fields = Cursor(&payload[5..PAYLOAD_BYTES]);
        let file = FileId::from_bytes(&fields.take::<16>()?)?;
        let length = fields.number()?;
        let digest = fields.take()?;
        let root = decode_root(&mut fields)?;
        if root.height == 0 {
            return Err(ValidationError::Record);
        }
        let record = FileRecord {
            file,
            location: location.clone(),
            kind: FileKind::Unbound,
            format: ContentFormat::Parquet,
            length,
            digest,
            content: FileContent::Chunks { root: Some(root) },
            hint: None,
        };
        record.validate()?;
        Ok(Some(record))
    }

    fn signer(
        &self,
        version: u32,
        table: TableLocation,
        stem: &str,
        payload: &[u8],
    ) -> Result<Hmac<Sha256>, ValidationError> {
        let key = self.keys.get(&version).ok_or(ValidationError::IdentityMismatch)?;
        let path = table.storage_path().ok_or(ValidationError::Key)?;
        let mut signer = Hmac::<Sha256>::new_from_slice(key).map_err(|_| ValidationError::Record)?;
        signer.update(b"iceberg-native-parquet-locator-v1");
        signer.update(path.to_string().as_bytes());
        signer.update(&(stem.len() as u64).to_be_bytes());
        signer.update(stem.as_bytes());
        signer.update(payload);
        Ok(signer)
    }
}

fn encode_root(bytes: &mut Vec<u8>, root: &ChunkRoot) {
    for field in [
        root.chunk.high,
        root.chunk.low,
        root.offset,
        root.physical_length,
        root.logical_offset,
        root.logical_length,
    ] {
        bytes.extend_from_slice(&field.to_be_bytes());
    }
    bytes.push(root.height);
    bytes.extend_from_slice(&root.digest);
}

fn decode_root(fields: &mut Cursor<'_>) -> Result<ChunkRoot, ValidationError> {
    Ok(ChunkRoot {
        chunk: crowdb_protocol::common::ChunkId {
            high: fields.number()?,
            low: fields.number()?,
        },
        offset: fields.number()?,
        physical_length: fields.number()?,
        logical_offset: fields.number()?,
        logical_length: fields.number()?,
        height: fields.take::<1>()?[0],
        digest: fields.take()?,
    })
}

struct Cursor<'a>(&'a [u8]);

impl Cursor<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], ValidationError> {
        let value = self
            .0
            .get(..N)
            .ok_or(ValidationError::Record)?
            .try_into()
            .map_err(|_| ValidationError::Record)?;
        self.0 = &self.0[N..];
        Ok(value)
    }

    fn number(&mut self) -> Result<u64, ValidationError> {
        Ok(u64::from_be_bytes(self.take()?))
    }
}
