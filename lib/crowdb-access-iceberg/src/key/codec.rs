use std::ops::Range;

use crate::error::ValidationError;

use super::CatalogId;

pub const MAX_KEY_BYTES: usize = 4096;
const PREFIX: &[u8; 4] = b"ICE\0";
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum SystemScope {
    ActiveRoot = 0,
    ManagementOperation = 1,
    Audit = 2,
    RetryBinding = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CatalogScope {
    Authority = 0,
    NamespaceName = 1,
    NamespaceAuthority = 2,
    TableName = 3,
    TableHead = 4,
    File = 5,
    Operation = 6,
    Reclamation = 7,
    OperationPayload = 8,
    NamespaceOperation = 9,
    FileLocation = 10,
    MultipartSession = 11,
    MultipartPart = 12,
    MultipartAdmission = 13,
    MetadataProjection = 14,
    TableCommitOperation = 15,
    TableCreateOperation = 16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergKey {
    System {
        scope: SystemScope,
        suffix: Vec<u8>,
    },
    Catalog {
        catalog: CatalogId,
        scope: CatalogScope,
        suffix: Vec<u8>,
    },
}

impl IcebergKey {
    /// # Errors
    /// Rejects invalid scope suffixes and oversized keys.
    pub fn encode(&self) -> Result<Vec<u8>, ValidationError> {
        if self.encoded_len() > MAX_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(PREFIX);
        bytes.push(VERSION);
        match self {
            Self::System { scope, suffix } => {
                validate_system(*scope, suffix)?;
                bytes.extend_from_slice(&[0, *scope as u8]);
                bytes.extend_from_slice(suffix);
            }
            Self::Catalog {
                catalog,
                scope,
                suffix,
            } => {
                validate_catalog(*scope, suffix)?;
                bytes.push(1);
                bytes.extend_from_slice(catalog.as_bytes());
                bytes.push(*scope as u8);
                bytes.extend_from_slice(suffix);
            }
        }
        Ok(bytes)
    }

    /// # Errors
    /// Rejects unknown versions, scopes, zero IDs, invalid suffixes and bounds.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValidationError> {
        if bytes.len() > MAX_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        if bytes.len() < 7 || bytes.get(..4) != Some(PREFIX.as_slice()) {
            return Err(ValidationError::Key);
        }
        if bytes[4] != VERSION {
            return Err(ValidationError::KeyVersion(bytes[4]));
        }
        match bytes[5] {
            0 => {
                let scope = system_scope(bytes[6])?;
                validate_system(scope, &bytes[7..])?;
                Ok(Self::System {
                    scope,
                    suffix: bytes[7..].to_vec(),
                })
            }
            1 if bytes.len() >= 23 => {
                let catalog = CatalogId::from_bytes(&bytes[6..22])?;
                let scope = catalog_scope(bytes[22])?;
                validate_catalog(scope, &bytes[23..])?;
                Ok(Self::Catalog {
                    catalog,
                    scope,
                    suffix: bytes[23..].to_vec(),
                })
            }
            _ => Err(ValidationError::Key),
        }
    }

    #[must_use]
    pub fn catalog_range(catalog: CatalogId) -> Range<Vec<u8>> {
        let mut start = Vec::with_capacity(22);
        start.extend_from_slice(PREFIX);
        start.extend_from_slice(&[VERSION, 1]);
        start.extend_from_slice(catalog.as_bytes());
        let mut end = start.clone();
        for byte in end.iter_mut().rev() {
            if *byte != u8::MAX {
                *byte += 1;
                break;
            }
            *byte = 0;
        }
        Range { start, end }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::System { suffix, .. } => 7_usize.saturating_add(suffix.len()),
            Self::Catalog { suffix, .. } => 23_usize.saturating_add(suffix.len()),
        }
    }
}

fn system_scope(value: u8) -> Result<SystemScope, ValidationError> {
    match value {
        0 => Ok(SystemScope::ActiveRoot),
        1 => Ok(SystemScope::ManagementOperation),
        2 => Ok(SystemScope::Audit),
        3 => Ok(SystemScope::RetryBinding),
        _ => Err(ValidationError::Key),
    }
}

fn catalog_scope(value: u8) -> Result<CatalogScope, ValidationError> {
    match value {
        0 => Ok(CatalogScope::Authority),
        1 => Ok(CatalogScope::NamespaceName),
        2 => Ok(CatalogScope::NamespaceAuthority),
        3 => Ok(CatalogScope::TableName),
        4 => Ok(CatalogScope::TableHead),
        5 => Ok(CatalogScope::File),
        6 => Ok(CatalogScope::Operation),
        7 => Ok(CatalogScope::Reclamation),
        8 => Ok(CatalogScope::OperationPayload),
        9 => Ok(CatalogScope::NamespaceOperation),
        10 => Ok(CatalogScope::FileLocation),
        11 => Ok(CatalogScope::MultipartSession),
        12 => Ok(CatalogScope::MultipartPart),
        13 => Ok(CatalogScope::MultipartAdmission),
        14 => Ok(CatalogScope::MetadataProjection),
        15 => Ok(CatalogScope::TableCommitOperation),
        16 => Ok(CatalogScope::TableCreateOperation),
        _ => Err(ValidationError::Key),
    }
}

fn validate_system(scope: SystemScope, suffix: &[u8]) -> Result<(), ValidationError> {
    if scope == SystemScope::ActiveRoot {
        if suffix.is_empty() {
            return Ok(());
        }
        return Err(ValidationError::Key);
    }
    super::OperationId::from_bytes(suffix).map(|_| ())
}

fn validate_catalog(scope: CatalogScope, suffix: &[u8]) -> Result<(), ValidationError> {
    match scope {
        CatalogScope::MetadataProjection => {
            if suffix.len() != 62 {
                return Err(ValidationError::Key);
            }
            let version = u16::from_be_bytes([suffix[56], suffix[57]]);
            let child = u16::from_be_bytes([suffix[58], suffix[59]]);
            let page = u16::from_be_bytes([suffix[60], suffix[61]]);
            if version == 0 || child > 64 || page >= 64 || (child == 0 && page != 0) {
                return Err(ValidationError::Key);
            }
            super::TableId::from_bytes(&suffix[..16]).map(|_| ())
        }
        CatalogScope::Authority | CatalogScope::MultipartAdmission if suffix.is_empty() => Ok(()),
        CatalogScope::Authority | CatalogScope::MultipartAdmission => Err(ValidationError::Key),
        CatalogScope::NamespaceAuthority
        | CatalogScope::TableHead
        | CatalogScope::File
        | CatalogScope::Operation
        | CatalogScope::NamespaceOperation
        | CatalogScope::TableCommitOperation
        | CatalogScope::TableCreateOperation
        | CatalogScope::MultipartSession => super::OperationId::from_bytes(suffix).map(|_| ()),
        CatalogScope::MultipartPart => {
            if suffix.len() != 18 || !(1..=10_000).contains(&u16::from_be_bytes([suffix[16], suffix[17]])) {
                return Err(ValidationError::Key);
            }
            super::OperationId::from_bytes(&suffix[..16]).map(|_| ())
        }
        CatalogScope::NamespaceName | CatalogScope::TableName => {
            let name = super::NameSuffix::decode(suffix)?;
            if scope == CatalogScope::TableName && name.parent.is_none() {
                return Err(ValidationError::Key);
            }
            Ok(())
        }
        CatalogScope::Reclamation => {
            if suffix.len() != 40 {
                return Err(ValidationError::Key);
            }
            super::TableId::from_bytes(&suffix[..16])?;
            super::FileId::from_bytes(&suffix[24..])?;
            Ok(())
        }
        CatalogScope::OperationPayload => {
            if suffix.len() != 50 || u16::from_be_bytes([suffix[48], suffix[49]]) >= 64 {
                return Err(ValidationError::Key);
            }
            super::OperationId::from_bytes(&suffix[..16]).map(|_| ())
        }
        CatalogScope::FileLocation => {
            super::TableId::from_bytes(suffix.get(..16).ok_or(ValidationError::Key)?)?;
            let relative = std::str::from_utf8(&suffix[16..]).map_err(|_| ValidationError::Key)?;
            crate::file::validate_relative_key(relative)
        }
    }
}
