use crowdb_protocol::iceberg_fb::{self as fb, FBIcebergRecord, FBIcebergRecordArgs, FBRecordValue};
use flatbuffers::FlatBufferBuilder;

use crate::catalog::{ActiveCatalogRecord, CatalogAuthority};
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, SystemScope};

pub const MAX_RECORD_BYTES: usize = 64 * 1024;
const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageRecord {
    Active(ActiveCatalogRecord),
    Authority(CatalogAuthority),
}

impl StorageRecord {
    /// # Errors
    /// Rejects invalid identities, unknown capabilities and record-size overflow.
    pub fn encode(&self) -> Result<Vec<u8>, ValidationError> {
        let mut builder = FlatBufferBuilder::with_capacity(2048);
        let (value_type, value) = match self {
            Self::Active(root) => (
                FBRecordValue::FBActiveCatalog,
                super::root::encode(&mut builder, *root)?.as_union_value(),
            ),
            Self::Authority(authority) => (
                FBRecordValue::FBCatalogAuthority,
                super::authority::encode(&mut builder, authority)?.as_union_value(),
            ),
        };
        let envelope = FBIcebergRecord::create(
            &mut builder,
            &FBIcebergRecordArgs {
                schema_version: SCHEMA_VERSION,
                value_type,
                value: Some(value),
            },
        );
        fb::finish_fbiceberg_record_buffer(&mut builder, envelope);
        let bytes = builder.finished_data();
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(ValidationError::RecordTooLarge);
        }
        Ok(bytes.to_vec())
    }

    /// # Errors
    /// Rejects malformed envelopes, unknown schemas, invalid fields or mismatched keys.
    pub fn decode(key: &IcebergKey, bytes: &[u8]) -> Result<Self, ValidationError> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(ValidationError::RecordTooLarge);
        }
        if bytes.len() < 8 || !fb::fbiceberg_record_buffer_has_identifier(bytes) {
            return Err(ValidationError::Record);
        }
        let envelope = fb::root_as_fbiceberg_record(bytes).map_err(|_| ValidationError::Record)?;
        if envelope.schema_version() != SCHEMA_VERSION {
            return Err(ValidationError::RecordVersion(envelope.schema_version()));
        }
        let record = match envelope.value_type() {
            FBRecordValue::FBActiveCatalog => Self::Active(super::root::decode(
                envelope
                    .value_as_fbactive_catalog()
                    .ok_or(ValidationError::Record)?,
            )?),
            FBRecordValue::FBCatalogAuthority => Self::Authority(super::authority::decode(
                envelope
                    .value_as_fbcatalog_authority()
                    .ok_or(ValidationError::Record)?,
            )?),
            _ => return Err(ValidationError::Record),
        };
        record.validate_key(key)?;
        Ok(record)
    }

    fn validate_key(&self, key: &IcebergKey) -> Result<(), ValidationError> {
        match (self, key) {
            (
                Self::Active(_),
                IcebergKey::System {
                    scope: SystemScope::ActiveRoot,
                    suffix,
                },
            ) if suffix.is_empty() => Ok(()),
            (
                Self::Authority(authority),
                IcebergKey::Catalog {
                    catalog,
                    scope: CatalogScope::Authority,
                    suffix,
                },
            ) if *catalog == authority.catalog && suffix.is_empty() => Ok(()),
            _ => Err(ValidationError::IdentityMismatch),
        }
    }
}
