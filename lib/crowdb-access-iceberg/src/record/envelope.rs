use crowdb_protocol::iceberg_fb::{self as fb, FBIcebergRecord, FBIcebergRecordArgs, FBRecordValue};
use flatbuffers::FlatBufferBuilder;

use crate::catalog::{ActiveCatalogRecord, CatalogAuthority};
use crate::error::ValidationError;
use crate::file::{
    file_key, location_key, FileMapping, FileRecord, MultipartAdmissionRecord, MultipartPart,
    MultipartSession,
};
use crate::key::{CatalogScope, IcebergKey, SystemScope};
use crate::namespace::{authority_key, name_key, NamespaceAuthority, NamespaceMapping, NamespaceOperation};
use crate::operation::{ledger_key_matches, ManagementOperation, PayloadPage, RetryRecord, RetryResult};

pub const MAX_RECORD_BYTES: usize = 64 * 1024;
const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageRecord {
    GcNode(Box<crate::gc::GcNode>),
    GcTask(Box<crate::gc::GcTask>),
    GcCandidate(Box<crate::gc::GcCandidate>),
    GcPage(Box<crate::gc::GcPage>),
    GcPin(Box<crate::gc::GcPin>),
    TableLifecycleOperation(Box<crate::table::TableLifecycleOperation>),
    TablePurgeTask(Box<crate::table::TablePurgeTask>),
    TableCreateOperation(Box<crate::commit::TableCreateOperation>),
    TableCommitOperation(Box<crate::commit::TableCommitOperation>),
    TableHead(Box<crate::table::TableHead>),
    TableMapping(crate::table::TableMapping),
    Active(ActiveCatalogRecord),
    Authority(CatalogAuthority),
    Management(Box<ManagementOperation>),
    Retry(Box<RetryRecord>),
    NamespaceAuthority(Box<NamespaceAuthority>),
    NamespaceMapping(NamespaceMapping),
    PayloadPage(Box<PayloadPage>),
    RetryResult(Box<RetryResult>),
    NamespaceOperation(Box<NamespaceOperation>),
    File(Box<FileRecord>),
    FileMapping(FileMapping),
    MultipartSession(Box<MultipartSession>),
    MultipartPart(Box<MultipartPart>),
    MultipartAdmission(Box<MultipartAdmissionRecord>),
}

impl StorageRecord {
    /// # Errors
    /// Rejects invalid identities, unknown capabilities and record-size overflow.
    pub fn encode(&self) -> Result<Vec<u8>, ValidationError> {
        let mut builder = FlatBufferBuilder::with_capacity(2048);
        let (value_type, value) = self.encode_value(&mut builder)?;
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

    fn encode_value(
        &self,
        builder: &mut FlatBufferBuilder<'_>,
    ) -> Result<(FBRecordValue, flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>), ValidationError> {
        if let Some(value) = self.encode_gc_value(builder)? {
            return Ok(value);
        }
        Ok(match self {
            Self::GcNode(_) | Self::GcTask(_) | Self::GcCandidate(_) | Self::GcPage(_) | Self::GcPin(_) => {
                return Err(ValidationError::Record);
            }
            Self::TableLifecycleOperation(operation) => (
                FBRecordValue::FBTableLifecycleOperation,
                super::table_lifecycle::encode(builder, operation)?.as_union_value(),
            ),
            Self::TablePurgeTask(task) => (
                FBRecordValue::FBTablePurgeTask,
                super::table_lifecycle::encode_purge(builder, task)?.as_union_value(),
            ),
            Self::TableCreateOperation(operation) => (
                FBRecordValue::FBTableCreateOperation,
                super::table_create::encode(builder, operation)?.as_union_value(),
            ),
            Self::TableCommitOperation(operation) => (
                FBRecordValue::FBTableCommitOperation,
                super::table_commit::encode(builder, operation)?.as_union_value(),
            ),
            Self::TableHead(head) => (
                FBRecordValue::FBTableHead,
                super::table::encode_head(builder, head)?.as_union_value(),
            ),
            Self::TableMapping(mapping) => (
                FBRecordValue::FBTableMapping,
                super::table::encode_mapping(builder, mapping)?.as_union_value(),
            ),
            Self::MultipartAdmission(record) => (
                FBRecordValue::FBMultipartAdmission,
                super::multipart_admission::encode(builder, record)?.as_union_value(),
            ),
            Self::MultipartSession(session) => (
                FBRecordValue::FBMultipartSession,
                super::multipart::encode_session(builder, session)?.as_union_value(),
            ),
            Self::MultipartPart(part) => (
                FBRecordValue::FBMultipartPart,
                super::multipart::encode_part(builder, part)?.as_union_value(),
            ),
            Self::File(record) => (
                FBRecordValue::FBFileRecord,
                super::file::encode(builder, record)?.as_union_value(),
            ),
            Self::FileMapping(mapping) => (
                FBRecordValue::FBFileMapping,
                super::file::encode_mapping(builder, mapping).as_union_value(),
            ),
            Self::NamespaceOperation(operation) => (
                FBRecordValue::FBNamespaceOperation,
                super::namespace_operation::encode(builder, operation)?.as_union_value(),
            ),
            Self::PayloadPage(page) => (
                FBRecordValue::FBPayloadPage,
                super::payload::encode_page(builder, page)?.as_union_value(),
            ),
            Self::RetryResult(result) => (
                FBRecordValue::FBRetryResult,
                super::payload::encode_result(builder, result)?.as_union_value(),
            ),
            Self::NamespaceAuthority(authority) => (
                FBRecordValue::FBNamespaceAuthority,
                super::namespace::encode_authority(builder, authority)?.as_union_value(),
            ),
            Self::NamespaceMapping(mapping) => (
                FBRecordValue::FBNamespaceMapping,
                super::namespace::encode_mapping(builder, mapping)?.as_union_value(),
            ),
            Self::Retry(record) => (
                FBRecordValue::FBRetryRecord,
                super::retry::encode(builder, record)?.as_union_value(),
            ),
            Self::Management(operation) => (
                FBRecordValue::FBManagementOperation,
                super::management::encode(builder, operation)?.as_union_value(),
            ),
            Self::Active(root) => (
                FBRecordValue::FBActiveCatalog,
                super::root::encode(builder, *root)?.as_union_value(),
            ),
            Self::Authority(authority) => (
                FBRecordValue::FBCatalogAuthority,
                super::authority::encode(builder, authority)?.as_union_value(),
            ),
        })
    }

    fn encode_gc_value(
        &self,
        builder: &mut FlatBufferBuilder<'_>,
    ) -> Result<Option<(FBRecordValue, flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>)>, ValidationError>
    {
        Ok(Some(match self {
            Self::GcNode(node) => (
                FBRecordValue::FBGcNode,
                super::gc_node::encode(builder, node)?.as_union_value(),
            ),
            Self::GcTask(task) => (
                FBRecordValue::FBGcTask,
                super::gc::encode_task(builder, task)?.as_union_value(),
            ),
            Self::GcCandidate(candidate) => (
                FBRecordValue::FBGcCandidate,
                super::gc::encode_candidate(builder, candidate)?.as_union_value(),
            ),
            Self::GcPage(page) => (
                FBRecordValue::FBGcPage,
                super::gc::encode_page(builder, page)?.as_union_value(),
            ),
            Self::GcPin(pin) => (
                FBRecordValue::FBGcPin,
                super::gc::encode_pin(builder, pin)?.as_union_value(),
            ),
            _ => return Ok(None),
        }))
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
        let record = Self::decode_value(envelope)?;
        record.validate_key(key)?;
        Ok(record)
    }

    fn decode_value(envelope: FBIcebergRecord<'_>) -> Result<Self, ValidationError> {
        match envelope.value_type() {
            FBRecordValue::FBGcNode => {
                return Ok(Self::GcNode(Box::new(super::gc_node::decode(
                    envelope.value_as_fbgc_node().ok_or(ValidationError::Record)?,
                )?)))
            }
            FBRecordValue::FBGcTask => {
                return Ok(Self::GcTask(Box::new(super::gc::decode_task(
                    envelope.value_as_fbgc_task().ok_or(ValidationError::Record)?,
                )?)))
            }
            FBRecordValue::FBGcCandidate => {
                return Ok(Self::GcCandidate(Box::new(super::gc::decode_candidate(
                    envelope
                        .value_as_fbgc_candidate()
                        .ok_or(ValidationError::Record)?,
                )?)))
            }
            FBRecordValue::FBGcPage => {
                return Ok(Self::GcPage(Box::new(super::gc::decode_page(
                    envelope.value_as_fbgc_page().ok_or(ValidationError::Record)?,
                )?)))
            }
            FBRecordValue::FBGcPin => {
                return Ok(Self::GcPin(Box::new(super::gc::decode_pin(
                    envelope.value_as_fbgc_pin().ok_or(ValidationError::Record)?,
                )?)))
            }
            _ => {}
        }
        if matches!(
            envelope.value_type(),
            FBRecordValue::FBTableCreateOperation
                | FBRecordValue::FBTableLifecycleOperation
                | FBRecordValue::FBTablePurgeTask
                | FBRecordValue::FBTableCommitOperation
                | FBRecordValue::FBTableHead
                | FBRecordValue::FBTableMapping
        ) {
            return Self::decode_table(envelope);
        }
        Self::decode_domain(envelope)
    }

    fn decode_table(envelope: FBIcebergRecord<'_>) -> Result<Self, ValidationError> {
        let record = match envelope.value_type() {
            FBRecordValue::FBTableLifecycleOperation => {
                Self::TableLifecycleOperation(Box::new(super::table_lifecycle::decode(
                    envelope
                        .value_as_fbtable_lifecycle_operation()
                        .ok_or(ValidationError::Record)?,
                )?))
            }
            FBRecordValue::FBTablePurgeTask => {
                Self::TablePurgeTask(Box::new(super::table_lifecycle::decode_purge(
                    envelope
                        .value_as_fbtable_purge_task()
                        .ok_or(ValidationError::Record)?,
                )?))
            }
            FBRecordValue::FBTableCreateOperation => {
                Self::TableCreateOperation(Box::new(super::table_create::decode(
                    envelope
                        .value_as_fbtable_create_operation()
                        .ok_or(ValidationError::Record)?,
                )?))
            }
            FBRecordValue::FBTableCommitOperation => {
                Self::TableCommitOperation(Box::new(super::table_commit::decode(
                    envelope
                        .value_as_fbtable_commit_operation()
                        .ok_or(ValidationError::Record)?,
                )?))
            }
            FBRecordValue::FBTableHead => Self::TableHead(Box::new(super::table::decode_head(
                envelope.value_as_fbtable_head().ok_or(ValidationError::Record)?,
            )?)),
            FBRecordValue::FBTableMapping => Self::TableMapping(super::table::decode_mapping(
                envelope
                    .value_as_fbtable_mapping()
                    .ok_or(ValidationError::Record)?,
            )?),
            _ => return Err(ValidationError::Record),
        };
        Ok(record)
    }

    fn decode_domain(envelope: FBIcebergRecord<'_>) -> Result<Self, ValidationError> {
        let record = match envelope.value_type() {
            FBRecordValue::FBMultipartAdmission => {
                Self::MultipartAdmission(Box::new(super::multipart_admission::decode(
                    envelope
                        .value_as_fbmultipart_admission()
                        .ok_or(ValidationError::Record)?,
                )?))
            }
            FBRecordValue::FBMultipartSession => {
                Self::MultipartSession(Box::new(super::multipart::decode_session(
                    envelope
                        .value_as_fbmultipart_session()
                        .ok_or(ValidationError::Record)?,
                )?))
            }
            FBRecordValue::FBMultipartPart => Self::MultipartPart(Box::new(super::multipart::decode_part(
                envelope
                    .value_as_fbmultipart_part()
                    .ok_or(ValidationError::Record)?,
            )?)),
            FBRecordValue::FBFileRecord => Self::File(Box::new(super::file::decode(
                envelope.value_as_fbfile_record().ok_or(ValidationError::Record)?,
            )?)),
            FBRecordValue::FBFileMapping => Self::FileMapping(super::file::decode_mapping(
                envelope
                    .value_as_fbfile_mapping()
                    .ok_or(ValidationError::Record)?,
            )?),
            FBRecordValue::FBNamespaceOperation => {
                Self::NamespaceOperation(Box::new(super::namespace_operation::decode(
                    envelope
                        .value_as_fbnamespace_operation()
                        .ok_or(ValidationError::Record)?,
                )?))
            }
            FBRecordValue::FBPayloadPage => Self::PayloadPage(Box::new(super::payload::decode_page(
                envelope
                    .value_as_fbpayload_page()
                    .ok_or(ValidationError::Record)?,
            )?)),
            FBRecordValue::FBRetryResult => Self::RetryResult(Box::new(super::payload::decode_result(
                envelope
                    .value_as_fbretry_result()
                    .ok_or(ValidationError::Record)?,
            )?)),
            FBRecordValue::FBNamespaceAuthority => {
                Self::NamespaceAuthority(Box::new(super::namespace::decode_authority(
                    envelope
                        .value_as_fbnamespace_authority()
                        .ok_or(ValidationError::Record)?,
                )?))
            }
            FBRecordValue::FBNamespaceMapping => Self::NamespaceMapping(super::namespace::decode_mapping(
                envelope
                    .value_as_fbnamespace_mapping()
                    .ok_or(ValidationError::Record)?,
            )?),
            FBRecordValue::FBRetryRecord => Self::Retry(Box::new(super::retry::decode(
                envelope
                    .value_as_fbretry_record()
                    .ok_or(ValidationError::Record)?,
            )?)),
            FBRecordValue::FBManagementOperation => Self::Management(Box::new(super::management::decode(
                envelope
                    .value_as_fbmanagement_operation()
                    .ok_or(ValidationError::Record)?,
            )?)),
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
        Ok(record)
    }

    fn validate_key(&self, key: &IcebergKey) -> Result<(), ValidationError> {
        match (self, key) {
            (Self::GcNode(node), key) if *key == node.key() || *key == node.pending_key() => Ok(()),
            (Self::GcTask(task), key) if *key == task.key() => Ok(()),
            (Self::GcCandidate(candidate), key) if *key == candidate.key() => Ok(()),
            (Self::GcCandidate(candidate), key)
                if *key == candidate.claim_key()
                    && candidate.phase == crate::gc::CandidatePhase::Retained
                    && candidate.revision == 1 =>
            {
                Ok(())
            }
            (Self::GcPage(page), key) if *key == page.key() => Ok(()),
            (Self::GcPin(pin), key) if *key == pin.key() => Ok(()),
            (Self::TableLifecycleOperation(operation), key) if *key == operation.key() => Ok(()),
            (Self::TablePurgeTask(task), key) if *key == task.key() => Ok(()),
            (Self::TableCreateOperation(operation), key) if *key == operation.key() => Ok(()),
            (Self::TableCommitOperation(operation), key) if *key == operation.key() => Ok(()),
            (Self::TableHead(head), key) if *key == crate::table::head_key(head.catalog, head.table) => {
                Ok(())
            }
            (Self::TableMapping(mapping), key)
                if *key == crate::table::name_key(mapping.catalog, mapping.namespace, &mapping.name)? =>
            {
                Ok(())
            }
            (Self::MultipartAdmission(record), key) if *key == record.key() => Ok(()),
            (Self::MultipartSession(session), key) if *key == session.key() => Ok(()),
            (Self::MultipartPart(part), key) if *key == part.key() => Ok(()),
            (Self::File(record), key) if *key == file_key(record.location.table().catalog, record.file) => {
                Ok(())
            }
            (Self::FileMapping(mapping), key) if *key == location_key(&mapping.location) => Ok(()),
            (Self::NamespaceOperation(operation), key) if *key == operation.key() => Ok(()),
            (Self::PayloadPage(page), key) if *key == page.reference.page_key(page.index)? => Ok(()),
            (Self::RetryResult(result), key) if *key == result.binding.result_key() => Ok(()),
            (Self::NamespaceAuthority(authority), key)
                if *key == authority_key(authority.catalog, authority.namespace) =>
            {
                Ok(())
            }
            (Self::NamespaceMapping(mapping), key)
                if *key == name_key(mapping.catalog, mapping.parent, &mapping.name)? =>
            {
                Ok(())
            }
            (
                Self::Retry(record),
                IcebergKey::System {
                    scope: SystemScope::RetryBinding | SystemScope::RetryOverflow,
                    ..
                },
            ) if record.body.is_empty()
                && ledger_key_matches(SystemScope::RetryBinding, record.identity.operation, key) =>
            {
                Ok(())
            }
            (Self::Retry(record), IcebergKey::Catalog { .. })
                if record.status != 0 && *key == record.result_key() =>
            {
                Ok(())
            }
            (Self::Management(operation), IcebergKey::System { scope, .. })
                if matches!(
                    scope,
                    SystemScope::ManagementOperation
                        | SystemScope::ManagementOverflow
                        | SystemScope::Audit
                        | SystemScope::AuditOverflow
                ) && ledger_key_matches(
                    match scope {
                        SystemScope::ManagementOperation | SystemScope::ManagementOverflow => {
                            SystemScope::ManagementOperation
                        }
                        _ => SystemScope::Audit,
                    },
                    operation.id(),
                    key,
                ) =>
            {
                Ok(())
            }
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
