use crowdb_protocol::iceberg_fb::{FBTableCreateOperation, FBTableCreateOperationArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::{
    catalog::CatalogContext,
    commit::{TableCommitOutcome, TableCreateOperation, TableCreatePhase},
    error::ValidationError,
    key::{CatalogId, OperationId},
    namespace::NamespaceIdentifier,
    operation::RequestIdentity,
};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    operation: &TableCreateOperation,
) -> Result<WIPOffset<FBTableCreateOperation<'buffer>>, ValidationError> {
    operation.validate()?;
    let catalog = builder.create_vector(operation.context.catalog.as_bytes());
    let identity = builder.create_vector(operation.identity.operation.as_bytes());
    let principal = builder.create_string(&operation.principal);
    let namespace_name = builder.create_vector(&operation.namespace.encode()?);
    let input = super::payload::encode_reference(builder, &operation.input)?;
    let document = super::payload::encode_reference(builder, &operation.document)?;
    let response = super::payload::encode_reference(builder, &operation.response)?;
    let candidate = super::table::encode_head(builder, &operation.candidate)?;
    let admission = operation
        .admission
        .as_ref()
        .map(|value| super::namespace_operation::encode_mutation(builder, value))
        .transpose()?;
    let outcome_body = operation
        .outcome
        .as_ref()
        .map(|value| super::payload::encode_reference(builder, &value.body))
        .transpose()?;
    Ok(FBTableCreateOperation::create(
        builder,
        &FBTableCreateOperationArgs {
            catalog: Some(catalog),
            activation_epoch: operation.context.activation_epoch,
            operation: Some(identity),
            issued_ms: operation.identity.issued_ms,
            principal: Some(principal),
            namespace_name: Some(namespace_name),
            revision: operation.revision,
            timestamp_ms: operation.timestamp_ms,
            phase: operation.phase as u8,
            input: Some(input),
            document: Some(document),
            response: Some(response),
            candidate: Some(candidate),
            admission,
            outcome_status: operation.outcome.as_ref().map_or(0, |outcome| outcome.status),
            outcome_body,
        },
    ))
}

pub(super) fn decode(value: FBTableCreateOperation<'_>) -> Result<TableCreateOperation, ValidationError> {
    if value.principal().len() > 256 {
        return Err(ValidationError::RecordTooLarge);
    }
    let operation = TableCreateOperation {
        context: CatalogContext {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            activation_epoch: value.activation_epoch(),
        },
        identity: RequestIdentity {
            operation: OperationId::from_bytes(value.operation().bytes())?,
            issued_ms: value.issued_ms(),
        },
        principal: value.principal().to_owned(),
        namespace: NamespaceIdentifier::decode(value.namespace_name().bytes())?,
        revision: value.revision(),
        timestamp_ms: value.timestamp_ms(),
        phase: match value.phase() {
            0 => TableCreatePhase::Prepared,
            1 => TableCreatePhase::Reserved,
            2 => TableCreatePhase::FilesReady,
            3 => TableCreatePhase::Admitting,
            4 => TableCreatePhase::Admitted,
            5 => TableCreatePhase::Publishing,
            6 => TableCreatePhase::Published,
            7 => TableCreatePhase::Complete,
            8 => TableCreatePhase::Aborting,
            9 => TableCreatePhase::Aborted,
            _ => return Err(ValidationError::Record),
        },
        input: super::payload::decode_reference(value.input())?,
        document: super::payload::decode_reference(value.document())?,
        response: super::payload::decode_reference(value.response())?,
        candidate: super::table::decode_head(value.candidate())?,
        admission: value
            .admission()
            .map(super::namespace_operation::decode_mutation)
            .transpose()?,
        outcome: match (value.outcome_status(), value.outcome_body()) {
            (0, None) => None,
            (status, Some(body)) if status != 0 => Some(TableCommitOutcome {
                status,
                body: super::payload::decode_reference(body)?,
            }),
            _ => return Err(ValidationError::Record),
        },
    };
    operation.validate()?;
    Ok(operation)
}
