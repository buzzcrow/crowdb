use crate::{
    catalog::CatalogContext,
    commit::TableCommitOutcome,
    error::ValidationError,
    key::{CatalogId, OperationId},
    namespace::NamespaceIdentifier,
    operation::RequestIdentity,
    table::{TableLifecycleOperation, TableLifecyclePhase, TablePurgeTask},
};
use crowdb_protocol::iceberg_fb::{
    FBTableLifecycleOperation, FBTableLifecycleOperationArgs, FBTablePurgeTask, FBTablePurgeTaskArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    operation: &TableLifecycleOperation,
) -> Result<WIPOffset<FBTableLifecycleOperation<'buffer>>, ValidationError> {
    operation.validate()?;
    let catalog = builder.create_vector(operation.context.catalog.as_bytes());
    let identity = builder.create_vector(operation.identity.operation.as_bytes());
    let principal = builder.create_string(&operation.principal);
    let input = super::payload::encode_reference(builder, &operation.input)?;
    let source = super::table::encode_mapping(builder, &operation.source)?;
    let before = super::table::encode_head(builder, &operation.before)?;
    let candidate = super::table::encode_head(builder, &operation.candidate)?;
    let destination_namespace = operation
        .destination_namespace
        .as_ref()
        .map(|namespace| namespace.encode().map(|bytes| builder.create_vector(&bytes)))
        .transpose()?;
    let admission = operation
        .admission
        .as_ref()
        .map(|mutation| super::namespace_operation::encode_mutation(builder, mutation))
        .transpose()?;
    let outcome_body = operation
        .outcome
        .as_ref()
        .map(|outcome| super::payload::encode_reference(builder, &outcome.body))
        .transpose()?;
    Ok(FBTableLifecycleOperation::create(
        builder,
        &FBTableLifecycleOperationArgs {
            catalog: Some(catalog),
            activation_epoch: operation.context.activation_epoch,
            operation: Some(identity),
            issued_ms: operation.identity.issued_ms,
            principal: Some(principal),
            revision: operation.revision,
            phase: operation.phase as u8,
            input: Some(input),
            source: Some(source),
            before: Some(before),
            candidate: Some(candidate),
            destination_namespace,
            purge_requested: operation.purge_requested,
            admission,
            outcome_status: operation.outcome.as_ref().map_or(0, |outcome| outcome.status),
            outcome_body,
        },
    ))
}

pub(super) fn decode(
    value: FBTableLifecycleOperation<'_>,
) -> Result<TableLifecycleOperation, ValidationError> {
    if value.principal().len() > 256 {
        return Err(ValidationError::RecordTooLarge);
    }
    let operation = TableLifecycleOperation {
        context: CatalogContext {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            activation_epoch: value.activation_epoch(),
        },
        identity: RequestIdentity {
            operation: OperationId::from_bytes(value.operation().bytes())?,
            issued_ms: value.issued_ms(),
        },
        principal: value.principal().to_owned(),
        revision: value.revision(),
        phase: match value.phase() {
            0 => TableLifecyclePhase::Prepared,
            1 => TableLifecyclePhase::Reserved,
            2 => TableLifecyclePhase::Admitting,
            3 => TableLifecyclePhase::Publishing,
            4 => TableLifecyclePhase::Published,
            5 => TableLifecyclePhase::Complete,
            6 => TableLifecyclePhase::Aborting,
            7 => TableLifecyclePhase::Aborted,
            _ => return Err(ValidationError::Record),
        },
        input: super::payload::decode_reference(value.input())?,
        source: super::table::decode_mapping(value.source())?,
        before: super::table::decode_head(value.before())?,
        candidate: super::table::decode_head(value.candidate())?,
        destination_namespace: value
            .destination_namespace()
            .map(|bytes| NamespaceIdentifier::decode(bytes.bytes()))
            .transpose()?,
        purge_requested: value.purge_requested(),
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

pub(super) fn encode_purge<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    task: &TablePurgeTask,
) -> Result<WIPOffset<FBTablePurgeTask<'buffer>>, ValidationError> {
    task.validate()?;
    let head = super::table::encode_head(builder, &task.head)?;
    Ok(FBTablePurgeTask::create(
        builder,
        &FBTablePurgeTaskArgs {
            activation_epoch: task.activation_epoch,
            head: Some(head),
        },
    ))
}

pub(super) fn decode_purge(value: FBTablePurgeTask<'_>) -> Result<TablePurgeTask, ValidationError> {
    let task = TablePurgeTask {
        activation_epoch: value.activation_epoch(),
        head: super::table::decode_head(value.head())?,
    };
    task.validate()?;
    Ok(task)
}
