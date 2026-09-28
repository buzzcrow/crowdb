use crowdb_protocol::iceberg_fb::{FBTableCommitOperation, FBTableCommitOperationArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::{
    catalog::CatalogContext,
    commit::{TableCommitOperation, TableCommitOutcome, TableCommitPhase},
    error::ValidationError,
    key::{CatalogId, OperationId},
    operation::RequestIdentity,
};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    operation: &TableCommitOperation,
) -> Result<WIPOffset<FBTableCommitOperation<'buffer>>, ValidationError> {
    operation.validate()?;
    let catalog = builder.create_vector(operation.context.catalog.as_bytes());
    let identity = builder.create_vector(operation.identity.operation.as_bytes());
    let principal = builder.create_string(&operation.principal);
    let input = super::payload::encode_reference(builder, &operation.input)?;
    let before = super::table::encode_head(builder, &operation.before)?;
    let candidate = operation
        .candidate
        .as_ref()
        .map(|head| super::table::encode_head(builder, head))
        .transpose()?;
    let outcome_body = operation
        .outcome
        .as_ref()
        .map(|outcome| super::payload::encode_reference(builder, &outcome.body))
        .transpose()?;
    Ok(FBTableCommitOperation::create(
        builder,
        &FBTableCommitOperationArgs {
            catalog: Some(catalog),
            activation_epoch: operation.context.activation_epoch,
            operation: Some(identity),
            issued_ms: operation.identity.issued_ms,
            principal: Some(principal),
            revision: operation.revision,
            timestamp_ms: operation.timestamp_ms,
            phase: operation.phase as u8,
            input: Some(input),
            before: Some(before),
            candidate,
            outcome_status: operation.outcome.as_ref().map_or(0, |outcome| outcome.status),
            outcome_body,
        },
    ))
}

pub(super) fn decode(value: FBTableCommitOperation<'_>) -> Result<TableCommitOperation, ValidationError> {
    if value.principal().len() > 256 {
        return Err(ValidationError::RecordTooLarge);
    }
    let operation = TableCommitOperation {
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
        timestamp_ms: value.timestamp_ms(),
        phase: match value.phase() {
            0 => TableCommitPhase::Prepared,
            1 => TableCommitPhase::Validated,
            2 => TableCommitPhase::Writing,
            3 => TableCommitPhase::Publishing,
            4 => TableCommitPhase::Published,
            5 => TableCommitPhase::Complete,
            6 => TableCommitPhase::Rejected,
            _ => return Err(ValidationError::Record),
        },
        input: super::payload::decode_reference(value.input())?,
        before: super::table::decode_head(value.before())?,
        candidate: value.candidate().map(super::table::decode_head).transpose()?,
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
