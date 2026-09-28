use crowdb_protocol::iceberg_fb::{
    FBNamespaceMutation, FBNamespaceMutationArgs, FBNamespaceOperation, FBNamespaceOperationArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::key::{CatalogId, NamespaceId, OperationId, MAX_KEY_BYTES};
use crate::namespace::{
    NamespaceAction, NamespaceIdentifier, NamespaceMutation, NamespaceOperation, NamespaceOutcome,
    NamespacePhase,
};
use crate::operation::RequestIdentity;

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    operation: &NamespaceOperation,
) -> Result<WIPOffset<FBNamespaceOperation<'buffer>>, ValidationError> {
    operation.validate()?;
    let catalog = builder.create_vector(operation.context.catalog.as_bytes());
    let operation_id = builder.create_vector(operation.identity.operation.as_bytes());
    let principal = builder.create_string(&operation.principal);
    let identifier = builder.create_vector(&operation.identifier.encode()?);
    let namespace_id = builder.create_vector(operation.namespace.as_bytes());
    let parent = operation
        .parent
        .map(|parent| builder.create_vector(parent.as_bytes()));
    let input = super::payload::encode_reference(builder, &operation.input)?;
    let mutation = operation
        .mutation
        .as_ref()
        .map(|mutation| encode_mutation(builder, mutation))
        .transpose()?;
    let scan_after = builder.create_vector(&operation.scan_after);
    let outcome_body = operation
        .outcome
        .as_ref()
        .map(|outcome| super::payload::encode_reference(builder, &outcome.body))
        .transpose()?;
    Ok(FBNamespaceOperation::create(
        builder,
        &FBNamespaceOperationArgs {
            catalog: Some(catalog),
            activation_epoch: operation.context.activation_epoch,
            operation: Some(operation_id),
            issued_ms: operation.identity.issued_ms,
            principal: Some(principal),
            action: operation.action as u8,
            identifier: Some(identifier),
            namespace_id: Some(namespace_id),
            parent,
            phase: operation.phase as u8,
            revision: operation.revision,
            input: Some(input),
            mutation,
            scan_after: Some(scan_after),
            scan_generation: operation.scan_generation,
            outcome_status: operation.outcome.as_ref().map_or(0, |outcome| outcome.status),
            outcome_body,
        },
    ))
}

pub(super) fn decode(value: FBNamespaceOperation<'_>) -> Result<NamespaceOperation, ValidationError> {
    if value.principal().len() > 256 || value.scan_after().len() > MAX_KEY_BYTES {
        return Err(ValidationError::RecordTooLarge);
    }
    let operation = NamespaceOperation {
        context: CatalogContext {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            activation_epoch: value.activation_epoch(),
        },
        identity: RequestIdentity {
            operation: OperationId::from_bytes(value.operation().bytes())?,
            issued_ms: value.issued_ms(),
        },
        principal: value.principal().to_owned(),
        action: match value.action() {
            0 => NamespaceAction::Create,
            1 => NamespaceAction::Update,
            2 => NamespaceAction::Drop,
            _ => return Err(ValidationError::Record),
        },
        identifier: NamespaceIdentifier::decode(value.identifier().bytes())?,
        namespace: NamespaceId::from_bytes(value.namespace_id().bytes())?,
        parent: value
            .parent()
            .map(|parent| NamespaceId::from_bytes(parent.bytes()))
            .transpose()?,
        phase: decode_phase(value.phase())?,
        revision: value.revision(),
        input: super::payload::decode_reference(value.input())?,
        mutation: value.mutation().map(decode_mutation).transpose()?,
        scan_after: value.scan_after().bytes().to_vec(),
        scan_generation: value.scan_generation(),
        outcome: match (value.outcome_status(), value.outcome_body()) {
            (0, None) => None,
            (status, Some(body)) if status != 0 => Some(NamespaceOutcome {
                status,
                body: super::payload::decode_reference(body)?,
            }),
            _ => return Err(ValidationError::Record),
        },
    };
    operation.validate()?;
    Ok(operation)
}

fn decode_phase(phase: u8) -> Result<NamespacePhase, ValidationError> {
    match phase {
        0 => Ok(NamespacePhase::Prepared),
        1 => Ok(NamespacePhase::Reserved),
        2 => Ok(NamespacePhase::Admitting),
        3 => Ok(NamespacePhase::Admitted),
        4 => Ok(NamespacePhase::Publishing),
        5 => Ok(NamespacePhase::Published),
        6 => Ok(NamespacePhase::Fencing),
        7 => Ok(NamespacePhase::ProbingNamespaces),
        8 => Ok(NamespacePhase::ProbingTables),
        9 => Ok(NamespacePhase::Restoring),
        10 => Ok(NamespacePhase::Tombstoning),
        11 => Ok(NamespacePhase::Aborting),
        12 => Ok(NamespacePhase::Complete),
        13 => Ok(NamespacePhase::Aborted),
        _ => Err(ValidationError::Record),
    }
}

pub(super) fn encode_mutation<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    mutation: &NamespaceMutation,
) -> Result<WIPOffset<FBNamespaceMutation<'buffer>>, ValidationError> {
    let key = builder.create_vector(&mutation.key);
    let before = mutation
        .before
        .as_ref()
        .map(|before| super::payload::encode_reference(builder, before))
        .transpose()?;
    let after = super::payload::encode_reference(builder, &mutation.after)?;
    Ok(FBNamespaceMutation::create(
        builder,
        &FBNamespaceMutationArgs {
            key: Some(key),
            before,
            after: Some(after),
        },
    ))
}

pub(super) fn decode_mutation(value: FBNamespaceMutation<'_>) -> Result<NamespaceMutation, ValidationError> {
    if value.key().len() > MAX_KEY_BYTES {
        return Err(ValidationError::KeyTooLarge);
    }
    Ok(NamespaceMutation {
        key: value.key().bytes().to_vec(),
        before: value.before().map(super::payload::decode_reference).transpose()?,
        after: super::payload::decode_reference(value.after())?,
    })
}
