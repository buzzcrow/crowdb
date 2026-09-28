use crowdb_protocol::iceberg_fb::{FBManagementOperation, FBManagementOperationArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::catalog::ClearBounds;
use crate::error::ValidationError;
use crate::key::{CatalogId, OperationId};
use crate::operation::{
    ManagementAction, ManagementOperation, ManagementPhase, ManagementRequest, RequestIdentity,
};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    operation: &ManagementOperation,
) -> Result<WIPOffset<FBManagementOperation<'buffer>>, ValidationError> {
    operation.validate()?;
    let identity = builder.create_vector(operation.id().as_bytes());
    let principal = builder.create_string(&operation.request.principal);
    let display_name = builder.create_string(&operation.request.display_name);
    let confirmation = operation
        .request
        .confirmation
        .map(|identity| builder.create_vector(identity.as_bytes()));
    let digest = builder.create_vector(&operation.request.digest());
    let candidate = builder.create_vector(operation.candidate.as_bytes());
    let original_root = builder.create_vector(&operation.original_root);
    let original_authority = builder.create_vector(&operation.original_authority);
    let result_authority = builder.create_vector(&operation.result_authority);
    let publication_proof = builder.create_vector(&operation.publication_proof);
    Ok(FBManagementOperation::create(
        builder,
        &FBManagementOperationArgs {
            operation: Some(identity),
            issued_ms: operation.request.identity.issued_ms,
            principal: Some(principal),
            action: operation.request.action as u8,
            expected_epoch: operation.request.expected_epoch,
            display_name: Some(display_name),
            confirmation,
            request_digest: Some(digest),
            phase: operation.phase as u8,
            candidate: Some(candidate),
            original_root: Some(original_root),
            original_authority: Some(original_authority),
            result_authority: Some(result_authority),
            root_lease_ms: operation.bounds.root_lease_ms,
            request_ms: operation.bounds.request_ms,
            delegated_access_ms: operation.bounds.delegated_access_ms,
            clock_skew_ms: operation.bounds.clock_skew_ms,
            retained_until_ms: operation.retained_until_ms,
            publication_proof: Some(publication_proof),
            grace_completed_ms: operation.grace_completed_ms,
            capability_bits: operation.request.capabilities.map_or(0, |value| value.bits()),
        },
    ))
}

pub(super) fn decode(value: FBManagementOperation<'_>) -> Result<ManagementOperation, ValidationError> {
    if value.principal().len() > 256
        || value.display_name().len() > 1024
        || value.original_root().len() > 4096
        || value.original_authority().len() > 4096
        || value.result_authority().len() > 4096
        || value.publication_proof().len() > 4096
    {
        return Err(ValidationError::RecordTooLarge);
    }
    if value.action() != ManagementAction::Activate as u8 && value.capability_bits() != 0 {
        return Err(ValidationError::Record);
    }
    let request = ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(value.operation().bytes())?,
            issued_ms: value.issued_ms(),
        },
        principal: value.principal().to_owned(),
        action: match value.action() {
            0 => ManagementAction::Initialize,
            1 => ManagementAction::Rename,
            2 => ManagementAction::Clear,
            3 => ManagementAction::Activate,
            _ => return Err(ValidationError::Record),
        },
        expected_epoch: value.expected_epoch(),
        display_name: value.display_name().to_owned(),
        confirmation: value
            .confirmation()
            .map(|bytes| CatalogId::from_bytes(bytes.bytes()))
            .transpose()?,
        capabilities: (value.action() == ManagementAction::Activate as u8)
            .then(|| crate::catalog::Capabilities::from_bits(value.capability_bits()))
            .transpose()?,
    };
    if value.request_digest().bytes() != request.digest() {
        return Err(ValidationError::Record);
    }
    let operation = ManagementOperation {
        request,
        phase: match value.phase() {
            0 => ManagementPhase::Prepared,
            1 => ManagementPhase::Published,
            2 => ManagementPhase::Complete,
            3 => ManagementPhase::Conflict,
            _ => return Err(ValidationError::Record),
        },
        candidate: CatalogId::from_bytes(value.candidate().bytes())?,
        original_root: value.original_root().bytes().to_vec(),
        original_authority: value.original_authority().bytes().to_vec(),
        result_authority: value.result_authority().bytes().to_vec(),
        bounds: ClearBounds {
            root_lease_ms: value.root_lease_ms(),
            request_ms: value.request_ms(),
            delegated_access_ms: value.delegated_access_ms(),
            clock_skew_ms: value.clock_skew_ms(),
        },
        retained_until_ms: value.retained_until_ms(),
        publication_proof: value.publication_proof().bytes().to_vec(),
        grace_completed_ms: value.grace_completed_ms(),
    };
    operation.validate()?;
    Ok(operation)
}
