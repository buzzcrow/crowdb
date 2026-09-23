use crowdb_protocol::iceberg_fb::{
    FBMultipartAdmission, FBMultipartAdmissionArgs, FBMultipartCreditMutation, FBMultipartCreditMutationArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::file::{
    MultipartAdmissionLimits, MultipartAdmissionRecord, MultipartCreditAction, MultipartCreditMutation,
};
use crate::key::{CatalogId, OperationId};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    record: &MultipartAdmissionRecord,
) -> Result<WIPOffset<FBMultipartAdmission<'buffer>>, ValidationError> {
    record.validate()?;
    let catalog = builder.create_vector(record.context.catalog.as_bytes());
    let policy = builder.create_vector(record.policy.as_bytes());
    let pending = record
        .pending
        .as_ref()
        .map(|pending| encode_mutation(builder, pending))
        .transpose()?;
    Ok(FBMultipartAdmission::create(
        builder,
        &FBMultipartAdmissionArgs {
            catalog: Some(catalog),
            activation_epoch: record.context.activation_epoch,
            policy: Some(policy),
            revision: record.revision,
            max_sessions: record.limits.max_sessions,
            max_reserved_bytes: record.limits.max_reserved_bytes,
            sessions: record.sessions,
            reserved_bytes: record.reserved_bytes,
            pending,
        },
    ))
}

fn encode_mutation<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    mutation: &MultipartCreditMutation,
) -> Result<WIPOffset<FBMultipartCreditMutation<'buffer>>, ValidationError> {
    let upload = builder.create_vector(mutation.upload.as_bytes());
    let before = mutation
        .before
        .as_ref()
        .map(|reference| super::payload::encode_reference(builder, reference))
        .transpose()?;
    let after = super::payload::encode_reference(builder, &mutation.after)?;
    Ok(FBMultipartCreditMutation::create(
        builder,
        &FBMultipartCreditMutationArgs {
            action: match mutation.action {
                MultipartCreditAction::Reserve => 0,
                MultipartCreditAction::Release => 1,
            },
            upload: Some(upload),
            reservation_bytes: mutation.reservation_bytes,
            before,
            after: Some(after),
        },
    ))
}

pub(super) fn decode(value: FBMultipartAdmission<'_>) -> Result<MultipartAdmissionRecord, ValidationError> {
    let record = MultipartAdmissionRecord {
        context: CatalogContext {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            activation_epoch: value.activation_epoch(),
        },
        policy: OperationId::from_bytes(value.policy().bytes())?,
        revision: value.revision(),
        limits: MultipartAdmissionLimits {
            max_sessions: value.max_sessions(),
            max_reserved_bytes: value.max_reserved_bytes(),
        },
        sessions: value.sessions(),
        reserved_bytes: value.reserved_bytes(),
        pending: value.pending().map(decode_mutation).transpose()?,
    };
    record.validate()?;
    Ok(record)
}

fn decode_mutation(value: FBMultipartCreditMutation<'_>) -> Result<MultipartCreditMutation, ValidationError> {
    Ok(MultipartCreditMutation {
        action: match value.action() {
            0 => MultipartCreditAction::Reserve,
            1 => MultipartCreditAction::Release,
            _ => return Err(ValidationError::Record),
        },
        upload: OperationId::from_bytes(value.upload().bytes())?,
        reservation_bytes: value.reservation_bytes(),
        before: value.before().map(super::payload::decode_reference).transpose()?,
        after: super::payload::decode_reference(value.after())?,
    })
}
