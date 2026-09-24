use crate::{
    commit::{TableCreateStage, TableStageBinding},
    error::ValidationError,
    key::OperationId,
    operation::RequestIdentity,
};
use crowdb_protocol::iceberg_fb::{
    FBTableCreateStage, FBTableCreateStageArgs, FBTableStageBinding, FBTableStageBindingArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    stage: &TableCreateStage,
) -> Result<WIPOffset<FBTableCreateStage<'buffer>>, ValidationError> {
    let response = super::super::payload::encode_reference(builder, &stage.response)?;
    let binding = stage
        .binding
        .as_ref()
        .map(|binding| {
            let operation = builder.create_vector(binding.identity.operation.as_bytes());
            let input = super::super::payload::encode_reference(builder, &binding.input)?;
            Ok::<_, ValidationError>(FBTableStageBinding::create(
                builder,
                &FBTableStageBindingArgs {
                    operation: Some(operation),
                    issued_ms: binding.identity.issued_ms,
                    input: Some(input),
                },
            ))
        })
        .transpose()?;
    Ok(FBTableCreateStage::create(
        builder,
        &FBTableCreateStageArgs {
            created_ms: stage.created_ms,
            expires_ms: stage.expires_ms,
            response: Some(response),
            binding,
        },
    ))
}

pub(super) fn decode(value: FBTableCreateStage<'_>) -> Result<TableCreateStage, ValidationError> {
    let binding = value
        .binding()
        .map(|binding| {
            Ok::<_, ValidationError>(TableStageBinding {
                identity: RequestIdentity {
                    operation: OperationId::from_bytes(binding.operation().bytes())?,
                    issued_ms: binding.issued_ms(),
                },
                input: super::super::payload::decode_reference(binding.input())?,
            })
        })
        .transpose()?;
    Ok(TableCreateStage {
        created_ms: value.created_ms(),
        expires_ms: value.expires_ms(),
        response: super::super::payload::decode_reference(value.response())?,
        binding,
    })
}
