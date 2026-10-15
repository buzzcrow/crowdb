use crowdb_protocol::iceberg_fb::{FBNativeInjection, FBNativeInjectionArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::{error::ValidationError, file::NativeInjection, key::OperationId, operation::RequestIdentity};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    value: &NativeInjection,
) -> Result<WIPOffset<FBNativeInjection<'buffer>>, ValidationError> {
    value.validate()?;
    let operation = builder.create_vector(value.identity.operation.as_bytes());
    let principal = builder.create_vector(&value.principal);
    let stem = builder.create_string(&value.stem);
    let file = super::file::encode(builder, &value.file)?;
    Ok(FBNativeInjection::create(
        builder,
        &FBNativeInjectionArgs {
            operation: Some(operation),
            issued_ms: value.identity.issued_ms,
            activation_epoch: value.activation_epoch,
            principal: Some(principal),
            stem: Some(stem),
            file: Some(file),
        },
    ))
}

pub(super) fn decode(value: FBNativeInjection<'_>) -> Result<NativeInjection, ValidationError> {
    let receipt = NativeInjection {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(value.operation().bytes())?,
            issued_ms: value.issued_ms(),
        },
        activation_epoch: value.activation_epoch(),
        principal: value
            .principal()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
        stem: value.stem().into(),
        file: super::file::decode(value.file())?,
    };
    receipt.validate()?;
    Ok(receipt)
}
