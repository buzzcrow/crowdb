use crowdb_protocol::iceberg_fb::{
    FBPayloadPage, FBPayloadPageArgs, FBPayloadReference, FBPayloadReferenceArgs, FBRetryResult,
    FBRetryResultArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::error::ValidationError;
use crate::key::{CatalogId, OperationId};
use crate::operation::{PayloadPage, PayloadReference, RetryResult, PAYLOAD_PAGE_BYTES};

pub(super) fn encode_reference<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    reference: &PayloadReference,
) -> Result<WIPOffset<FBPayloadReference<'buffer>>, ValidationError> {
    reference.validate()?;
    let catalog = builder.create_vector(reference.catalog.as_bytes());
    let operation = builder.create_vector(reference.operation.as_bytes());
    let digest = builder.create_vector(&reference.digest);
    let length = u32::try_from(reference.length).map_err(|_| ValidationError::RecordTooLarge)?;
    Ok(FBPayloadReference::create(
        builder,
        &FBPayloadReferenceArgs {
            catalog: Some(catalog),
            operation: Some(operation),
            digest: Some(digest),
            length,
        },
    ))
}

pub(super) fn decode_reference(value: FBPayloadReference<'_>) -> Result<PayloadReference, ValidationError> {
    let reference = PayloadReference {
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        operation: OperationId::from_bytes(value.operation().bytes())?,
        digest: value
            .digest()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
        length: usize::try_from(value.length()).map_err(|_| ValidationError::RecordTooLarge)?,
    };
    reference.validate()?;
    Ok(reference)
}

pub(super) fn encode_page<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    page: &PayloadPage,
) -> Result<WIPOffset<FBPayloadPage<'buffer>>, ValidationError> {
    page.validate()?;
    let reference = encode_reference(builder, &page.reference)?;
    let bytes = builder.create_vector(&page.bytes);
    Ok(FBPayloadPage::create(
        builder,
        &FBPayloadPageArgs {
            reference: Some(reference),
            index: page.index,
            bytes: Some(bytes),
        },
    ))
}

pub(super) fn decode_page(value: FBPayloadPage<'_>) -> Result<PayloadPage, ValidationError> {
    if value.bytes().len() > PAYLOAD_PAGE_BYTES {
        return Err(ValidationError::RecordTooLarge);
    }
    let page = PayloadPage {
        reference: decode_reference(value.reference())?,
        index: value.index(),
        bytes: value.bytes().bytes().to_vec(),
    };
    page.validate()?;
    Ok(page)
}

pub(super) fn encode_result<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    result: &RetryResult,
) -> Result<WIPOffset<FBRetryResult<'buffer>>, ValidationError> {
    result.validate()?;
    let binding = super::retry::encode(builder, &result.binding)?;
    let body = encode_reference(builder, &result.body)?;
    Ok(FBRetryResult::create(
        builder,
        &FBRetryResultArgs {
            binding: Some(binding),
            body: Some(body),
        },
    ))
}

pub(super) fn decode_result(value: FBRetryResult<'_>) -> Result<RetryResult, ValidationError> {
    let result = RetryResult {
        binding: super::retry::decode(value.binding())?,
        body: decode_reference(value.body())?,
    };
    result.validate()?;
    Ok(result)
}
