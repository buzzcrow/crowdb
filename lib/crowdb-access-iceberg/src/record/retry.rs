use crowdb_protocol::iceberg_fb::{FBRetryRecord, FBRetryRecordArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::key::{CatalogId, OperationId};
use crate::operation::{RequestIdentity, RetryRecord};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    record: &RetryRecord,
) -> Result<WIPOffset<FBRetryRecord<'buffer>>, ValidationError> {
    record.validate()?;
    let operation = builder.create_vector(record.identity.operation.as_bytes());
    let principal = builder.create_string(&record.principal);
    let route = builder.create_string(&record.route);
    let digest = builder.create_vector(&record.digest);
    let catalog = builder.create_vector(record.context.catalog.as_bytes());
    let body = builder.create_vector(&record.body);
    Ok(FBRetryRecord::create(
        builder,
        &FBRetryRecordArgs {
            operation: Some(operation),
            issued_ms: record.identity.issued_ms,
            principal: Some(principal),
            route: Some(route),
            digest: Some(digest),
            catalog: Some(catalog),
            activation_epoch: record.context.activation_epoch,
            retained_until_ms: record.retained_until_ms,
            status: record.status,
            body: Some(body),
        },
    ))
}

pub(super) fn decode(value: FBRetryRecord<'_>) -> Result<RetryRecord, ValidationError> {
    if value.principal().len() > 256 || value.route().len() > 1024 || value.body().len() > 16 * 1024 {
        return Err(ValidationError::RecordTooLarge);
    }
    let record = RetryRecord {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(value.operation().bytes())?,
            issued_ms: value.issued_ms(),
        },
        principal: value.principal().to_owned(),
        route: value.route().to_owned(),
        digest: value
            .digest()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
        context: CatalogContext {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            activation_epoch: value.activation_epoch(),
        },
        retained_until_ms: value.retained_until_ms(),
        status: value.status(),
        body: value.body().bytes().to_vec(),
    };
    record.validate()?;
    Ok(record)
}
