use crowdb_protocol::iceberg_fb::{FBTableHead, FBTableHeadArgs, FBTableMapping, FBTableMappingArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::{
    error::ValidationError,
    key::{CatalogId, FileId, NamespaceId, OperationId, TableId},
    table::{TableHead, TableLifecycle, TableMapping, TableMappingState},
};

pub(super) fn encode_head<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    head: &TableHead,
) -> Result<WIPOffset<FBTableHead<'buffer>>, ValidationError> {
    head.validate()?;
    let catalog = builder.create_vector(head.catalog.as_bytes());
    let table_id = builder.create_vector(head.table.as_bytes());
    let namespace_id = builder.create_vector(head.namespace.as_bytes());
    let name = builder.create_string(&head.name);
    let metadata_file = builder.create_vector(head.metadata_file.as_bytes());
    let metadata_location = builder.create_string(&head.metadata_location.to_string());
    let metadata_digest = builder.create_vector(&head.metadata_digest);
    let table_uuid = head
        .table_uuid
        .map(|value| builder.create_vector(value.as_bytes()));
    let pending_operation = head
        .pending_operation
        .map(|value| builder.create_vector(value.as_bytes()));
    Ok(FBTableHead::create(
        builder,
        &FBTableHeadArgs {
            catalog: Some(catalog),
            table_id: Some(table_id),
            namespace_id: Some(namespace_id),
            name: Some(name),
            name_epoch: head.name_epoch,
            lifecycle: match head.lifecycle {
                TableLifecycle::Ready => 0,
                TableLifecycle::Tombstone => 1,
            },
            generation: head.generation,
            metadata_file: Some(metadata_file),
            metadata_location: Some(metadata_location),
            metadata_digest: Some(metadata_digest),
            format_version: head.format_version,
            table_uuid,
            operation_fence: head.operation_fence,
            pending_operation,
        },
    ))
}

pub(super) fn decode_head(value: FBTableHead<'_>) -> Result<TableHead, ValidationError> {
    let head = TableHead {
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        table: TableId::from_bytes(value.table_id().bytes())?,
        namespace: NamespaceId::from_bytes(value.namespace_id().bytes())?,
        name: value.name().to_owned(),
        name_epoch: value.name_epoch(),
        lifecycle: match value.lifecycle() {
            0 => TableLifecycle::Ready,
            1 => TableLifecycle::Tombstone,
            _ => return Err(ValidationError::Record),
        },
        generation: value.generation(),
        metadata_file: FileId::from_bytes(value.metadata_file().bytes())?,
        metadata_location: value.metadata_location().parse()?,
        metadata_digest: value
            .metadata_digest()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
        format_version: value.format_version(),
        table_uuid: value
            .table_uuid()
            .map(|bytes| uuid::Uuid::from_slice(bytes.bytes()).map_err(|_| ValidationError::Record))
            .transpose()?,
        operation_fence: value.operation_fence(),
        pending_operation: value
            .pending_operation()
            .map(|bytes| OperationId::from_bytes(bytes.bytes()))
            .transpose()?,
    };
    head.validate()?;
    Ok(head)
}

pub(super) fn encode_mapping<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    mapping: &TableMapping,
) -> Result<WIPOffset<FBTableMapping<'buffer>>, ValidationError> {
    mapping.validate()?;
    let catalog = builder.create_vector(mapping.catalog.as_bytes());
    let table_id = builder.create_vector(mapping.table.as_bytes());
    let namespace_id = builder.create_vector(mapping.namespace.as_bytes());
    let name = builder.create_string(&mapping.name);
    let operation = builder.create_vector(mapping.operation.as_bytes());
    Ok(FBTableMapping::create(
        builder,
        &FBTableMappingArgs {
            catalog: Some(catalog),
            table_id: Some(table_id),
            namespace_id: Some(namespace_id),
            name: Some(name),
            name_epoch: mapping.name_epoch,
            operation: Some(operation),
            state: match mapping.state {
                TableMappingState::Reserved => 0,
                TableMappingState::Published => 1,
            },
        },
    ))
}

pub(super) fn decode_mapping(value: FBTableMapping<'_>) -> Result<TableMapping, ValidationError> {
    let mapping = TableMapping {
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        table: TableId::from_bytes(value.table_id().bytes())?,
        namespace: NamespaceId::from_bytes(value.namespace_id().bytes())?,
        name: value.name().to_owned(),
        name_epoch: value.name_epoch(),
        operation: OperationId::from_bytes(value.operation().bytes())?,
        state: match value.state() {
            0 => TableMappingState::Reserved,
            1 => TableMappingState::Published,
            _ => return Err(ValidationError::Record),
        },
    };
    mapping.validate()?;
    Ok(mapping)
}
