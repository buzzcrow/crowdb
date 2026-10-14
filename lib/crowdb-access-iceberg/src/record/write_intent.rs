use crowdb_protocol::iceberg_fb::{FBFileWriteIntent, FBFileWriteIntentArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::{
    error::ValidationError,
    file::{FileIdentity, FileWriteIntent, TableLocation},
    key::{CatalogId, FileId, NamespaceId, OperationId, TableId},
};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    intent: &FileWriteIntent,
) -> Result<WIPOffset<FBFileWriteIntent<'buffer>>, ValidationError> {
    intent.validate()?;
    let catalog = builder.create_vector(intent.owner.table.catalog.as_bytes());
    let table_id = builder.create_vector(intent.owner.table.table.as_bytes());
    let file_id = builder.create_vector(intent.owner.file.as_bytes());
    let identity = builder.create_vector(intent.identity.as_bytes());
    let storage_namespace = intent
        .owner
        .table
        .storage_namespace
        .map(|id| builder.create_vector(id.as_bytes()));
    let root = super::file::encode_root(builder, &intent.root);
    Ok(FBFileWriteIntent::create(
        builder,
        &FBFileWriteIntentArgs {
            catalog: Some(catalog),
            table_id: Some(table_id),
            file_id: Some(file_id),
            identity: Some(identity),
            root: Some(root),
            created_ms: intent.created_ms,
            not_before_ms: intent.not_before_ms,
            deleting: intent.deleting,
            storage_namespace,
        },
    ))
}

pub(super) fn decode(value: FBFileWriteIntent<'_>) -> Result<FileWriteIntent, ValidationError> {
    let intent = FileWriteIntent {
        identity: OperationId::from_bytes(value.identity().bytes())?,
        owner: FileIdentity {
            table: TableLocation {
                storage_namespace: value
                    .storage_namespace()
                    .map(|id| NamespaceId::from_bytes(id.bytes()))
                    .transpose()?,
                catalog: CatalogId::from_bytes(value.catalog().bytes())?,
                table: TableId::from_bytes(value.table_id().bytes())?,
            },
            file: FileId::from_bytes(value.file_id().bytes())?,
        },
        root: super::file::decode_root(value.root())?,
        created_ms: value.created_ms(),
        not_before_ms: value.not_before_ms(),
        deleting: value.deleting(),
    };
    intent.validate()?;
    Ok(intent)
}
