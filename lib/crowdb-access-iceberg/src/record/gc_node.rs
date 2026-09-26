use crowdb_protocol::iceberg_fb::{FBGcNode, FBGcNodeArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::{
    error::ValidationError,
    gc::{AvroMarkCursor, GcNode, ReachableKind},
    key::{FileId, OperationId},
};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    node: &GcNode,
) -> Result<WIPOffset<FBGcNode<'buffer>>, ValidationError> {
    node.validate()?;
    let task = builder.create_vector(node.task.as_bytes());
    let file_id = builder.create_vector(node.file.as_bytes());
    let location = builder.create_string(&node.location.to_string());
    let digest = builder.create_vector(&node.digest);
    let checkpoint = builder.create_vector(&node.cursor.checkpoint);
    let continuation = node
        .continuation
        .as_ref()
        .map(|value| super::payload::encode_reference(builder, value))
        .transpose()?;
    let head = node
        .head
        .as_ref()
        .map(|value| super::table::encode_head(builder, value))
        .transpose()?;
    Ok(FBGcNode::create(
        builder,
        &FBGcNodeArgs {
            continuation,
            head,
            task: Some(task),
            file_id: Some(file_id),
            location: Some(location),
            digest: Some(digest),
            kind: node.kind as u8,
            checkpoint: Some(checkpoint),
            record_offset: node.cursor.record_offset,
            complete: node.complete,
        },
    ))
}

pub(super) fn decode(value: FBGcNode<'_>) -> Result<GcNode, ValidationError> {
    let node = GcNode {
        continuation: value
            .continuation()
            .map(super::payload::decode_reference)
            .transpose()?,
        head: value.head().map(super::table::decode_head).transpose()?,
        task: OperationId::from_bytes(value.task().bytes())?,
        file: FileId::from_bytes(value.file_id().bytes())?,
        location: value.location().parse()?,
        digest: value
            .digest()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
        kind: match value.kind() {
            0 => ReachableKind::Metadata,
            1 => ReachableKind::ManifestList,
            2 => ReachableKind::Manifest,
            3 => ReachableKind::File,
            _ => return Err(ValidationError::Record),
        },
        cursor: AvroMarkCursor {
            checkpoint: value.checkpoint().bytes().to_vec(),
            record_offset: value.record_offset(),
        },
        complete: value.complete(),
    };
    node.validate()?;
    Ok(node)
}
