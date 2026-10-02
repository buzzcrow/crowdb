use crowdb_protocol::common::ChunkId;
use crowdb_protocol::iceberg_fb::{
    FBDeletedFile, FBDeletedFileArgs, FBFileChunkRoot, FBFileChunkRootArgs, FBFileMapping, FBFileMappingArgs,
    FBFileRecord, FBFileRecordArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::error::ValidationError;
use crate::file::{
    ChunkRoot, ContentFormat, DeletedFile, FileContent, FileKind, FileMapping, FileRecord, FormatHint,
    InlineCodec,
};
use crate::key::FileId;

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    record: &FileRecord,
) -> Result<WIPOffset<FBFileRecord<'buffer>>, ValidationError> {
    record.validate()?;
    let file_id = builder.create_vector(record.file.as_bytes());
    let location = builder.create_string(&record.location.to_string());
    let digest = builder.create_vector(&record.digest);
    let (storage, inline_bytes, root, locations, etag) = match &record.content {
        FileContent::Inline { codec, bytes } => (
            match codec {
                InlineCodec::Raw => 0,
                InlineCodec::Lz4 => 1,
            },
            Some(builder.create_vector(bytes)),
            None,
            None,
            None,
        ),
        FileContent::Chunks { root } => (
            2,
            None,
            root.as_ref().map(|root| encode_root(builder, root)),
            None,
            None,
        ),
        FileContent::Locations { bytes, etag } => (
            3,
            None,
            None,
            Some(builder.create_vector(bytes)),
            Some(builder.create_string(etag)),
        ),
    };
    Ok(FBFileRecord::create(
        builder,
        &FBFileRecordArgs {
            file_id: Some(file_id),
            location: Some(location),
            kind: record.kind as u8,
            format: record.format as u8,
            length: record.length,
            digest: Some(digest),
            storage,
            inline_bytes,
            root,
            has_hint: record.hint.is_some(),
            hint_offset: record.hint.map_or(0, |hint| hint.offset),
            hint_length: record.hint.map_or(0, |hint| hint.length),
            locations,
            etag,
        },
    ))
}

pub(super) fn decode(value: FBFileRecord<'_>) -> Result<FileRecord, ValidationError> {
    let content = match (
        value.storage(),
        value.inline_bytes(),
        value.root(),
        value.locations(),
        value.etag(),
    ) {
        (codec @ (0 | 1), Some(bytes), None, None, None) => FileContent::Inline {
            codec: if codec == 0 {
                InlineCodec::Raw
            } else {
                InlineCodec::Lz4
            },
            bytes: bytes.bytes().to_vec(),
        },
        (2, None, root, None, None) => FileContent::Chunks {
            root: root.map(decode_root).transpose()?,
        },
        (3, None, None, Some(locations), Some(etag)) => FileContent::Locations {
            bytes: locations.bytes().to_vec(),
            etag: etag.to_owned(),
        },
        _ => return Err(ValidationError::Record),
    };
    let record = FileRecord {
        file: FileId::from_bytes(value.file_id().bytes())?,
        location: value.location().parse()?,
        kind: match value.kind() {
            0 => FileKind::Metadata,
            1 => FileKind::ManifestList,
            2 => FileKind::Manifest,
            3 => FileKind::Data,
            4 => FileKind::PositionDelete,
            5 => FileKind::EqualityDelete,
            6 => FileKind::DeletionVector,
            7 => FileKind::Statistics,
            8 => FileKind::Unbound,
            _ => return Err(ValidationError::Record),
        },
        format: match value.format() {
            0 => ContentFormat::Json,
            1 => ContentFormat::Avro,
            2 => ContentFormat::Parquet,
            3 => ContentFormat::Orc,
            4 => ContentFormat::Puffin,
            5 => ContentFormat::Opaque,
            _ => return Err(ValidationError::Record),
        },
        length: value.length(),
        digest: value
            .digest()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
        content,
        hint: value.has_hint().then_some(FormatHint {
            offset: value.hint_offset(),
            length: value.hint_length(),
        }),
    };
    record.validate()?;
    Ok(record)
}

pub(super) fn encode_root<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    root: &ChunkRoot,
) -> WIPOffset<FBFileChunkRoot<'buffer>> {
    let digest = builder.create_vector(&root.digest);
    FBFileChunkRoot::create(
        builder,
        &FBFileChunkRootArgs {
            chunk_high: root.chunk.high,
            chunk_low: root.chunk.low,
            offset: root.offset,
            physical_length: root.physical_length,
            logical_offset: root.logical_offset,
            logical_length: root.logical_length,
            height: root.height,
            digest: Some(digest),
        },
    )
}

pub(super) fn decode_root(root: FBFileChunkRoot<'_>) -> Result<ChunkRoot, ValidationError> {
    Ok(ChunkRoot {
        chunk: ChunkId {
            high: root.chunk_high(),
            low: root.chunk_low(),
        },
        offset: root.offset(),
        physical_length: root.physical_length(),
        logical_offset: root.logical_offset(),
        logical_length: root.logical_length(),
        height: root.height(),
        digest: root
            .digest()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
    })
}

pub(super) fn encode_mapping<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    mapping: &FileMapping,
) -> WIPOffset<FBFileMapping<'buffer>> {
    let location = builder.create_string(&mapping.location.to_string());
    let file_id = builder.create_vector(mapping.file.as_bytes());
    FBFileMapping::create(
        builder,
        &FBFileMappingArgs {
            location: Some(location),
            file_id: Some(file_id),
        },
    )
}

pub(super) fn decode_mapping(value: FBFileMapping<'_>) -> Result<FileMapping, ValidationError> {
    Ok(FileMapping {
        location: value.location().parse()?,
        file: FileId::from_bytes(value.file_id().bytes())?,
    })
}

pub(super) fn encode_deleted<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    record: &DeletedFile,
) -> Result<WIPOffset<FBDeletedFile<'buffer>>, ValidationError> {
    record.validate()?;
    let file = encode(builder, &record.file)?;
    Ok(FBDeletedFile::create(
        builder,
        &FBDeletedFileArgs {
            file: Some(file),
            deleted_ms: record.deleted_ms,
        },
    ))
}

pub(super) fn decode_deleted(value: FBDeletedFile<'_>) -> Result<DeletedFile, ValidationError> {
    let record = DeletedFile {
        file: decode(value.file())?,
        deleted_ms: value.deleted_ms(),
    };
    record.validate()?;
    Ok(record)
}
