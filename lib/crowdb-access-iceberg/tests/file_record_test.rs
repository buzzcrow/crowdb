use crowdb_access_iceberg::file::{
    file_key, location_key, ChunkRoot, ContentFormat, FileContent, FileKind, FileMapping, FileRecord,
    FormatHint, InlineCodec, TableLocation, MAX_COMPRESSION_INPUT_BYTES, MAX_INLINE_BYTES,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use crowdb_access_iceberg::record::{StorageRecord, MAX_RECORD_BYTES};
use crowdb_protocol::common::ChunkId;
use sha2::{Digest, Sha256};

fn record(input: &[u8]) -> FileRecord {
    FileRecord {
        file: FileId::random(),
        location: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        }
        .file("metadata/0001.json")
        .unwrap(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: input.len() as u64,
        digest: Sha256::digest(input).into(),
        content: FileContent::select_inline(FileKind::Metadata, input).unwrap(),
        hint: None,
    }
}

#[test]
fn inline_selection_enforces_kind_stored_size_and_compression_input_bounds() {
    for kind in [FileKind::Metadata, FileKind::ManifestList, FileKind::Manifest] {
        for length in [
            0,
            1,
            MAX_INLINE_BYTES,
            MAX_INLINE_BYTES + 1,
            MAX_COMPRESSION_INPUT_BYTES,
        ] {
            let input = vec![b'a'; length];
            let content = FileContent::select_inline(kind, &input).unwrap();
            let FileContent::Inline { codec, bytes } = &content else {
                panic!("not inline")
            };
            assert!(bytes.len() <= MAX_INLINE_BYTES);
            assert_eq!(
                *codec,
                if length <= MAX_INLINE_BYTES {
                    InlineCodec::Raw
                } else {
                    InlineCodec::Lz4
                }
            );
            assert_eq!(
                content
                    .inline_bytes(length as u64, &Sha256::digest(&input).into())
                    .unwrap()
                    .unwrap(),
                input
            );
        }
        assert!(FileContent::select_inline(kind, &vec![0; MAX_COMPRESSION_INPUT_BYTES + 1]).is_none());
        let noise: Vec<u8> = (0_u32..2048)
            .flat_map(|index| Sha256::digest(index.to_be_bytes()))
            .collect();
        assert_eq!(noise.len(), MAX_COMPRESSION_INPUT_BYTES);
        assert!(FileContent::select_inline(kind, &noise).is_none());
    }
    for kind in [
        FileKind::Data,
        FileKind::PositionDelete,
        FileKind::EqualityDelete,
        FileKind::DeletionVector,
        FileKind::Statistics,
    ] {
        for length in [0, 1, MAX_INLINE_BYTES, MAX_COMPRESSION_INPUT_BYTES] {
            assert!(FileContent::select_inline(kind, &vec![0; length]).is_none());
        }
    }
}

#[test]
fn file_records_and_exact_location_mappings_are_key_bound() {
    for input in [b"{}".to_vec(), vec![b' '; MAX_COMPRESSION_INPUT_BYTES]] {
        let file = record(&input);
        let key = file_key(file.location.table().catalog, file.file);
        let value = StorageRecord::File(Box::new(file.clone()));
        let bytes = value.encode().unwrap();
        assert!(bytes.len() < MAX_RECORD_BYTES);
        assert_eq!(StorageRecord::decode(&key, &bytes).unwrap(), value);
        assert!(StorageRecord::decode(&file_key(CatalogId::random(), file.file), &bytes).is_err());
        assert!(
            StorageRecord::decode(&file_key(file.location.table().catalog, FileId::random()), &bytes)
                .is_err()
        );
        let mapping = StorageRecord::FileMapping(FileMapping {
            location: file.location.clone(),
            file: file.file,
        });
        let bytes = mapping.encode().unwrap();
        let key = location_key(&file.location);
        assert_eq!(
            crowdb_access_iceberg::key::IcebergKey::decode(&key.encode().unwrap()).unwrap(),
            key
        );
        assert_eq!(StorageRecord::decode(&key, &bytes).unwrap(), mapping);
        let other = file.location.table().file("metadata/0002.json").unwrap();
        assert!(StorageRecord::decode(&location_key(&other), &bytes).is_err());
    }
}

#[test]
fn malformed_inline_storage_fails_before_unbounded_decompression() {
    let valid = record(&vec![b'x'; MAX_COMPRESSION_INPUT_BYTES]);
    let mut corrupt = valid.clone();
    corrupt.length = u64::MAX;
    assert!(corrupt.validate().is_err());
    corrupt = valid.clone();
    corrupt.digest[0] ^= 1;
    assert!(corrupt.validate().is_err());
    corrupt = valid.clone();
    corrupt.length -= 1;
    assert!(corrupt.validate().is_err());
    corrupt = valid.clone();
    corrupt.content = FileContent::Inline {
        codec: InlineCodec::Lz4,
        bytes: vec![0; MAX_INLINE_BYTES + 1],
    };
    assert!(corrupt.validate().is_err());
    corrupt = valid.clone();
    corrupt.kind = FileKind::Data;
    corrupt.format = ContentFormat::Parquet;
    assert!(corrupt.validate().is_err());
    corrupt = valid;
    corrupt.format = ContentFormat::Puffin;
    assert!(corrupt.validate().is_err());
}

#[test]
fn chunk_roots_are_fixed_size_and_invalid_hints_do_not_replace_canonical_bytes() {
    let mut file = record(b"{}");
    file.content = FileContent::Chunks {
        root: Some(ChunkRoot {
            chunk: ChunkId { high: 1, low: 2 },
            offset: 4096,
            physical_length: 8192,
            logical_offset: 0,
            logical_length: file.length,
            height: 0,
            digest: file.digest,
        }),
    };
    file.hint = Some(FormatHint {
        offset: u64::MAX,
        length: 2,
    });
    assert!(file.usable_hint().is_none());
    file.validate().unwrap();
    let key = file_key(file.location.table().catalog, file.file);
    let value = StorageRecord::File(Box::new(file.clone()));
    assert_eq!(
        StorageRecord::decode(&key, &value.encode().unwrap()).unwrap(),
        value
    );
    if let FileContent::Chunks { root: Some(root) } = &mut file.content {
        root.height = 9;
    }
    assert!(file.validate().is_err());
    file.content = FileContent::Chunks { root: None };
    assert!(file.validate().is_err());
    file.length = 0;
    assert!(file.validate().is_err());
    file.digest = Sha256::digest([]).into();
    file.validate().unwrap();
}

#[test]
fn file_decoder_rejects_unknown_tags_and_incoherent_storage_variants() {
    use crowdb_protocol::iceberg_fb as fb;
    use flatbuffers::FlatBufferBuilder;
    let file = record(b"{}");
    let key = file_key(file.location.table().catalog, file.file);
    for (kind, format, storage, length, payload, digest_length) in [
        (255, 0, 0, 2, true, 32),
        (0, 255, 0, 2, true, 32),
        (0, 0, 255, 2, true, 32),
        (0, 0, 2, 2, true, 32),
        (0, 0, 0, 2, false, 32),
        (0, 0, 0, u64::MAX, true, 32),
        (0, 0, 0, 2, true, 31),
        (0, 0, 1, 2, true, 32),
    ] {
        let mut builder = FlatBufferBuilder::new();
        let file_id = builder.create_vector(file.file.as_bytes());
        let location = builder.create_string(&file.location.to_string());
        let digest = builder.create_vector(&file.digest[..digest_length]);
        let bytes = payload.then(|| builder.create_vector(b"{}"));
        let value = fb::FBFileRecord::create(
            &mut builder,
            &fb::FBFileRecordArgs {
                file_id: Some(file_id),
                location: Some(location),
                digest: Some(digest),
                kind,
                format,
                storage,
                length,
                inline_bytes: bytes,
                ..fb::FBFileRecordArgs::default()
            },
        );
        let envelope = fb::FBIcebergRecord::create(
            &mut builder,
            &fb::FBIcebergRecordArgs {
                schema_version: 1,
                value_type: fb::FBRecordValue::FBFileRecord,
                value: Some(value.as_union_value()),
            },
        );
        fb::finish_fbiceberg_record_buffer(&mut builder, envelope);
        assert!(StorageRecord::decode(&key, builder.finished_data()).is_err());
    }
}
