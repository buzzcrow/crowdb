// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    decode_stream_chunk_record, encode_stream_chunk_record, StreamChunkRecord, StreamChunkState,
    StreamMappings, StreamName, StreamPurpose,
};
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::FrameLayout;
use crowdb_protocol::key::{BinaryKey, StreamChunkRecordKey, StreamChunkRecordOrder};

fn record(lengths: &[u32]) -> StreamChunkRecord {
    StreamChunkRecord {
        stream_id: StreamName { high: 7, low: 9 },
        purpose: StreamPurpose::Stream,
        chunk_id: ChunkId {
            high: (ChunkType::Stream as u64) << 56,
            low: 1,
        },
        state: StreamChunkState::Sealed,
        logical_start: Some(123),
        logical_end: Some(123 + lengths.iter().map(|length| u64::from(*length)).sum::<u64>()),
        sequence: 1,
        writer_epoch: 2,
        generation: 3,
        owner_session: [4; 16],
        trim_offset: 0,
        closed: false,
        mappings: StreamMappings::new(lengths).unwrap(),
    }
}

#[test]
fn keys_sort_by_stream_then_logical_start_with_candidate_reservations_at_tail() {
    let id = StreamName { high: 7, low: 9 };
    let orders = [
        StreamChunkRecordOrder::Attached(0),
        StreamChunkRecordOrder::Attached(255),
        StreamChunkRecordOrder::Attached(256),
        StreamChunkRecordOrder::Attached(u64::MAX),
        StreamChunkRecordOrder::Candidate(1),
        StreamChunkRecordOrder::Candidate(2),
    ];
    let mut previous = Vec::new();
    for order in orders {
        let key = StreamChunkRecordKey { stream_id: id, order };
        let bytes = key.to_bytes();
        assert_eq!(bytes.len(), 28);
        assert!(bytes.starts_with(&StreamChunkRecordKey::stream_prefix(id)));
        assert!(bytes > previous);
        assert_eq!(StreamChunkRecordKey::from_bytes(&bytes).unwrap(), key);
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(StreamChunkRecordKey::from_bytes(&trailing).is_err());
        previous = bytes;
    }
}

#[test]
fn mapping_codec_preserves_group_boundaries_and_bounded_random_seek() {
    for lengths in [
        vec![4096; 4096],
        vec![4 * 1024 * 1024; 20],
        (0..3073)
            .map(|index| 1 + u32::try_from((index * 731) % 16384).unwrap())
            .collect(),
    ] {
        let record = record(&lengths);
        let bytes = encode_stream_chunk_record(&record).unwrap();
        let decoded = decode_stream_chunk_record(&bytes).unwrap();
        assert_eq!(decoded, record);
        let index = decoded.validate().unwrap();
        let mut logical = 123;
        let mut physical = 0;
        for length in lengths {
            for offset in [logical, logical + u64::from(length) - 1] {
                let mapping = index.locate(&decoded.mappings, offset).unwrap();
                assert_eq!(
                    (mapping.logical_start, mapping.logical_end, mapping.physical_start),
                    (logical, logical + u64::from(length), physical)
                );
            }
            logical += u64::from(length);
            physical += FrameLayout::StreamGroupV2
                .physical_length(u64::from(length))
                .unwrap();
        }
        assert_eq!(index.physical_end(), physical);
        assert!(index.locate(&decoded.mappings, logical).is_err());
        assert!(index.locate(&decoded.mappings, 122).is_err());
        println!(
            "groups={} encoded_record_bytes={} seek_samples_bytes={}",
            decoded.mappings.count(),
            bytes.len(),
            index.resident_bytes()
        );
    }
}

#[test]
fn bounds_checksum_and_current_schema_detect_corruption() {
    let record = record(&[1, 4096, 65536, 4 * 1024 * 1024]);
    let bytes = encode_stream_chunk_record(&record).unwrap();
    for offset in 0..bytes.len() {
        let mut damaged = bytes.clone();
        damaged[offset] ^= 1;
        assert!(decode_stream_chunk_record(&damaged).is_err(), "offset={offset}");
    }
    let mut invalid = record.clone();
    invalid.logical_end = Some(record.logical_end.unwrap() + 1);
    assert!(encode_stream_chunk_record(&invalid).is_err());
    invalid = record.clone();
    invalid.logical_start = None;
    assert!(encode_stream_chunk_record(&invalid).is_err());
    invalid = record;
    invalid.purpose = StreamPurpose::Wal;
    assert!(encode_stream_chunk_record(&invalid).is_err());
    assert!(StreamMappings::new(&[0]).is_err());
    assert!(StreamMappings::new(&[u32::MAX]).is_err());
}

#[test]
fn candidate_has_no_assigned_logical_interval() {
    let mut candidate = record(&[]);
    candidate.state = StreamChunkState::Candidate;
    candidate.logical_start = None;
    candidate.logical_end = None;
    candidate.generation = 0;
    let bytes = encode_stream_chunk_record(&candidate).unwrap();
    assert_eq!(decode_stream_chunk_record(&bytes).unwrap(), candidate);
    candidate.logical_start = Some(0);
    assert!(encode_stream_chunk_record(&candidate).is_err());
}

fn rewrite_checksum(bytes: &mut [u8]) {
    let end = bytes.len() - 4;
    let length = u32::try_from(bytes.len() - 16).unwrap();
    bytes[8..12].copy_from_slice(&length.to_le_bytes());
    let checksum = crowdb_common::ec_isal::crc32c_update(0, &bytes[..end]);
    bytes[end..].copy_from_slice(&checksum.to_le_bytes());
}

#[test]
fn valid_checksum_does_not_authorize_excessive_counts_or_noncanonical_blocks() {
    let record = record(&[1, 2]);
    let bytes = encode_stream_chunk_record(&record).unwrap();
    let length_offset = bytes.len() - 4 - record.mappings.encoded_len() - 8;
    let count_offset = length_offset - 4;
    let mut excessive = bytes.clone();
    excessive[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    rewrite_checksum(&mut excessive);
    assert!(decode_stream_chunk_record(&excessive).is_err());
    excessive = bytes.clone();
    excessive[length_offset..length_offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    rewrite_checksum(&mut excessive);
    assert!(decode_stream_chunk_record(&excessive).is_err());
    let mut split = bytes[..length_offset].to_vec();
    // Two singleton blocks would inflate seek samples instead of using one block.
    let blocks = [1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0];
    split.extend_from_slice(&u64::try_from(blocks.len()).unwrap().to_le_bytes());
    split.extend_from_slice(&blocks);
    split.extend_from_slice(&[0; 4]);
    rewrite_checksum(&mut split);
    assert!(decode_stream_chunk_record(&split).is_err());
}

#[test]
fn stream_identity_record_has_an_id_key_and_preserves_consumer_creation_metadata() {
    use crowdb_protocol::{chunk_stream::StreamRecord, key::StreamRecordKey};
    let id = StreamName {
        high: u64::MAX,
        low: 17,
    };
    let mut record = StreamRecord::creating(id, Some("chunk-kv-partition".into()), StreamPurpose::Wal);
    record.consumer_id = Some("partition-17".into());
    let key = StreamRecordKey { stream_name: id };
    assert_eq!(key.to_bytes().len(), 19);
    assert_eq!(StreamRecordKey::from_bytes(&key.to_bytes()).unwrap(), key);
    assert!(record.created_at_ms > 0);
    let bytes = serde_json::to_vec(&record).unwrap();
    assert_eq!(serde_json::from_slice::<StreamRecord>(&bytes).unwrap(), record);
    let mut later = record.clone();
    later.created_at_ms += 1;
    assert!(record.same_identity(&later));
    later.consumer_id = Some("other-partition".into());
    assert!(!record.same_identity(&later));
    let mut value = serde_json::to_value(&record).unwrap();
    value["old_tail_pointer"] = serde_json::json!(1);
    assert!(serde_json::from_value::<StreamRecord>(value).is_err());
}

#[test]
fn full_chunk_mapping_sizes_and_seek_samples_stay_bounded() {
    use crowdb_protocol::chunk_stream::MAX_STREAM_CHUNK_BYTES;
    for length in [1_u32, 4096] {
        let stride = FrameLayout::StreamGroupV2
            .physical_length(u64::from(length))
            .unwrap();
        let count = usize::try_from(MAX_STREAM_CHUNK_BYTES / stride).unwrap();
        let record = record(&vec![length; count]);
        let encoded = encode_stream_chunk_record(&record).unwrap();
        let index = record.validate().unwrap();
        assert!(encoded.len() < 200_000);
        assert!(index.resident_bytes() < 1024 * 1024);
        for group in [0, count / 2, count - 1] {
            let offset = 123 + u64::try_from(group).unwrap() * u64::from(length);
            let mapping = index.locate(&record.mappings, offset).unwrap();
            assert_eq!(mapping.physical_start, u64::try_from(group).unwrap() * stride);
        }
        println!(
            "append_bytes={length} groups={count} full_chunk_record_bytes={} sample_bytes={}",
            encoded.len(),
            index.resident_bytes()
        );
    }
}
