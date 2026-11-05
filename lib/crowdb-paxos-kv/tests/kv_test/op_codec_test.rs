// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Isolated unit tests for `kv/op` wire-format encode/decode.
//!
//! `mem_kv_test.rs` exercises `Batch::decode` indirectly through `InMemKV::apply`.
//! These tests target `Batch::decode` directly: round-trip, truncation, empty
//! payload, multi-op, large keys/values, and boundary conditions.

use bytes::Bytes;
use crowdb_paxos_kv::kv::{Batch, BatchOp, Op};

fn put(key: &[u8], value: &[u8]) -> BatchOp {
    BatchOp {
        key: Bytes::copy_from_slice(key),
        op: Op::Put(Bytes::copy_from_slice(value)),
    }
}

fn del(key: &[u8]) -> BatchOp {
    BatchOp {
        key: Bytes::copy_from_slice(key),
        op: Op::Delete,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn encode(ops: &[(&[u8], Option<&[u8]>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(ops.len() as u16).to_le_bytes());
    for (key, value_opt) in ops {
        buf.push(u8::from(value_opt.is_none())); // 0 = Put, 1 = Delete
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        let vlen = value_opt.map_or(0, <[u8]>::len) as u32;
        buf.extend_from_slice(&vlen.to_le_bytes());
        if let Some(v) = value_opt {
            buf.extend_from_slice(v);
        }
    }
    buf
}

#[test]
fn decode_empty_payload_returns_empty_batch() {
    let payload = Bytes::new();
    let batch = Batch::decode(&payload);
    assert!(batch.ops.is_empty());
}

#[test]
fn decode_zero_ops_returns_empty_batch() {
    let buf = Bytes::from_static(&[0u8, 0u8]);
    let batch = Batch::decode(&buf);
    assert!(batch.ops.is_empty());
}

#[test]
fn decode_single_put_roundtrips() {
    let buf = encode(&[(b"k", Some(b"v"))]);
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops, vec![put(b"k", b"v")]);
}

#[test]
fn decode_single_delete_roundtrips() {
    let buf = encode(&[(b"k", None)]);
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops, vec![del(b"k")]);
}

#[test]
fn decode_multi_op_roundtrips() {
    let buf = encode(&[(b"k1", Some(b"v1")), (b"k2", None), (b"k3", Some(b"v3"))]);
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops, vec![put(b"k1", b"v1"), del(b"k2"), put(b"k3", b"v3")]);
}

#[test]
fn decode_empty_value_put() {
    let buf = encode(&[(b"k", Some(b""))]);
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops, vec![put(b"k", b"")]);
}

#[test]
fn decode_large_key_and_value() {
    let key = vec![0x42u8; 1024];
    let value = vec![0xABu8; 4096];
    let buf = encode(&[(&key, Some(&value))]);
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops.len(), 1);
    assert_eq!(batch.ops[0].key, Bytes::from(key));
    assert_eq!(batch.ops[0].op, Op::Put(Bytes::from(value)));
}

#[test]
fn decode_truncated_after_op_count_stops_cleanly() {
    // Claim 3 ops but provide no data (only op_count bytes).
    let buf = Bytes::from_static(&[3u8, 0u8]);
    let batch = Batch::decode(&buf);
    assert!(batch.ops.is_empty(), "truncated payload yields no ops");
}

#[test]
fn decode_truncated_mid_key_stops_cleanly() {
    // Claim 1 Put, key_len=10, but only 3 key bytes.
    let mut buf = vec![1u8, 0u8, 0u8]; // 1 op (u16 LE), Put
    buf.extend_from_slice(&10u32.to_le_bytes()); // key_len = 10
    buf.extend_from_slice(b"abc"); // only 3 bytes
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops.len(), 1);
    // The decoder reads key_len=10 but only 3 bytes remain; slice clamps to available.
    assert_eq!(batch.ops[0].key, Bytes::from_static(b""));
}

#[test]
fn decode_truncated_mid_value_stops_cleanly() {
    // Claim 1 Put, key="k", value_len=10, but only 3 value bytes.
    let mut buf = vec![1u8, 0u8, 0u8]; // 1 op (u16 LE), Put
    buf.extend_from_slice(&1u32.to_le_bytes()); // key_len = 1
    buf.push(b'k');
    buf.extend_from_slice(&10u32.to_le_bytes()); // value_len = 10
    buf.extend_from_slice(b"abc"); // only 3 bytes
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops.len(), 1);
    // value_len=10 but only 3 bytes remain; slice clamps to available.
    assert_eq!(batch.ops[0].op, Op::Put(Bytes::from_static(b"")));
}

#[test]
fn decode_unknown_kind_byte_becomes_delete() {
    // kind=2 is not 0 (Put) so it falls to the else branch → Delete.
    let mut buf = vec![1u8, 0u8, 2u8]; // 1 op (u16 LE), unknown kind
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.push(b'k');
    buf.extend_from_slice(&0u32.to_le_bytes());
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops, vec![del(b"k")]);
}

#[test]
fn decode_max_op_count_u16() {
    // 300 ops, each a single-byte key Put with empty value.
    let mut buf = (300u16).to_le_bytes().to_vec();
    for i in 0..300u16 {
        buf.push(0u8); // Put
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(u8::try_from(i % 256).expect("i % 256 fits in u8"));
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    let batch = Batch::decode(&Bytes::from(buf));
    assert_eq!(batch.ops.len(), 300);
}

#[test]
fn checked_decode_rejects_every_truncated_prefix_without_partial_operations() {
    let bytes = encode(&[(b"one", Some(b"first")), (b"two", None)]);
    for length in 1..bytes.len() {
        let payload = Bytes::copy_from_slice(&bytes[..length]);
        assert!(Batch::try_decode(&payload).is_err(), "prefix {length}");
    }
    assert_eq!(
        Batch::try_decode(&Bytes::from(bytes)).unwrap().ops,
        vec![put(b"one", b"first"), del(b"two")]
    );
}

#[test]
fn checked_decode_rejects_unknown_kind_delete_payload_and_trailing_bytes() {
    let mut unknown = encode(&[(b"key", None)]);
    unknown[2] = 2;
    assert!(Batch::try_decode(&Bytes::from(unknown)).is_err());
    let mut delete = encode(&[(b"key", Some(b"value"))]);
    delete[2] = 1;
    assert!(Batch::try_decode(&Bytes::from(delete)).is_err());
    let mut trailing = encode(&[(b"key", Some(b"value"))]);
    trailing.push(0);
    assert!(Batch::try_decode(&Bytes::from(trailing)).is_err());
    assert!(Batch::try_decode(&Bytes::from_static(&[0, 0, 1])).is_err());
}

#[test]
fn checked_decode_preserves_empty_gap_entries_and_empty_live_values() {
    assert!(Batch::try_decode(&Bytes::new()).unwrap().ops.is_empty());
    assert!(Batch::try_decode(&Bytes::from_static(&[0, 0]))
        .unwrap()
        .ops
        .is_empty());
    let batch = Batch::try_decode(&Bytes::from(encode(&[(b"", Some(b""))]))).unwrap();
    assert_eq!(batch.ops, vec![put(b"", b"")]);
}
