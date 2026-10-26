// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{FieldLocator, ReadPage, ResolvedReadPage, SampleView, SnapshotId};
use crowdb_access_server::dataset::DatasetHttpService;
use hyper::body::Bytes;
use md5::{Digest, Md5};
use std::collections::BTreeMap;

const LIMIT: usize = 8 * 1024 * 1024;
const SNAPSHOT: &str = "11111111111111111111111111111111";

#[test]
fn ordered_cells_preserve_unicode_missing_tombstone_empty_and_chunk_wire_bytes() {
    let (prefix, metadata) = DatasetHttpService::batch_header_for_tests(resolved(Some("游标\"/next")))
        .expect("bounded batch header");
    let expected = concat!(
        "{\"snapshot\":\"11111111111111111111111111111111\",",
        "\"sample_ids\":[\"YQ==\",\"+/8=\"],\"fields\":[",
        "{\"sample\":\"YQ==\",\"field\":\"空\",\"state\":\"value\",\"length\":0,",
        "\"md5\":\"d41d8cd98f00b204e9800998ecf8427e\"},",
        "{\"sample\":\"YQ==\",\"field\":\"erased\",\"state\":\"tombstone\",\"length\":0,\"md5\":null},",
        "{\"sample\":\"YQ==\",\"field\":\"absent\",\"state\":\"missing\",\"length\":0,\"md5\":null},",
        "{\"sample\":\"YQ==\",\"field\":\"blob\",\"state\":\"value\",\"length\":1,",
        "\"md5\":\"9dd4e461268c8034f5c8564e155c67a6\"},",
        "{\"sample\":\"+/8=\",\"field\":\"空\",\"state\":\"missing\",\"length\":0,\"md5\":null},",
        "{\"sample\":\"+/8=\",\"field\":\"erased\",\"state\":\"missing\",\"length\":0,\"md5\":null},",
        "{\"sample\":\"+/8=\",\"field\":\"absent\",\"state\":\"missing\",\"length\":0,\"md5\":null},",
        "{\"sample\":\"+/8=\",\"field\":\"blob\",\"state\":\"value\",\"length\":4294967297,",
        "\"md5\":\"abababababababababababababababab\"}],",
        "\"next\":\"游标\\\"/next\",\"end\":false}"
    );
    assert_wire(&prefix, &metadata, expected);
}

#[test]
fn terminal_header_keeps_sample_and_field_order_without_a_cursor() {
    let (prefix, metadata) = DatasetHttpService::batch_header_for_tests(resolved(None)).unwrap();
    assert!(metadata.ends_with(b",\"next\":null,\"end\":true}"));
    let value: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
    assert_eq!(value["sample_ids"], serde_json::json!(["YQ==", "+/8="]));
    assert_eq!(value["fields"][0]["field"], "空");
    assert_eq!(value["fields"][7]["sample"], "+/8=");
    assert_eq!(value["next"], serde_json::Value::Null);
    assert_eq!(value["end"], true);
    assert_eq!(prefix.len(), 13);
    assert_eq!(
        u32::from_be_bytes(prefix[9..13].try_into().unwrap()) as usize,
        metadata.len()
    );
}

#[test]
fn empty_terminal_header_preserves_exact_wire_bytes() {
    let (prefix, metadata) = DatasetHttpService::batch_header_for_tests(empty(None)).unwrap();
    assert_wire(
        &prefix,
        &metadata,
        "{\"snapshot\":\"11111111111111111111111111111111\",\"sample_ids\":[],\"fields\":[],\"next\":null,\"end\":true}",
    );
}

#[test]
fn metadata_header_accepts_exact_limit_and_rejects_one_more_encoded_byte() {
    // Isolate the encoded-header limit; cursor admission has its own smaller bound.
    let (_, fixed) = DatasetHttpService::batch_header_for_tests(empty(Some(String::new()))).unwrap();
    let remaining = LIMIT - fixed.len();
    let (prefix, metadata) =
        DatasetHttpService::batch_header_for_tests(empty(Some("x".repeat(remaining)))).unwrap();
    assert_eq!(metadata.len(), LIMIT);
    assert_eq!(
        u32::from_be_bytes(prefix[9..13].try_into().unwrap()) as usize,
        LIMIT
    );
    assert_eq!(
        DatasetHttpService::batch_header_for_tests(empty(Some("x".repeat(remaining + 1)))),
        Err("request_too_large")
    );
}

fn assert_wire(prefix: &Bytes, metadata: &Bytes, expected: &str) {
    assert_eq!(metadata.as_ref(), expected.as_bytes());
    let mut expected_prefix = b"CDBDS001\x01".to_vec();
    expected_prefix.extend_from_slice(&u32::try_from(expected.len()).unwrap().to_be_bytes());
    assert_eq!(prefix.as_ref(), expected_prefix);
}

fn resolved(next: Option<&str>) -> ResolvedReadPage {
    let mut value = empty(next.map(str::to_owned));
    value.page.ids = vec![b"a".to_vec(), vec![0xfb, 0xff]];
    value.page.fields = ["空", "erased", "absent", "blob"].map(str::to_owned).to_vec();
    value.samples = vec![
        SampleView {
            sample_id: b"a".to_vec(),
            fields: BTreeMap::from([
                ("空".into(), inline(b"")),
                ("erased".into(), FieldLocator::Tombstone),
                ("blob".into(), inline(b"x")),
            ]),
            values: BTreeMap::new(),
        },
        SampleView {
            sample_id: vec![0xfb, 0xff],
            fields: BTreeMap::from([(
                "blob".into(),
                FieldLocator::Chunk {
                    location: b"unread-location".to_vec(),
                    length: 4_294_967_297,
                    md5: [0xab; 16],
                },
            )]),
            values: BTreeMap::new(),
        },
    ];
    value
}

fn empty(next: Option<String>) -> ResolvedReadPage {
    ResolvedReadPage {
        page: ReadPage {
            snapshot: SnapshotId::from_bytes(&hex::decode(SNAPSHOT).unwrap()).unwrap(),
            ids: Vec::new(),
            fields: Vec::new(),
            next,
        },
        samples: Vec::new(),
    }
}

fn inline(value: &[u8]) -> FieldLocator {
    FieldLocator::Inline {
        value: value.to_vec(),
        md5: Md5::digest(value).into(),
    }
}
