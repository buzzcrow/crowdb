// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::BytesMut;
use crowdb_sdk_dataset::{
    wire::{BatchDecoder, BatchEvent},
    DatasetInfo, Publication, Snapshot,
};

const BATCH: &[u8] = include_bytes!("../../../contract-fixtures/dataset-v1/batch.bin");

#[test]
fn frozen_control_json_round_trips() {
    let publication: Publication = serde_json::from_str(include_str!(
        "../../../contract-fixtures/dataset-v1/publication.json"
    ))
    .unwrap();
    assert_eq!(publication.operation, "01".repeat(16));
    let dataset: DatasetInfo =
        serde_json::from_str(include_str!("../../../contract-fixtures/dataset-v1/dataset.json")).unwrap();
    assert_eq!(dataset.name, "training/a b");
    let snapshot: Snapshot = serde_json::from_str(include_str!(
        "../../../contract-fixtures/dataset-v1/snapshot.json"
    ))
    .unwrap();
    assert!(!snapshot.is_latest);
    let mut invalid_state = serde_json::to_value(&snapshot).unwrap();
    invalid_state["state"] = "unknown".into();
    assert!(serde_json::from_value::<Snapshot>(invalid_state).is_err());
    let mut value = serde_json::to_value(publication).unwrap();
    value["future_optional"] = true.into();
    serde_json::from_value::<Publication>(value.clone()).unwrap();
    value.as_object_mut().unwrap().remove("manifest");
    assert!(serde_json::from_value::<Publication>(value).is_err());
}

fn decode(bytes: &[u8], chunk: usize) -> Result<Vec<u8>, crowdb_sdk_dataset::Error> {
    let mut decoder = BatchDecoder::new();
    let mut buffer = BytesMut::new();
    let mut output = Vec::new();
    for data in bytes.chunks(chunk) {
        buffer.extend_from_slice(data);
        while let Some(event) = decoder.decode(&mut buffer)? {
            if let BatchEvent::Data(data) = event {
                output.extend_from_slice(&data);
            }
        }
    }
    decoder.finish()?;
    assert!(buffer.is_empty());
    Ok(output)
}

#[test]
fn all_transport_split_positions_preserve_frame_and_field_integrity() {
    for chunk in 1..=BATCH.len() {
        assert_eq!(decode(BATCH, chunk).unwrap(), b"hello");
    }
}

#[test]
fn truncated_corrupt_version_and_trailing_bytes_cannot_succeed() {
    for bad in [
        include_bytes!("../../../contract-fixtures/dataset-v1/batch-truncated.bin").as_slice(),
        include_bytes!("../../../contract-fixtures/dataset-v1/batch-corrupt.bin").as_slice(),
        include_bytes!("../../../contract-fixtures/dataset-v1/batch-version.bin").as_slice(),
    ] {
        assert!(decode(bad, bad.len()).is_err());
    }
    let mut trailing = BATCH.to_vec();
    trailing.push(0);
    assert!(decode(&trailing, 1).is_err());
    for length in 0..BATCH.len() {
        let mut decoder = BatchDecoder::new();
        let mut buffer = BytesMut::from(&BATCH[..length]);
        while let Ok(Some(_)) = decoder.decode(&mut buffer) {}
        assert!(decoder.finish().is_err());
    }
}

#[test]
fn terminal_stream_error_is_distinct_from_corrupt_data() {
    let bytes = include_bytes!("../../../contract-fixtures/dataset-v1/batch-error.bin");
    let error = decode(bytes, 1).unwrap_err();
    let crowdb_sdk_dataset::Error::Stream { error } = error else {
        panic!("expected stream error")
    };
    assert_eq!(error.code, "cancelled");
    assert!(!error.retryable);
}
