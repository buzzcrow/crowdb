// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::KvRpcTransport;
use crowdb_protocol::kv_client_fb::{
    FBKvClientRetCode, FBKvCompareObservation, FBKvCompareObservationArgs, FBKvResponse, FBKvResponseArgs,
};
use flatbuffers::FlatBufferBuilder;

fn reply(code: FBKvClientRetCode, revision: u64, observed: Option<(u64, &[u8])>) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let comparison = observed.map(|(revision, value)| {
        let key = builder.create_vector(b"key\0\xff");
        let value = builder.create_vector(value);
        FBKvCompareObservation::create(
            &mut builder,
            &FBKvCompareObservationArgs {
                key: Some(key),
                revision,
                value: Some(value),
            },
        )
    });
    let response = FBKvResponse::create(
        &mut builder,
        &FBKvResponseArgs {
            id: 42,
            ret_code: code,
            revision,
            compare_observation: comparison,
            ..Default::default()
        },
    );
    builder.finish(response, None);
    builder.finished_data().to_vec()
}

#[test]
fn legacy_failure_is_not_misread_as_an_observed_absent_key() {
    let legacy =
        KvRpcTransport::parse_response_for_tests(&reply(FBKvClientRetCode::CasFailed, 8, None)).unwrap();
    assert_eq!(legacy.revision, 8);
    assert!(legacy.compare_observation.is_none());
    let absent =
        KvRpcTransport::parse_response_for_tests(&reply(FBKvClientRetCode::CasFailed, 0, Some((0, b""))))
            .unwrap()
            .compare_observation
            .unwrap();
    assert_eq!(absent.revision, 0);
    assert_eq!(absent.key.as_ref(), b"key\0\xff");
    assert!(absent.value.is_empty());
    let empty =
        KvRpcTransport::parse_response_for_tests(&reply(FBKvClientRetCode::CasFailed, 9, Some((9, b""))))
            .unwrap()
            .compare_observation
            .unwrap();
    assert_eq!(empty.revision, 9);
    assert!(empty.value.is_empty());
}

#[test]
fn invalid_observations_never_become_definite_comparison_results() {
    for (code, revision, observed) in [
        (FBKvClientRetCode::CasFailed, 7, (8, b"value".as_slice())),
        (FBKvClientRetCode::CasFailed, 0, (0, b"value".as_slice())),
        (FBKvClientRetCode::CasBusy, 8, (8, b"value".as_slice())),
        (FBKvClientRetCode::OutcomeUnknown, 8, (8, b"value".as_slice())),
        (FBKvClientRetCode::Success, 8, (8, b"value".as_slice())),
    ] {
        assert!(KvRpcTransport::parse_response_for_tests(&reply(code, revision, Some(observed))).is_err());
    }
    let mut builder = FlatBufferBuilder::new();
    let comparison = FBKvCompareObservation::create(&mut builder, &FBKvCompareObservationArgs::default());
    let response = FBKvResponse::create(
        &mut builder,
        &FBKvResponseArgs {
            ret_code: FBKvClientRetCode::CasFailed,
            compare_observation: Some(comparison),
            ..Default::default()
        },
    );
    builder.finish(response, None);
    assert!(KvRpcTransport::parse_response_for_tests(builder.finished_data()).is_err());
}
