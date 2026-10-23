// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::TestDirectory as Test;
use crowdb_protocol::chunk_stream::{
    decode_stream_directory_record, encode_stream_directory_page, stream_directory_page_id,
    StreamDirectoryPageId, StreamPurpose,
};

#[test]
fn enumerated_record_derives_only_the_current_checked_fence() {
    let page = Test::leaf(10, 0, 20);
    let (reference, bytes) = encode_stream_directory_page(&page).unwrap();
    assert_eq!(
        decode_stream_directory_record(&bytes, reference.id, Test::NAME, StreamPurpose::Stream).unwrap(),
        (reference, page)
    );
    assert!(decode_stream_directory_record(
        &bytes,
        StreamDirectoryPageId([0; 32]),
        Test::NAME,
        StreamPurpose::Stream
    )
    .is_err());
    for offset in [4, 6, 7, 8] {
        let mut bad = bytes.clone();
        bad[offset] ^= 1;
        let id = stream_directory_page_id(&bad).unwrap();
        assert!(decode_stream_directory_record(&bad, id, Test::NAME, StreamPurpose::Stream).is_err());
    }
    let mut wrong = Test::NAME;
    wrong.low += 1;
    assert!(decode_stream_directory_record(&bytes, reference.id, wrong, StreamPurpose::Stream).is_err());
    let mut trailing = bytes;
    trailing.push(0);
    let id = stream_directory_page_id(&trailing).unwrap();
    assert!(decode_stream_directory_record(&trailing, id, Test::NAME, StreamPurpose::Stream).is_err());
}
