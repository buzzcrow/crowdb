// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::range::{resolve_range, ByteRange, RangeError};

#[test]
fn resolves_full_and_each_single_range_shape() {
    assert_eq!(resolve_range(None, 10), Ok(None));
    assert_eq!(
        resolve_range(Some("bytes=0-3"), 10),
        Ok(Some(ByteRange { start: 0, end: 4 }))
    );
    assert_eq!(
        resolve_range(Some("bytes=4-"), 10),
        Ok(Some(ByteRange { start: 4, end: 10 }))
    );
    assert_eq!(
        resolve_range(Some("bytes=-3"), 10),
        Ok(Some(ByteRange { start: 7, end: 10 }))
    );
    assert_eq!(
        resolve_range(Some("bytes=7-99"), 10),
        Ok(Some(ByteRange { start: 7, end: 10 }))
    );
}

#[test]
fn rejects_multi_malformed_and_unsatisfiable_ranges() {
    assert_eq!(resolve_range(Some("items=0-1"), 10), Err(RangeError::Invalid));
    assert_eq!(
        resolve_range(Some("bytes=0-1,3-4"), 10),
        Err(RangeError::Multiple)
    );
    assert_eq!(
        resolve_range(Some("bytes=10-"), 10),
        Err(RangeError::Unsatisfiable)
    );
    assert_eq!(
        resolve_range(Some("bytes=-0"), 10),
        Err(RangeError::Unsatisfiable)
    );
}
