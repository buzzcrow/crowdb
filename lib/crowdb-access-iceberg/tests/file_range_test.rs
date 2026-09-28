use crowdb_access_iceberg::file::{resolve_range, ByteRange, RangeError};

#[test]
fn ranges_resolve_exact_half_open_intervals_without_overflow() {
    assert_eq!(resolve_range(None, 0), Ok(None));
    for (header, start, end) in [
        ("bytes=0-0", 0, 1),
        ("bytes=3-", 3, 10),
        ("bytes=-3", 7, 10),
        ("bytes=-20", 0, 10),
        ("bytes=5-999", 5, 10),
        ("bytes=0-18446744073709551615", 0, 10),
    ] {
        assert_eq!(
            resolve_range(Some(header), 10),
            Ok(Some(ByteRange { start, end }))
        );
    }
    assert_eq!(
        resolve_range(Some("bytes=18446744073709551614-"), u64::MAX),
        Ok(Some(ByteRange {
            start: u64::MAX - 1,
            end: u64::MAX
        }))
    );
}

#[test]
fn malformed_multiple_and_unsatisfiable_ranges_are_distinct() {
    for value in [
        "",
        "Bytes=0-1",
        "bytes=+1-2",
        "bytes=1-+2",
        "bytes= 1-2",
        "bytes=-",
        "bytes=0-1-2",
        "bytes=0-18446744073709551616",
    ] {
        assert_eq!(resolve_range(Some(value), 10), Err(RangeError::Invalid));
    }
    assert_eq!(
        resolve_range(Some("bytes=0-1,4-5"), 10),
        Err(RangeError::Multiple)
    );
    for value in ["bytes=10-", "bytes=5-4", "bytes=-0"] {
        assert_eq!(resolve_range(Some(value), 10), Err(RangeError::Unsatisfiable));
    }
    assert_eq!(
        resolve_range(Some("bytes=0-0"), 0),
        Err(RangeError::Unsatisfiable)
    );
}
