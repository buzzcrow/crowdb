// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_server::s3::{CompleteRequestError, CompleteSelection};

#[test]
fn s3_and_iceberg_use_the_same_bounded_completion_parser() {
    let digest = "ab".repeat(16);
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{digest}\"</ETag></Part></CompleteMultipartUpload>"
    );
    let s3 = CompleteSelection::parse(body.as_bytes()).unwrap();
    let iceberg = crowdb_access_server::iceberg::CompleteSelection::parse(body.as_bytes()).unwrap();
    assert_eq!(s3.parts(), iceberg.parts());
    assert_eq!(s3.parts()[0].etag, digest);
}

#[test]
fn completion_parser_distinguishes_part_order_from_malformed_requests() {
    let digest = "ab".repeat(16);
    let part = |number| format!("<Part><PartNumber>{number}</PartNumber><ETag>\"{digest}\"</ETag></Part>");
    for numbers in [[2, 1], [1, 1]] {
        let body = format!(
            "<CompleteMultipartUpload>{}{}</CompleteMultipartUpload>",
            part(numbers[0]),
            part(numbers[1])
        );
        assert_eq!(
            CompleteSelection::parse(body.as_bytes()),
            Err(CompleteRequestError::InvalidPartOrder)
        );
    }
    assert_eq!(
        CompleteSelection::parse(b"<CompleteMultipartUpload/>"),
        Err(CompleteRequestError::InvalidRequest)
    );
}
