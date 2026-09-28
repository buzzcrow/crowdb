// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_server::s3::CompleteSelection;

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
