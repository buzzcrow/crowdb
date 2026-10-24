// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::operation::RequestIdentity;

#[test]
fn uuidv7_wire_keys_validate_version_and_clock() {
    let valid = "00000000-0064-7000-8000-000000000001";
    assert_eq!(RequestIdentity::parse(valid, 100).unwrap().issued_ms, 100);
    assert!(RequestIdentity::parse("00000000-0064-4000-8000-000000000001", 100).is_err());
    assert!(RequestIdentity::parse("00000001-0064-7000-8000-000000000001", 100).is_err());
    assert!(RequestIdentity::parse("not-a-uuid", 100).is_err());
}
