// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#![cfg(feature = "ffi")]

use crowdb_paxos_kv_client::ffi::parse_diskio_exclusions_for_tests as parse;

#[test]
fn exclusion_wire_keeps_every_identity_bit_and_rejects_invalid_batches() {
    let entry = serde_json::json!({ "disk_group_id": "7", "disk_id_high": u64::MAX.to_string(),
        "disk_id_low": u64::MAX.to_string(), "disk_revision": u64::MAX.to_string() });
    let parsed = parse(&serde_json::to_string(&vec![entry.clone()]).unwrap()).unwrap();
    assert_eq!(parsed[0].disk_id.high, u64::MAX);
    assert_eq!(parsed[0].disk_revision, u64::MAX);
    assert!(parse("[]").is_err());
    assert!(parse(&serde_json::to_string(&vec![entry.clone(); 17]).unwrap()).is_err());
    assert!(parse(&serde_json::to_string(&vec![entry.clone(); 2]).unwrap()).is_err());
    let mut invalid = entry;
    invalid["disk_revision"] = "0".into();
    assert!(parse(&serde_json::to_string(&vec![invalid.clone()]).unwrap()).is_err());
    invalid["disk_revision"] = "18446744073709551616".into();
    assert!(parse(&serde_json::to_string(&vec![invalid]).unwrap()).is_err());
}
