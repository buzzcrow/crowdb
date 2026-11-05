// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_test_harness::test_dirs::TestDir;

#[test]
fn persisted_identity_rejects_conflicting_configuration_and_corruption() {
    use crowdb_paxos_kv_server::background::identity::load_or_create;

    let root = TestDir::new("service-identity").unwrap();
    let first = load_or_create(root.path(), Some(42), Some(2)).unwrap();
    assert_eq!(first.instance_id, 42);
    assert_eq!(load_or_create(root.path(), None, Some(2)).unwrap(), first);
    assert!(load_or_create(root.path(), Some(43), Some(2)).is_err());
    assert!(load_or_create(root.path(), None, Some(3)).is_err());
    let path = root.path().join("service-identity.json");
    std::fs::write(&path, b"partial").unwrap();
    assert!(load_or_create(root.path(), None, Some(2)).is_err());
    assert_eq!(std::fs::read(path).unwrap(), b"partial");
}
