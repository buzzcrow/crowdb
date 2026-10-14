use crowdb_access_iceberg::{
    file::{NativeFileLocation, MAX_OBJECT_KEY_BYTES},
    key::{
        CatalogId, CatalogScope, IcebergKey, NamespaceId, StorageKey, StorageTablePath, TableId,
        TableStorageKey, TableStorageScope,
    },
};

fn table() -> StorageTablePath {
    StorageTablePath {
        catalog: CatalogId::from_bytes(&[0xab; 16]).unwrap(),
        storage_namespace: NamespaceId::from_bytes(&[0xcd; 16]).unwrap(),
        table: TableId::from_bytes(&[0xef; 16]).unwrap(),
    }
}

fn key(table: StorageTablePath, scope: TableStorageScope, suffix: &str) -> TableStorageKey {
    TableStorageKey {
        table,
        scope,
        suffix: suffix.to_owned(),
    }
}

#[test]
fn native_uri_preserves_immutable_identity_and_exact_filename() {
    let table = table();
    assert_eq!(table.to_string().len(), StorageTablePath::PREFIX_BYTES);
    assert_eq!(table.to_string().parse::<StorageTablePath>().unwrap(), table);
    assert_eq!(table.uri(), format!("s3://iceberg/{table}"));
    for filename in ["metadata/v1.json", "数据/a+b", "a%2Fb", "a//b", "a/", "a b"] {
        let location = table.logical_file(filename).unwrap();
        assert_eq!(location.table(), table);
        assert_eq!(location.relative_key(), filename);
        assert_eq!(
            location.to_string().parse::<NativeFileLocation>().unwrap(),
            location
        );
        assert_eq!(
            NativeFileLocation::from_object_key("iceberg", &location.object_key()).unwrap(),
            location
        );
    }
    assert_ne!(table.file("a%2Fb").unwrap(), table.file("a/b").unwrap());
    assert!(table.logical_file("f/v1/name.token.parquet").is_err());
    assert!(table.file("f/v1/name.token.parquet").is_ok());
}

#[test]
fn native_uri_rejects_aliases_incomplete_scopes_and_escaping_names() {
    let table = table();
    let uri = table.file("metadata/v1.json").unwrap().to_string();
    for invalid in [
        uri.replacen("abab", "ABAB", 1),
        uri.replacen("abababab", "abab-abab", 1),
        uri.replacen("iceberg/", "iceberg:9000/", 1),
        uri.replacen("iceberg/", "user@iceberg/", 1),
        uri.replacen("iceberg/", "iceberg-old/", 1),
        uri.replacen("s3:", "s3a:", 1),
        uri.replacen(&table.catalog.to_string(), &"0".repeat(32), 1),
    ] {
        assert!(invalid.parse::<NativeFileLocation>().is_err(), "{invalid}");
    }
    for filename in ["", "/a", "../a", "a/../b", "a/./b", "a\\b", "a?b", "a#b", "a\0b"] {
        assert!(table.file(filename).is_err(), "{filename:?}");
    }
    for prefix in [
        format!("{}/", table.catalog),
        format!("{}/{}/", table.catalog, table.storage_namespace),
        table.to_string().trim_end_matches('/').to_owned(),
        format!("{table}metadata/"),
    ] {
        assert!(prefix.parse::<StorageTablePath>().is_err(), "{prefix}");
    }
    assert!(table.uri().parse::<NativeFileLocation>().is_err());
}

#[test]
fn filename_limit_counts_three_ids_and_utf8_bytes() {
    let table = table();
    let available = MAX_OBJECT_KEY_BYTES - StorageTablePath::PREFIX_BYTES;
    let filename = "x".repeat(available);
    let location = table.file(&filename).unwrap();
    assert_eq!(location.object_key().len(), MAX_OBJECT_KEY_BYTES);
    assert_eq!(
        location.to_string().parse::<NativeFileLocation>().unwrap(),
        location
    );
    assert!(table.file(&(filename + "x")).is_err());
    assert!(table.file(&("x".repeat(available - 1) + "冰")).is_err());
}

#[test]
fn table_keys_are_version_isolated_and_round_trip() {
    let table = table();
    let legacy = IcebergKey::Catalog {
        catalog: table.catalog,
        scope: CatalogScope::File,
        suffix: vec![1; 16],
    }
    .encode()
    .unwrap();
    assert!(TableStorageKey::decode(&legacy).is_err());
    assert!(matches!(
        StorageKey::decode(&legacy).unwrap(),
        StorageKey::Legacy(_)
    ));
    for (scope, suffix, component) in [
        (TableStorageScope::Head, "", "head"),
        (TableStorageScope::File, "metadata/v1.json", "file/"),
        (TableStorageScope::Projection, "1/receipt", "projection/"),
        (
            TableStorageScope::Operation,
            "01010101010101010101010101010101/state",
            "operation/",
        ),
        (
            TableStorageScope::Multipart,
            "01010101010101010101010101010101/part/1",
            "multipart/",
        ),
        (
            TableStorageScope::Gc,
            "01010101010101010101010101010101/state",
            "gc/",
        ),
    ] {
        let key = key(table, scope, suffix);
        let bytes = key.encode().unwrap();
        assert_eq!(TableStorageKey::decode(&bytes).unwrap(), key);
        assert_eq!(
            StorageKey::decode(&bytes).unwrap(),
            StorageKey::NativeTable(key.clone())
        );
        assert!(IcebergKey::decode(&bytes).is_err());
        assert!(TableStorageKey::table_range(table).contains(&bytes));
        assert_eq!(&bytes[5..], format!("{table}{component}{suffix}").as_bytes());
    }
}

#[test]
fn scan_validation_never_combines_storage_versions() {
    let table = table();
    let legacy = IcebergKey::catalog_range(table.catalog);
    let native = TableStorageKey::table_range(table);
    assert!(StorageKey::validate_range(&legacy).is_ok());
    assert!(StorageKey::validate_range(&native).is_ok());
    assert!(StorageKey::validate_range(&TableStorageKey::file_range(table, "metadata/").unwrap()).is_ok());
    assert!(StorageKey::validate_range(&(legacy.start..native.end.clone())).is_err());
    assert!(StorageKey::validate_range(&(native.end.clone()..native.start.clone())).is_err());
    assert!(StorageKey::validate_range(&(native.start.clone()..native.start)).is_err());
    assert!(StorageKey::decode(b"ICE\0\x03").is_err());
    assert!(StorageKey::decode(b"ICE\0\x02not-a-path").is_err());
}

#[test]
fn prefix_scans_exclude_other_kinds_namespaces_and_tables() {
    let table = table();
    let prefix = TableStorageKey::file_range(table, "metadata/v1").unwrap();
    for filename in [
        "metadata/v1.json",
        "metadata/v10.json",
        "metadata/v1/manifest.avro",
    ] {
        assert!(prefix.contains(&key(table, TableStorageScope::File, filename).encode().unwrap()));
    }
    for filename in ["data/v1.parquet", "metadata/v2.json", "metadata/v0.json"] {
        assert!(!prefix.contains(&key(table, TableStorageScope::File, filename).encode().unwrap()));
    }
    for foreign in [
        StorageTablePath {
            table: TableId::random(),
            ..table
        },
        StorageTablePath {
            storage_namespace: NamespaceId::random(),
            ..table
        },
        StorageTablePath {
            catalog: CatalogId::random(),
            ..table
        },
    ] {
        let bytes = key(foreign, TableStorageScope::File, "metadata/v1.json")
            .encode()
            .unwrap();
        assert!(!prefix.contains(&bytes));
        assert!(!TableStorageKey::table_range(table).contains(&bytes));
    }
    let all_files = TableStorageKey::file_range(table, "").unwrap();
    assert!(!all_files.contains(&key(table, TableStorageScope::Head, "").encode().unwrap()));
    assert!(!all_files.contains(
        &key(table, TableStorageScope::Projection, "1/receipt")
            .encode()
            .unwrap()
    ));
    assert!(TableStorageKey::file_range(table, "../").is_err());
}

#[test]
fn malformed_keys_cannot_alias_head_or_filename() {
    let table = table();
    for (scope, suffix) in [
        (TableStorageScope::Head, "file"),
        (TableStorageScope::File, ""),
        (TableStorageScope::File, "../file"),
        (TableStorageScope::Operation, "../operation"),
        (TableStorageScope::Operation, "upload/state"),
        (TableStorageScope::Gc, "00000000000000000000000000000000/state"),
        (
            TableStorageScope::Multipart,
            "ABABABABABABABABABABABABABABABAB/part",
        ),
        (TableStorageScope::Projection, "0/receipt"),
        (TableStorageScope::Projection, "01/receipt"),
        (TableStorageScope::Projection, "1/"),
    ] {
        assert!(key(table, scope, suffix).encode().is_err());
    }
    for suffix in ["head/", "head/file", "file/", "unknown/x", "file/../x"] {
        let mut bytes = b"ICE\0\x02".to_vec();
        bytes.extend_from_slice(format!("{table}{suffix}").as_bytes());
        assert!(TableStorageKey::decode(&bytes).is_err(), "{suffix}");
    }
}
