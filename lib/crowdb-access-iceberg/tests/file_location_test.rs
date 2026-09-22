use crowdb_access_iceberg::file::{FileLocation, TableLocation, MAX_OBJECT_KEY_BYTES};
use crowdb_access_iceberg::key::{CatalogId, TableId};

fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::from_bytes(&[0xff; 16]).unwrap(),
        table: TableId::from_bytes(&[0xab; 16]).unwrap(),
    }
}

#[test]
fn canonical_locations_round_trip_without_names_or_key_normalization() {
    let table = table();
    assert_eq!(table.bucket(), "iceberg-77777777777777777777777774");
    assert_eq!(table.to_string().parse::<TableLocation>().unwrap(), table);
    for key in [
        "metadata/v1.json",
        "数据/a+b",
        "a%2Fb",
        "a/b",
        "a//b",
        "a/",
        "%2e%2e",
        "a b",
    ] {
        let file = table.file(key).unwrap();
        assert_eq!(file.relative_key(), key);
        assert_eq!(file.table(), table);
        assert_eq!(file.to_string().parse::<FileLocation>().unwrap(), file);
        assert_eq!(
            FileLocation::from_object_key(&table.bucket(), &file.object_key()).unwrap(),
            file
        );
    }
    assert_ne!(table.file("a%2Fb").unwrap(), table.file("a/b").unwrap());
    assert_ne!(table.file("a//b").unwrap(), table.file("a/b").unwrap());
}

#[test]
fn location_aliases_and_path_escapes_fail_closed() {
    let table = table();
    for relative in [
        "", "/file", "../file", "a/../b", "a/./b", "a/..", "a\\b", "a?b", "a#b", "a\0b", "a\nb",
    ] {
        assert!(table.file(relative).is_err(), "{relative:?}");
    }
    let file = table.file("data/file").unwrap().to_string();
    for invalid in [
        file.replacen("s3:", "s3a:", 1),
        file.replacen("s3:", "S3:", 1),
        file.replacen("iceberg-", "ICEBERG-", 1),
        file.replacen("/t/", "/T/", 1),
        file.replacen("abab", "ABAB", 1),
        file.replacen("7774/", "7775/", 1),
        file.replacen("/t/", ":9000/t/", 1),
        file.replacen("iceberg-", "user@iceberg-", 1),
        format!("{file}?versionId=1"),
    ] {
        assert!(invalid.parse::<FileLocation>().is_err(), "{invalid}");
    }
    assert!(table.to_string().parse::<FileLocation>().is_err());
    assert!(file.parse::<TableLocation>().is_err());
}

#[test]
fn object_key_limit_counts_utf8_bytes_including_table_prefix() {
    let table = table();
    let available = MAX_OBJECT_KEY_BYTES - table.object_prefix().len();
    let key = "x".repeat(available);
    let file = table.file(&key).unwrap();
    assert_eq!(file.object_key().len(), MAX_OBJECT_KEY_BYTES);
    assert_eq!(file.to_string().parse::<FileLocation>().unwrap(), file);
    assert!(table.file(&(key.clone() + "x")).is_err());
    assert!(table.file(&("x".repeat(available - 1) + "冰")).is_err());
    assert!(FileLocation::from_object_key(&table.bucket(), &(file.object_key() + "x")).is_err());
}
