use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::key::{
    CatalogId, CatalogScope, IcebergKey, NameSuffix, NamespaceId, OperationId, SystemScope, MAX_KEY_BYTES,
};

#[test]
fn keys_round_trip_and_catalog_ranges_exclude_system_records() {
    for bytes in [[1; 16], [0xff; 16]] {
        let catalog = CatalogId::from_bytes(&bytes).unwrap();
        let range = IcebergKey::catalog_range(catalog);
        for scope in [
            CatalogScope::Authority,
            CatalogScope::NamespaceAuthority,
            CatalogScope::TableHead,
            CatalogScope::File,
            CatalogScope::Operation,
        ] {
            let suffix = if scope == CatalogScope::Authority {
                Vec::new()
            } else {
                vec![9; 16]
            };
            let key = IcebergKey::Catalog {
                catalog,
                scope,
                suffix,
            };
            let encoded = key.encode().unwrap();
            assert!(range.contains(&encoded));
            assert_eq!(IcebergKey::decode(&encoded).unwrap(), key);
        }
        for scope in [
            SystemScope::ActiveRoot,
            SystemScope::ManagementOperation,
            SystemScope::Audit,
            SystemScope::RetryBinding,
        ] {
            let suffix = if scope == SystemScope::ActiveRoot {
                Vec::new()
            } else {
                vec![2; 16]
            };
            let key = IcebergKey::System { scope, suffix };
            let encoded = key.encode().unwrap();
            assert!(!range.contains(&encoded));
            assert_eq!(IcebergKey::decode(&encoded).unwrap(), key);
        }
    }
}

#[test]
fn adjacent_catalogs_do_not_overlap() {
    let mut first = [0; 16];
    first[15] = 255;
    let mut second = [0; 16];
    second[14] = 1;
    let first = IcebergKey::catalog_range(CatalogId::from_bytes(&first).unwrap());
    let second = IcebergKey::catalog_range(CatalogId::from_bytes(&second).unwrap());
    assert_eq!(first.end, second.start);
}

#[test]
fn unknown_versions_scopes_zero_ids_and_truncation_fail_closed() {
    let key = IcebergKey::Catalog {
        catalog: CatalogId::random(),
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    }
    .encode()
    .unwrap();
    for length in 0..key.len() {
        assert!(IcebergKey::decode(&key[..length]).is_err());
    }
    let mut invalid = key.clone();
    invalid[4] = 2;
    assert_eq!(IcebergKey::decode(&invalid), Err(ValidationError::KeyVersion(2)));
    invalid = key.clone();
    invalid[22] = 255;
    assert!(IcebergKey::decode(&invalid).is_err());
    invalid = key;
    invalid[6..22].fill(0);
    assert_eq!(IcebergKey::decode(&invalid), Err(ValidationError::Identity));
    assert!(OperationId::from_bytes(&[0; 16]).is_err());
    assert!(CatalogId::from_bytes(&[1; 15]).is_err());
}

#[test]
fn key_byte_limit_is_enforced_before_encoding() {
    let catalog = CatalogId::random();
    let mut key = IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::NamespaceName,
        suffix: NameSuffix {
            parent: None,
            name: &"x".repeat(MAX_KEY_BYTES - 41),
        }
        .encode()
        .unwrap(),
    };
    let bytes = key.encode().unwrap();
    assert_eq!(bytes.len(), MAX_KEY_BYTES);
    assert_eq!(IcebergKey::decode(&bytes).unwrap(), key);
    if let IcebergKey::Catalog { suffix, .. } = &mut key {
        suffix.push(0);
    }
    assert_eq!(key.encode(), Err(ValidationError::KeyTooLarge));
    assert_eq!(
        IcebergKey::decode(&vec![0; MAX_KEY_BYTES + 1]),
        Err(ValidationError::KeyTooLarge)
    );
}

#[test]
fn name_fields_preserve_utf8_delimiters_and_parent_identity() {
    let parent = NamespaceId::random();
    for name in ["a/b", "a%1Fb", "数据.表", "a:1"] {
        let suffix = NameSuffix {
            parent: Some(parent),
            name,
        };
        let bytes = suffix.encode().unwrap();
        assert_eq!(NameSuffix::decode(&bytes).unwrap(), suffix);
        let key = IcebergKey::Catalog {
            catalog: CatalogId::random(),
            scope: CatalogScope::TableName,
            suffix: bytes,
        };
        assert_eq!(IcebergKey::decode(&key.encode().unwrap()).unwrap(), key);
    }
    for name in ["", "a\0b", "a\u{1f}b"] {
        assert!(NameSuffix { parent: None, name }.encode().is_err());
    }
    let mut bytes = NameSuffix {
        parent: None,
        name: "name",
    }
    .encode()
    .unwrap();
    bytes.push(0);
    assert!(NameSuffix::decode(&bytes).is_err());
    assert!(IcebergKey::Catalog {
        catalog: CatalogId::random(),
        scope: CatalogScope::TableName,
        suffix: NameSuffix {
            parent: None,
            name: "table"
        }
        .encode()
        .unwrap()
    }
    .encode()
    .is_err());
}

#[test]
fn root_has_no_suffix_and_authority_ids_are_fixed_width() {
    assert!(IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: vec![1]
    }
    .encode()
    .is_err());
    assert!(IcebergKey::System {
        scope: SystemScope::ManagementOperation,
        suffix: vec![1; 17]
    }
    .encode()
    .is_err());
    assert!(IcebergKey::Catalog {
        catalog: CatalogId::random(),
        scope: CatalogScope::TableHead,
        suffix: vec![1; 15]
    }
    .encode()
    .is_err());
}
