use crowdb_access_iceberg::catalog::{CatalogAuthority, CatalogContext, CatalogLifecycle};
use crowdb_access_iceberg::file::{FileGrantError, FileGrantIssuer, FileOperation, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, TableId};
use crowdb_access_iceberg::wire::{BearerAuthenticator, FileDelegationLimits, LoadCredentialsResponse};

fn limits() -> FileDelegationLimits {
    FileDelegationLimits {
        ttl_ms: 900_000,
        max_request_bytes: 100,
        max_file_bytes: 1000,
    }
}

fn context() -> CatalogContext {
    CatalogContext {
        catalog: CatalogId::random(),
        activation_epoch: 1,
    }
}

fn authenticator() -> BearerAuthenticator {
    BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap()
}

fn authority(context: CatalogContext) -> CatalogAuthority {
    let mut authority = CatalogAuthority::new(context.catalog, "catalog".into()).unwrap();
    authority.admission_bounds.delegated_access_ms = 900_000;
    authority
}

#[test]
fn only_independent_writer_receives_file_mutations() {
    let auth = authenticator();
    let issuer = FileGrantIssuer::new([1; 32], 900_000).unwrap();
    let context = context();
    let table = TableId::random();
    let location = TableLocation {
        catalog: context.catalog,
        table,
    }
    .file("data.parquet")
    .unwrap();
    let mut fingerprints = Vec::new();
    for role in ["r", "w", "m", "c"] {
        let principal = auth.authenticate(&format!("Bearer {}", role.repeat(32))).unwrap();
        let credentials = limits()
            .issue(&issuer, principal, context, &authority(context), table, 1000)
            .unwrap();
        let grant = credentials.grant();
        fingerprints.push(grant.principal);
        for operation in [FileOperation::Head, FileOperation::Get] {
            assert_eq!(grant.authorize(operation, &location, 100, 1000), Ok(()));
        }
        for operation in [
            FileOperation::Put,
            FileOperation::CreateMultipart,
            FileOperation::UploadPart,
            FileOperation::ListParts,
            FileOperation::CompleteMultipart,
            FileOperation::AbortMultipart,
        ] {
            assert_eq!(
                grant.authorize(operation, &location, 100, 1000),
                if role == "w" {
                    Ok(())
                } else {
                    Err(FileGrantError::Forbidden)
                }
            );
        }
        assert_eq!(
            grant.authorize(FileOperation::Get, &location, 101, 1000),
            Err(FileGrantError::Bounds)
        );
        assert_eq!(
            grant.authorize(FileOperation::Get, &location, 100, 1001),
            Err(FileGrantError::Bounds)
        );
        let other = TableLocation {
            catalog: context.catalog,
            table: TableId::random(),
        }
        .file("data.parquet")
        .unwrap();
        assert_eq!(
            grant.authorize(FileOperation::Get, &other, 0, 0),
            Err(FileGrantError::Forbidden)
        );
    }
    fingerprints.sort_unstable();
    fingerprints.dedup();
    assert_eq!(fingerprints.len(), 4);
}

#[test]
fn refresh_rotates_credentials_and_serializes_official_sdk_properties() {
    let principal = authenticator()
        .authenticate(&format!("Bearer {}", "w".repeat(32)))
        .unwrap();
    let issuer = FileGrantIssuer::new([1; 32], 900_000).unwrap();
    let context = context();
    let table = TableId::random();
    let initial = limits()
        .issue(&issuer, principal, context, &authority(context), table, 1000)
        .unwrap();
    let refreshed = limits()
        .issue(&issuer, principal, context, &authority(context), table, 1000)
        .unwrap();
    assert_ne!(initial.access_key_id(), refreshed.access_key_id());
    assert_ne!(initial.session_token(), refreshed.session_token());
    assert_eq!(initial.grant().principal, refreshed.grant().principal);
    let expected_key = refreshed.access_key_id().to_owned();
    let expected_secret = refreshed.secret_access_key().to_owned();
    let response = serde_json::to_value(LoadCredentialsResponse::from(refreshed)).unwrap();
    assert_eq!(response.as_object().unwrap().len(), 1);
    let credentials = response["storage-credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 1);
    assert_eq!(
        credentials[0]["prefix"],
        TableLocation {
            catalog: context.catalog,
            table
        }
        .to_string()
    );
    let config = &credentials[0]["config"];
    assert_eq!(config["s3.access-key-id"], expected_key);
    assert_eq!(config["s3.secret-access-key"], expected_secret);
    assert_eq!(config["s3.session-token-expires-at-ms"], "901000");
    let token = config["s3.session-token"].as_str().unwrap();
    assert!(issuer.verify(&expected_key, token, context, 900_999).is_ok());
    assert!(matches!(
        issuer.verify(&expected_key, token, context, 901_000),
        Err(FileGrantError::Expired)
    ));
}

#[test]
fn delegation_rejects_invalid_limits_and_unrepresentable_sdk_expiry() {
    let principal = authenticator()
        .authenticate(&format!("Bearer {}", "r".repeat(32)))
        .unwrap();
    let issuer = FileGrantIssuer::new([1; 32], 900_000).unwrap();
    let context = context();
    let mut authority = authority(context);
    authority.admission_bounds.delegated_access_ms = 1_000_000;
    for invalid in 0..7 {
        let mut limits = limits();
        let mut now_ms = 1000;
        match invalid {
            0 => limits.ttl_ms = 0,
            1 => limits.ttl_ms += 1,
            2 => limits.max_request_bytes = 0,
            3 => limits.max_file_bytes = 0,
            4 => limits.max_request_bytes = limits.max_file_bytes + 1,
            5 => now_ms = u64::MAX,
            _ => now_ms = i64::MAX as u64,
        }
        assert!(matches!(
            limits.issue(&issuer, principal, context, &authority, TableId::random(), now_ms),
            Err(FileGrantError::Invalid)
        ));
    }
}

#[test]
fn persisted_delegation_bound_is_independent_of_issuer_configuration() {
    let principal = authenticator()
        .authenticate(&format!("Bearer {}", "w".repeat(32)))
        .unwrap();
    let issuer = FileGrantIssuer::new([1; 32], 1_000_000).unwrap();
    let context = context();
    let table = TableId::random();
    for bound in [0, 1, 899_999, 900_000, 900_001] {
        let mut authority = authority(context);
        authority.admission_bounds.delegated_access_ms = bound;
        let result = limits().issue(&issuer, principal, context, &authority, table, 1000);
        if bound < 900_000 {
            assert!(matches!(result, Err(FileGrantError::Bounds)));
        } else {
            assert_eq!(result.unwrap().grant().expires_ms, 901_000);
        }
    }
    let mut retired = authority(context);
    retired.lifecycle = CatalogLifecycle::Retired;
    assert!(matches!(
        limits().issue(&issuer, principal, context, &retired, table, 1000),
        Err(FileGrantError::Forbidden)
    ));
    let mut foreign = authority(context);
    foreign.catalog = CatalogId::random();
    assert!(matches!(
        limits().issue(&issuer, principal, context, &foreign, table, 1000),
        Err(FileGrantError::Forbidden)
    ));
    let mut invalid = authority(context);
    invalid.admission_bounds.request_ms = 0;
    assert!(matches!(
        limits().issue(&issuer, principal, context, &invalid, table, 1000),
        Err(FileGrantError::Invalid)
    ));
}
