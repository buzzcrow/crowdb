use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    FileGrant, FileGrantError, FileGrantIssuer, FileOperation, FileOperations, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, OperationId, TableId};

fn grant() -> FileGrant {
    FileGrant {
        context: CatalogContext {
            catalog: CatalogId::random(),
            activation_epoch: 1,
        },
        table: TableId::random(),
        principal: [3; 32],
        nonce: OperationId::random(),
        issued_ms: 1000,
        expires_ms: 2000,
        operations: FileOperations::new(&[FileOperation::Head, FileOperation::Get]).unwrap(),
        max_request_bytes: 100,
        max_file_bytes: 1000,
    }
}

#[test]
fn delegated_credentials_verify_across_servers_without_storing_secret_material() {
    let primary = FileGrantIssuer::new([1; 32], 1000).unwrap();
    let secondary = FileGrantIssuer::new([1; 32], 1000).unwrap();
    let grant = grant();
    let issued = primary.issue(grant.clone()).unwrap();
    let verified = secondary
        .verify(
            issued.access_key_id(),
            issued.session_token(),
            grant.context,
            1000,
        )
        .unwrap();
    assert_eq!(verified.grant(), &grant);
    assert_eq!(verified.secret_access_key(), issued.secret_access_key());
    assert_eq!(issued.access_key_id().len(), 20);
    assert_eq!(issued.session_token().len(), 207);
    assert_eq!(issued.secret_access_key().len(), 43);
    assert!(!issued.session_token().contains(issued.secret_access_key()));
    let mut another = grant;
    another.nonce = OperationId::random();
    let another = primary.issue(another).unwrap();
    assert_ne!(issued.secret_access_key(), another.secret_access_key());
    assert_ne!(issued.access_key_id(), another.access_key_id());
}

#[test]
fn grants_bind_exact_table_operations_and_independent_byte_limits() {
    let grant = grant();
    let location = TableLocation {
        catalog: grant.context.catalog,
        table: grant.table,
    }
    .file("data/a%2Fb")
    .unwrap();
    assert_eq!(grant.authorize(FileOperation::Get, &location, 100, 1000), Ok(()));
    assert_eq!(
        grant.authorize(FileOperation::Put, &location, 0, 1),
        Err(FileGrantError::Forbidden)
    );
    assert_eq!(
        grant.authorize(FileOperation::Get, &location, 101, 1),
        Err(FileGrantError::Bounds)
    );
    assert_eq!(
        grant.authorize(FileOperation::Get, &location, 1, 1001),
        Err(FileGrantError::Bounds)
    );
    for table in [
        TableLocation {
            catalog: CatalogId::random(),
            table: grant.table,
        },
        TableLocation {
            catalog: grant.context.catalog,
            table: TableId::random(),
        },
    ] {
        assert_eq!(
            grant.authorize(FileOperation::Get, &table.file("data/a%2Fb").unwrap(), 0, 0),
            Err(FileGrantError::Forbidden)
        );
    }
    assert!(FileOperations::new(&[]).is_err());
    assert!(FileOperations::from_bits(1 << 8).is_err());
    assert!(FileOperations::from_bits(u16::MAX).is_err());
    let all = FileOperations::from_bits(255).unwrap();
    for operation in [
        FileOperation::Head,
        FileOperation::Get,
        FileOperation::Put,
        FileOperation::CreateMultipart,
        FileOperation::UploadPart,
        FileOperation::ListParts,
        FileOperation::CompleteMultipart,
        FileOperation::AbortMultipart,
    ] {
        assert!(all.allows(operation));
    }
}

#[test]
fn altered_tokens_wrong_issuers_and_retired_contexts_fail_closed() {
    let grant = grant();
    let issuer = FileGrantIssuer::new([1; 32], 1000).unwrap();
    let credential = issuer.issue(grant.clone()).unwrap();
    let bad = FileGrantIssuer::new([2; 32], 1000).unwrap();
    assert!(bad
        .verify(
            credential.access_key_id(),
            credential.session_token(),
            grant.context,
            1500
        )
        .is_err());
    for now in [999, 2000, u64::MAX] {
        assert!(matches!(
            issuer.verify(
                credential.access_key_id(),
                credential.session_token(),
                grant.context,
                now
            ),
            Err(FileGrantError::Expired)
        ));
    }
    assert!(issuer
        .verify(
            credential.access_key_id(),
            credential.session_token(),
            grant.context,
            1999
        )
        .is_ok());
    for context in [
        CatalogContext {
            activation_epoch: 2,
            ..grant.context
        },
        CatalogContext {
            catalog: CatalogId::random(),
            activation_epoch: 1,
        },
    ] {
        assert!(matches!(
            issuer.verify(
                credential.access_key_id(),
                credential.session_token(),
                context,
                1500
            ),
            Err(FileGrantError::Forbidden)
        ));
    }
    for index in [0, 30, 130, 206] {
        let mut token = credential.session_token().as_bytes().to_vec();
        token[index] = if token[index] == b'A' { b'B' } else { b'A' };
        let token = String::from_utf8(token).unwrap();
        assert!(issuer
            .verify(credential.access_key_id(), &token, grant.context, 1500)
            .is_err());
    }
    assert!(issuer
        .verify(
            "CICE0000000000000000",
            credential.session_token(),
            grant.context,
            1500
        )
        .is_err());
    for token in [String::new(), "x".repeat(10_000)] {
        assert!(issuer
            .verify(credential.access_key_id(), &token, grant.context, 1500)
            .is_err());
    }
}

#[test]
fn issuance_enforces_catalog_delegation_duration_and_nonzero_byte_budgets() {
    let issuer = FileGrantIssuer::new([1; 32], 1000).unwrap();
    assert!(FileGrantIssuer::new([0; 32], 1000).is_err());
    assert!(FileGrantIssuer::new([1; 32], 0).is_err());
    for invalid in 0..6 {
        let mut grant = grant();
        match invalid {
            0 => grant.expires_ms = 2001,
            1 => grant.expires_ms = grant.issued_ms,
            2 => grant.issued_ms = u64::MAX,
            3 => grant.max_request_bytes = 0,
            4 => grant.max_file_bytes = 0,
            _ => grant.max_request_bytes = grant.max_file_bytes + 1,
        }
        assert!(issuer.issue(grant).is_err());
    }
}
