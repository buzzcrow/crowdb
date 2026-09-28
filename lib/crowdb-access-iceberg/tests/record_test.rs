use crowdb_access_iceberg::catalog::{
    ActiveCatalogRecord, CatalogAuthority, CatalogContext, ClearBounds, ClearTransition, RootState,
};
use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::key::{CatalogId, CatalogScope, IcebergKey, OperationId, SystemScope};
use crowdb_access_iceberg::record::{StorageRecord, MAX_RECORD_BYTES};
use crowdb_protocol::iceberg_fb::{
    self as fb, FBActiveCatalog, FBActiveCatalogArgs, FBCatalogAuthority, FBCatalogAuthorityArgs,
    FBCatalogLifecycle, FBIcebergRecord, FBIcebergRecordArgs, FBRecordValue, FBRootPhase,
};
use flatbuffers::FlatBufferBuilder;

fn root_key() -> IcebergKey {
    IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: Vec::new(),
    }
}

#[test]
fn every_root_phase_round_trips_with_its_exact_catalog_and_deadline() {
    let previous = CatalogContext {
        catalog: CatalogId::random(),
        activation_epoch: 1,
    };
    let operation = OperationId::random();
    let transition = ClearTransition::new(
        operation,
        previous,
        CatalogId::random(),
        1_000,
        ClearBounds {
            root_lease_ms: 30,
            request_ms: 100,
            delegated_access_ms: 200,
            clock_skew_ms: 10,
        },
    )
    .unwrap();
    for state in [
        RootState::Ready,
        RootState::Initializing,
        RootState::Fencing,
        RootState::Maintenance(transition),
        RootState::Published(transition),
    ] {
        let context = if matches!(state, RootState::Published(_)) {
            transition.replacement
        } else {
            previous
        };
        let record = StorageRecord::Active(ActiveCatalogRecord {
            context,
            operation,
            state,
        });
        let encoded = record.encode().unwrap();
        assert_eq!(StorageRecord::decode(&root_key(), &encoded).unwrap(), record);
        assert!(encoded.len() < 1024);
    }
}

#[test]
fn authority_round_trip_binds_identity_to_its_key() {
    let catalog = CatalogId::random();
    let record = StorageRecord::Authority(CatalogAuthority::new(catalog, "目录".into()).unwrap());
    let encoded = record.encode().unwrap();
    let key = IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    };
    assert_eq!(StorageRecord::decode(&key, &encoded).unwrap(), record);
    let wrong = IcebergKey::Catalog {
        catalog: CatalogId::random(),
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    };
    assert_eq!(
        StorageRecord::decode(&wrong, &encoded),
        Err(ValidationError::IdentityMismatch)
    );
    assert!(StorageRecord::decode(&root_key(), &encoded).is_err());
    assert_eq!(
        StorageRecord::decode(&key, &vec![0; MAX_RECORD_BYTES + 1]),
        Err(ValidationError::RecordTooLarge)
    );
}

#[test]
fn root_codec_rejects_unknown_versions_phases_missing_transition_and_zero_ids() {
    for (version, phase, identity) in [
        (2, FBRootPhase::Ready, vec![1; 16]),
        (1, FBRootPhase(99), vec![1; 16]),
        (1, FBRootPhase::Maintenance, vec![1; 16]),
        (1, FBRootPhase::Published, vec![1; 16]),
        (1, FBRootPhase::Ready, vec![0; 16]),
        (1, FBRootPhase::Ready, vec![1; 15]),
    ] {
        let bytes = raw_root(version, phase, &identity);
        assert!(StorageRecord::decode(&root_key(), &bytes).is_err());
    }
    let encoded = raw_root(1, FBRootPhase::Ready, &[1; 16]);
    assert!(StorageRecord::decode(&root_key(), &encoded).is_ok());
    for length in 0..encoded.len() {
        assert!(StorageRecord::decode(&root_key(), &encoded[..length]).is_err());
    }
    let mut wrong_identifier = encoded;
    wrong_identifier[4] ^= 1;
    assert!(StorageRecord::decode(&root_key(), &wrong_identifier).is_err());
}

#[test]
fn authority_codec_rejects_unknown_lifecycle_capabilities_and_invalid_generations() {
    let catalog = CatalogId::random();
    let key = IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    };
    for (lifecycle, capabilities, generation) in [
        (FBCatalogLifecycle(99), 0, 1),
        (FBCatalogLifecycle::Ready, 0x8000, 1),
        (FBCatalogLifecycle::Ready, 8, 1),
        (FBCatalogLifecycle::Ready, 0, 0),
    ] {
        let mut builder = FlatBufferBuilder::new();
        let identity = builder.create_vector(catalog.as_bytes());
        let name = builder.create_string("catalog");
        let authority = FBCatalogAuthority::create(
            &mut builder,
            &FBCatalogAuthorityArgs {
                catalog: Some(identity),
                display_name: Some(name),
                lifecycle,
                capabilities,
                name_generation: generation,
                config_generation: 1,
                request_ms: 1,
                root_lease_ms: 0,
                delegated_access_ms: 0,
                clock_skew_ms: 0,
            },
        );
        let record = FBIcebergRecord::create(
            &mut builder,
            &FBIcebergRecordArgs {
                schema_version: 1,
                value_type: FBRecordValue::FBCatalogAuthority,
                value: Some(authority.as_union_value()),
            },
        );
        fb::finish_fbiceberg_record_buffer(&mut builder, record);
        assert!(StorageRecord::decode(&key, builder.finished_data()).is_err());
    }
}

#[test]
fn encode_rejects_clear_phase_identity_mismatch_and_modified_grace() {
    let previous = CatalogContext {
        catalog: CatalogId::random(),
        activation_epoch: 1,
    };
    let operation = OperationId::random();
    let transition = ClearTransition::new(
        operation,
        previous,
        CatalogId::random(),
        10,
        ClearBounds {
            root_lease_ms: 0,
            request_ms: 10,
            delegated_access_ms: 0,
            clock_skew_ms: 1,
        },
    )
    .unwrap();
    let root = ActiveCatalogRecord {
        context: previous,
        operation,
        state: RootState::Published(transition),
    };
    assert!(StorageRecord::Active(root).encode().is_err());
    assert!(StorageRecord::Active(ActiveCatalogRecord {
        state: RootState::Maintenance(ClearTransition {
            complete_after_ms: 11,
            ..transition
        }),
        ..root
    })
    .encode()
    .is_err());
}

fn raw_root(version: u16, phase: FBRootPhase, catalog: &[u8]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let catalog = builder.create_vector(catalog);
    let operation = builder.create_vector(&[2_u8; 16]);
    let root = FBActiveCatalog::create(
        &mut builder,
        &FBActiveCatalogArgs {
            catalog: Some(catalog),
            activation_epoch: 1,
            operation: Some(operation),
            phase,
            transition: None,
        },
    );
    let record = FBIcebergRecord::create(
        &mut builder,
        &FBIcebergRecordArgs {
            schema_version: version,
            value_type: FBRecordValue::FBActiveCatalog,
            value: Some(root.as_union_value()),
        },
    );
    fb::finish_fbiceberg_record_buffer(&mut builder, record);
    builder.finished_data().to_vec()
}
