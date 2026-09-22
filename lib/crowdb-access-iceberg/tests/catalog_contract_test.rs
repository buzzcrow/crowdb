use crowdb_access_iceberg::catalog::{Capabilities, ClearBounds, FormatSupport};
use crowdb_access_iceberg::catalog::{CatalogAuthority, CatalogContext, CatalogLifecycle, ClearTransition};
use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::key::{CatalogId, OperationId};

#[test]
fn foundation_advertises_no_unimplemented_format_support() {
    let capabilities = Capabilities::default();
    assert_eq!(capabilities.bits(), 0);
    assert_eq!(Capabilities::from_bits(0).unwrap(), capabilities);
}

#[test]
fn capability_decoding_rejects_unknown_and_incoherent_flags() {
    for bits in [0x8000, 0x4000, 2, 4, 8, 0x1000, 0x2000] {
        assert_eq!(Capabilities::from_bits(bits), Err(ValidationError::Capabilities));
    }
    let complete = Capabilities {
        versions: [FormatSupport::from_bits(15).unwrap(); 3],
        upgrade_v1_v2: true,
        upgrade_v2_v3: true,
    };
    assert_eq!(Capabilities::from_bits(complete.bits()).unwrap(), complete);
}

#[test]
fn clear_includes_every_old_access_window_and_rejects_overflow() {
    let bounds = ClearBounds {
        root_lease_ms: 100,
        request_ms: 200,
        delegated_access_ms: 300,
        clock_skew_ms: 40,
    };
    assert_eq!(bounds.completion_deadline(1_000).unwrap(), 1_640);
    assert_eq!(
        bounds.completion_deadline(u64::MAX),
        Err(ValidationError::Deadline)
    );
    assert_eq!(
        ClearBounds {
            request_ms: 0,
            ..bounds
        }
        .completion_deadline(0),
        Err(ValidationError::Deadline)
    );
    assert_eq!(
        ClearBounds {
            root_lease_ms: 0,
            delegated_access_ms: 0,
            ..bounds
        }
        .completion_deadline(1_000)
        .unwrap(),
        1_240
    );
}

#[test]
fn rename_preserves_identity_configuration_and_original_authority() {
    let original = CatalogAuthority::new(CatalogId::random(), "warehouse".into()).unwrap();
    let renamed = original.renamed("new-name".into()).unwrap();
    assert_eq!(renamed.catalog, original.catalog);
    assert_eq!(renamed.config_generation, original.config_generation);
    assert_eq!(renamed.capabilities, original.capabilities);
    assert_eq!(renamed.name_generation, original.name_generation + 1);
    assert_eq!(original.display_name, "warehouse");
    assert!(original.renamed("bad\0name".into()).is_err());
    assert!(CatalogAuthority {
        lifecycle: CatalogLifecycle::Retired,
        ..original.clone()
    }
    .renamed("new".into())
    .is_err());
    assert_eq!(
        CatalogAuthority {
            name_generation: u64::MAX,
            ..original
        }
        .renamed("new".into()),
        Err(ValidationError::GenerationExhausted)
    );
}

#[test]
fn clear_recovery_uses_persisted_deadline_and_rejects_corrupted_contexts() {
    let previous = CatalogContext {
        catalog: CatalogId::random(),
        activation_epoch: 9,
    };
    let bounds = ClearBounds {
        root_lease_ms: 100,
        request_ms: 200,
        delegated_access_ms: 300,
        clock_skew_ms: 40,
    };
    let transition = ClearTransition::new(
        OperationId::random(),
        previous,
        CatalogId::random(),
        1_000,
        bounds,
    )
    .unwrap();
    assert_eq!(transition.replacement.activation_epoch, 10);
    assert!(!transition.grace_elapsed(1_639).unwrap());
    assert!(transition.grace_elapsed(1_640).unwrap());
    assert!(transition.grace_elapsed(999).is_err());
    assert!(ClearTransition {
        complete_after_ms: 1_001,
        ..transition
    }
    .validate()
    .is_err());
    assert!(ClearTransition {
        replacement: previous,
        ..transition
    }
    .validate()
    .is_err());
    assert!(previous.replacement(previous.catalog).is_err());
    assert_eq!(
        CatalogContext {
            activation_epoch: u64::MAX,
            ..previous
        }
        .replacement(CatalogId::random()),
        Err(ValidationError::GenerationExhausted)
    );
}
