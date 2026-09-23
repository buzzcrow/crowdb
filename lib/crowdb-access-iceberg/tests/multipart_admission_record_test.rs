use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    MultipartAdmissionLimits, MultipartAdmissionRecord, MultipartCreditAction, MultipartCreditMutation,
};
use crowdb_access_iceberg::key::{CatalogId, CatalogScope, IcebergKey, OperationId};
use crowdb_access_iceberg::operation::PayloadReference;
use crowdb_access_iceberg::record::StorageRecord;

fn record() -> MultipartAdmissionRecord {
    MultipartAdmissionRecord {
        context: CatalogContext {
            catalog: CatalogId::random(),
            activation_epoch: 1,
        },
        policy: OperationId::random(),
        revision: 1,
        limits: MultipartAdmissionLimits {
            max_sessions: 2,
            max_reserved_bytes: 100,
        },
        sessions: 0,
        reserved_bytes: 0,
        pending: None,
    }
}

fn mutation(record: &MultipartAdmissionRecord, bytes: u64) -> MultipartCreditMutation {
    let upload = OperationId::random();
    MultipartCreditMutation {
        action: MultipartCreditAction::Reserve,
        upload,
        reservation_bytes: bytes,
        before: None,
        after: PayloadReference {
            catalog: record.context.catalog,
            operation: upload,
            digest: [1; 32],
            length: 100,
        },
    }
}

#[test]
fn admission_limits_independently_bound_sessions_and_reserved_bytes() {
    let initial = record();
    let mut first = initial.proposed(mutation(&initial, 60)).unwrap();
    assert_eq!((first.sessions, first.reserved_bytes), (1, 60));
    assert!(first.proposed(mutation(&first, 1)).is_err());
    first.pending = None;
    assert!(first.proposed(mutation(&first, 41)).is_err());
    let mut second = first.proposed(mutation(&first, 40)).unwrap();
    second.pending = None;
    assert!(second.proposed(mutation(&second, 1)).is_err());
    second.reserved_bytes = 2;
    assert!(second.proposed(mutation(&second, 1)).is_err());
    for limits in [
        MultipartAdmissionLimits {
            max_sessions: 0,
            max_reserved_bytes: 100,
        },
        MultipartAdmissionLimits {
            max_sessions: 65_537,
            max_reserved_bytes: 100,
        },
        MultipartAdmissionLimits {
            max_sessions: 1,
            max_reserved_bytes: 0,
        },
        MultipartAdmissionLimits {
            max_sessions: 1,
            max_reserved_bytes: u64::MAX,
        },
    ] {
        assert!(limits.validate().is_err());
    }
}

#[test]
fn bounded_credit_journals_round_trip_and_reject_foreign_keys() {
    let initial = record();
    let mut reserved = initial.proposed(mutation(&initial, 100)).unwrap();
    let mut release = reserved.pending.clone().unwrap();
    release.action = MultipartCreditAction::Release;
    release.before = Some(release.after.clone());
    release.after.digest = [2; 32];
    reserved.pending = None;
    let released = reserved.proposed(release).unwrap();
    assert_eq!((released.sessions, released.reserved_bytes), (0, 0));
    for value in [initial, reserved, released] {
        let key = value.key();
        assert_eq!(IcebergKey::decode(&key.encode().unwrap()).unwrap(), key);
        let record = StorageRecord::MultipartAdmission(Box::new(value));
        let bytes = record.encode().unwrap();
        assert!(bytes.len() < 1024);
        assert_eq!(StorageRecord::decode(&key, &bytes).unwrap(), record);
        let foreign = IcebergKey::Catalog {
            catalog: CatalogId::random(),
            scope: CatalogScope::MultipartAdmission,
            suffix: Vec::new(),
        };
        assert!(StorageRecord::decode(&foreign, &bytes).is_err());
        let invalid = IcebergKey::Catalog {
            suffix: vec![1],
            catalog: CatalogId::random(),
            scope: CatalogScope::MultipartAdmission,
        };
        assert!(invalid.encode().is_err());
    }
}

#[test]
fn malformed_credit_snapshots_and_counter_underflow_fail_closed() {
    let initial = record();
    let valid = mutation(&initial, 1);
    let mut invalid = valid.clone();
    invalid.after.catalog = CatalogId::random();
    assert!(initial.proposed(invalid).is_err());
    let mut invalid = valid.clone();
    invalid.after.operation = OperationId::random();
    assert!(initial.proposed(invalid).is_err());
    let mut invalid = valid.clone();
    invalid.after.length = 0;
    assert!(initial.proposed(invalid).is_err());
    let mut invalid = valid.clone();
    invalid.action = MultipartCreditAction::Release;
    invalid.before = Some(invalid.after.clone());
    assert!(initial.proposed(invalid).is_err());
    let mut overflow = initial;
    overflow.revision = u64::MAX;
    assert!(overflow.proposed(valid).is_err());
}
