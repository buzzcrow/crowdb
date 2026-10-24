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
fn legacy_limits_and_counter_records_reject_invalid_bounds() {
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
    for (sessions, bytes) in [(3, 3), (1, 101), (1, 0), (0, 1)] {
        let mut value = record();
        value.sessions = sessions;
        value.reserved_bytes = bytes;
        assert!(value.validate().is_err());
    }
}

#[test]
fn bounded_credit_journals_round_trip_and_reject_foreign_keys() {
    let initial = record();
    let mut reserved = initial.clone();
    reserved.revision = 2;
    reserved.sessions = 1;
    reserved.reserved_bytes = 100;
    reserved.pending = Some(mutation(&initial, 100));
    let mut release = reserved.pending.clone().unwrap();
    release.action = MultipartCreditAction::Release;
    release.before = Some(release.after.clone());
    release.after.digest = [2; 32];
    reserved.pending = None;
    let mut released = reserved.clone();
    released.revision += 1;
    released.sessions = 0;
    released.reserved_bytes = 0;
    released.pending = Some(release);
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
fn malformed_legacy_credit_payloads_fail_closed() {
    let mut valid = record();
    valid.revision = 2;
    valid.sessions = 1;
    valid.reserved_bytes = 1;
    valid.pending = Some(mutation(&valid, 1));
    valid.validate().unwrap();
    for case in 0..5 {
        let mut invalid = valid.clone();
        let pending = invalid.pending.as_mut().unwrap();
        match case {
            0 => pending.after.catalog = CatalogId::random(),
            1 => pending.after.operation = OperationId::random(),
            2 => pending.after.length = 0,
            3 => pending.before = Some(pending.after.clone()),
            4 => invalid.revision = u64::MAX,
            _ => unreachable!(),
        }
        assert!(invalid.validate().is_err());
    }
}
