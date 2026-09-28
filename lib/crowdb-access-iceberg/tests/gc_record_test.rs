use crowdb_access_iceberg::{
    catalog::CatalogContext,
    gc::{GcLimits, GcPage, GcPhase, GcStalledReason, GcTask, GcTaskKind},
    key::{CatalogId, CatalogScope, FileId, IcebergKey, OperationId},
    record::StorageRecord,
};

fn task() -> GcTask {
    GcTask {
        discovery_scope: 0,
        proof: crowdb_access_iceberg::gc::GcProofState::default(),
        sweep_round: 0,
        deferred_ranges: false,
        context: CatalogContext {
            catalog: CatalogId::random(),
            activation_epoch: 1,
        },
        identity: OperationId::random(),
        kind: GcTaskKind::RetiredCatalog,
        phase: GcPhase::Discover,
        revision: 1,
        created_ms: 100,
        not_before_ms: 1000,
        retry_at_ms: 0,
        attempts: 0,
        paused: false,
        fenced: false,
        stalled: GcStalledReason::None,
        quarantined_from: None,
        head: None,
        scan_after: Vec::new(),
        queue_read: 0,
        queue_write: 0,
        marked: 0,
        deleted: 0,
        reclaimed_bytes: 0,
    }
}

#[test]
fn task_codec_rejects_foreign_keys_and_invalid_progress() {
    let mut task = task();
    let record = StorageRecord::GcTask(Box::new(task.clone()));
    let bytes = record.encode().unwrap();
    assert_eq!(StorageRecord::decode(&task.key(), &bytes).unwrap(), record);
    let mut foreign = task.clone();
    foreign.context.catalog = CatalogId::random();
    assert!(StorageRecord::decode(&foreign.key(), &bytes).is_err());
    task.queue_read = 1;
    assert!(task.validate().is_err());
    task.queue_read = 0;
    task.scan_after = foreign.key().encode().unwrap();
    assert!(task.validate().is_err());
    task.scan_after.clear();
    task.discovery_scope = 3;
    assert!(task.validate().is_err());
    task.discovery_scope = 1;
    task.validate().unwrap();
    task.phase = GcPhase::Roots;
    assert!(task.validate().is_err());
}

#[test]
fn mark_pages_are_bounded_sorted_and_catalog_scoped() {
    let task = task();
    let key = IcebergKey::Catalog {
        catalog: task.context.catalog,
        scope: CatalogScope::File,
        suffix: FileId::random().as_bytes().to_vec(),
    }
    .encode()
    .unwrap();
    let mut page = GcPage {
        catalog: task.context.catalog,
        task: task.identity,
        kind: 1,
        sequence: 0,
        entries: vec![key.clone()],
    };
    let record = StorageRecord::GcPage(Box::new(page.clone()));
    assert_eq!(
        StorageRecord::decode(&page.key(), &record.encode().unwrap()).unwrap(),
        record
    );
    page.entries.push(key);
    assert!(page.validate().is_err());
    page.entries.truncate(1);
    page.catalog = CatalogId::random();
    assert!(page.validate().is_err());
}

#[test]
fn independent_gc_bounds_and_retry_backoff_reject_unbounded_work() {
    let limits = GcLimits::default();
    limits.validate().unwrap();
    assert_eq!(limits.retry_delay_ms(1), 1000);
    assert_eq!(limits.retry_delay_ms(2), 2000);
    assert_eq!(limits.retry_delay_ms(u32::MAX), 60_000);
    for invalid in [
        GcLimits {
            page_items: 0,
            ..limits
        },
        GcLimits {
            page_bytes: 64 * 1024,
            ..limits
        },
        GcLimits { step_ms: 0, ..limits },
        GcLimits {
            concurrency: 0,
            ..limits
        },
        GcLimits {
            minimum_retention_ms: 0,
            ..limits
        },
        GcLimits {
            retry_max_ms: 1,
            ..limits
        },
    ] {
        assert!(invalid.validate().is_err());
    }
}
