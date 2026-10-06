use async_trait::async_trait;
use crowdb_access_dataset::{
    AuthorityError, CasOutcome, DatasetError, DatasetIdentity, DatasetStore, FieldDefinition, FieldLocator,
    FieldRecord, ManifestRecord, NamespacePath, OperationId, PublicationState, SampleRecord, SchemaRecord,
    SnapshotId, SnapshotPublication, StoredValue,
};
use crowdb_access_dataset::{
    DatasetReadRequest, DeliveryWindow, GroupShuffle, Ordering, ReadCursor, ReadFailure, ReadLease,
    ReadLimits, ReadPlan, ReadSurface, RetryPolicy, SampleShuffle, Selection, ShuffleSpec,
};
use md5::{Digest, Md5};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

fn md5_digest(payload: &[u8]) -> [u8; 16] {
    let mut digest = Md5::new();
    digest.update(payload);
    digest.finalize().into()
}

struct TestChunkReader {
    location: Vec<u8>,
    payload: Vec<u8>,
}

#[async_trait]
impl crowdb_access_dataset::ChunkReader for TestChunkReader {
    async fn read(
        &self,
        location: &[u8],
        range: Option<std::ops::Range<u64>>,
        cancel: &crowdb_access_dataset::ReadCancellation,
    ) -> Result<Vec<u8>, crowdb_access_dataset::ChunkReadError> {
        if cancel.is_cancelled() {
            return Err(crowdb_access_dataset::ChunkReadError::Cancelled);
        }
        if location != self.location {
            return Err(crowdb_access_dataset::ChunkReadError::NotFound);
        }
        let Some(range) = range else {
            return Ok(self.payload.clone());
        };
        let start = usize::try_from(range.start).unwrap();
        let end = usize::try_from(range.end).unwrap();
        Ok(self.payload[start..end].to_vec())
    }
}

fn publication_manifest(marker: u8) -> Vec<u8> {
    let value = vec![marker];
    let manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::from_bytes(&[marker; 16]).unwrap(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "marker".into(),
                required: true,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: format!("sample-{marker}").into_bytes(),
            fields: vec![FieldRecord {
                name: "marker".into(),
                value: FieldLocator::Inline {
                    md5: md5_digest(&value),
                    value,
                },
            }],
        }],
    };
    bincode::serialize(&manifest).unwrap()
}

#[derive(Default)]
struct MemoryStore {
    values: Mutex<BTreeMap<Vec<u8>, StoredValue>>,
    fail_snapshot_write: Mutex<bool>,
}

impl MemoryStore {
    fn fail_next_snapshot_write(&self) {
        *self.fail_snapshot_write.lock().unwrap() = true;
    }
}

#[async_trait]
impl DatasetStore for MemoryStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, crowdb_access_dataset::StoreError> {
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, crowdb_access_dataset::StoreError> {
        if key.starts_with(b"DS\0S\0") && *self.fail_snapshot_write.lock().unwrap() {
            *self.fail_snapshot_write.lock().unwrap() = false;
            return Err(crowdb_access_dataset::StoreError::Rejected);
        }
        let mut values = self.values.lock().unwrap();
        let current = values.get(key).cloned();
        if current.as_ref().map(|entry| entry.bytes.as_slice()) != expected {
            return Ok(CasOutcome::Conflict(current));
        }
        let revision = current.as_ref().map_or(1, |entry| entry.revision + 1);
        values.insert(
            key.to_vec(),
            StoredValue {
                bytes: value.to_vec(),
                revision,
            },
        );
        Ok(CasOutcome::Applied(revision))
    }

    async fn delete(&self, key: &[u8]) -> Result<(), crowdb_access_dataset::StoreError> {
        self.values.lock().unwrap().remove(key);
        Ok(())
    }
}

#[test]
fn root_namespace_is_implicit_and_dataset_identity_is_qualified() {
    let root = NamespacePath::root();
    assert!(root.is_root());
    let dataset = DatasetIdentity::new(root, "images").unwrap();
    assert_eq!(dataset.key().unwrap(), b"\0\ndataset-ns\xff\0\x06images");
}

#[test]
fn namespace_segments_and_names_reject_reserved_bytes() {
    assert_eq!(
        NamespacePath::new(["a\u{1f}b"]),
        Err(DatasetError::InvalidNamespaceSegment)
    );
    assert_eq!(
        DatasetIdentity::new(NamespacePath::root(), "a\u{1f}b"),
        Err(DatasetError::InvalidName)
    );
}

#[test]
fn manifest_keeps_large_values_opaque() {
    let manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "image".into(),
                required: false,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: b"sample-1".to_vec(),
            fields: vec![FieldRecord {
                name: "image".into(),
                value: FieldLocator::Chunk {
                    location: b"chunk/location".to_vec(),
                    length: 1024,
                    md5: [7; 16],
                },
            }],
        }],
    };
    assert!(manifest.validate().is_ok());
}

#[test]
fn manifest_rejects_duplicate_samples_and_missing_required_fields() {
    let snapshot = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "label".into(),
                required: true,
            }],
        },
        samples: vec![
            SampleRecord {
                sample_id: b"same".to_vec(),
                fields: vec![],
            },
            SampleRecord {
                sample_id: b"same".to_vec(),
                fields: vec![],
            },
        ],
    };
    assert_eq!(manifest.validate(), Err(DatasetError::InvalidManifest));
}

#[test]
fn field_locator_verifies_inline_and_chunk_md5() {
    let payload = b"image-bytes";
    let mut digest = Md5::new();
    digest.update(payload);
    let md5: [u8; 16] = digest.finalize().into();
    let inline = FieldLocator::Inline {
        value: payload.to_vec(),
        md5,
    };
    assert!(inline.verify_payload(payload).is_ok());
    assert_eq!(
        inline.verify_payload(b"corrupt"),
        Err(DatasetError::ChecksumMismatch)
    );
    let chunk = FieldLocator::Chunk {
        location: b"opaque/chunk".to_vec(),
        length: payload.len() as u64,
        md5,
    };
    assert!(chunk.verify_payload(payload).is_ok());
    assert_eq!(
        chunk.verify_payload(b"short"),
        Err(DatasetError::FieldLengthMismatch)
    );
}

#[test]
fn manifest_rejects_corrupt_inline_checksum() {
    let manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "label".into(),
                required: false,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: b"sample".to_vec(),
            fields: vec![FieldRecord {
                name: "label".into(),
                value: FieldLocator::Inline {
                    value: b"value".to_vec(),
                    md5: [0; 16],
                },
            }],
        }],
    };
    assert_eq!(manifest.validate(), Err(DatasetError::ChecksumMismatch));
}

#[test]
fn read_plan_selects_and_orders_without_payload_reads() {
    let snapshot = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "text".into(),
                required: false,
            }],
        },
        samples: vec![
            SampleRecord {
                sample_id: b"b-2".to_vec(),
                fields: vec![FieldRecord {
                    name: "text".into(),
                    value: FieldLocator::Chunk {
                        location: b"opaque/chunk".to_vec(),
                        length: 11,
                        md5: [0; 16],
                    },
                }],
            },
            SampleRecord {
                sample_id: b"a-1".to_vec(),
                fields: vec![],
            },
        ],
    };
    let plan = ReadPlan {
        snapshot,
        selection: Selection::Prefix(b"b".to_vec()),
        ordering: Ordering::SampleId,
        projection: vec!["text".into()],
        batch_size: 2,
    };
    assert_eq!(plan.scan_ids(&manifest).unwrap(), vec![b"b-2".to_vec()]);
    assert_eq!(plan.scan_batches(&manifest).unwrap(), vec![vec![b"b-2".to_vec()]]);
    assert_eq!(plan.identity(), plan.clone().identity());
}

#[test]
fn read_plan_rejects_delivery_limits_before_payload_reads() {
    let snapshot = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "label".into(),
                required: false,
            }],
        },
        samples: (0..3)
            .map(|index| SampleRecord {
                sample_id: format!("sample-{index}").into_bytes(),
                fields: vec![FieldRecord {
                    name: "label".into(),
                    value: FieldLocator::Inline {
                        value: b"payload".to_vec(),
                        md5: md5_digest(b"payload"),
                    },
                }],
            })
            .collect(),
    };
    let plan = ReadPlan {
        snapshot,
        selection: Selection::Prefix(b"sample-".to_vec()),
        ordering: Ordering::SampleId,
        projection: vec!["label".into()],
        batch_size: 2,
    };
    let limits = ReadLimits {
        max_samples: 2,
        max_metadata_bytes: 64,
        max_batches: 2,
        prefetch: 1,
        in_flight: 1,
    };
    assert_eq!(
        plan.scan_batches_with_limits(&manifest, limits),
        Err(DatasetError::ReadLimitExceeded)
    );
    assert_eq!(
        plan.scan_batches_with_limits(
            &manifest,
            ReadLimits {
                max_samples: 4,
                max_metadata_bytes: 64,
                max_batches: 2,
                prefetch: 1,
                in_flight: 1,
            },
        )
        .unwrap()
        .len(),
        2
    );
}

#[test]
fn delivery_window_bounds_and_cancellation_are_lock_free() {
    let window = DeliveryWindow::new(2).unwrap();
    assert!(window.try_acquire());
    assert!(window.try_acquire());
    assert!(!window.try_acquire());
    assert_eq!(window.in_flight(), 2);
    window.release();
    assert!(window.try_acquire());
    window.cancel();
    assert!(!window.try_acquire());
    assert!(window.is_cancelled());
}

#[test]
fn retry_policy_retries_only_transient_failures() {
    let policy = RetryPolicy { max_attempts: 3 };
    assert!(policy.validate().is_ok());
    assert!(policy.should_retry(ReadFailure::Transient, 1));
    assert!(!policy.should_retry(ReadFailure::Transient, 3));
    assert!(!policy.should_retry(ReadFailure::Checksum, 1));
    assert_eq!(
        RetryPolicy { max_attempts: 0 }.validate(),
        Err(DatasetError::InvalidManifest)
    );
}

#[test]
fn sample_shuffle_is_deterministic_and_bounded() {
    let snapshot = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![],
        },
        samples: (0..5)
            .map(|index| SampleRecord {
                sample_id: format!("sample-{index}").into_bytes(),
                fields: vec![],
            })
            .collect(),
    };
    let plan = ReadPlan {
        snapshot,
        selection: Selection::Prefix(b"sample-".to_vec()),
        ordering: Ordering::SampleId,
        projection: vec![],
        batch_size: 2,
    };
    let spec = ShuffleSpec {
        seed: 17,
        epoch: 3,
        max_shuffle_bytes: 1024,
        max_shuffle_objects: 5,
    };
    let first = SampleShuffle::execute(&plan, &manifest, &spec).unwrap();
    assert_eq!(first, SampleShuffle::execute(&plan, &manifest, &spec).unwrap());
    assert_eq!(first.len(), 5);
    assert!(first.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(
        SampleShuffle::execute(
            &plan,
            &manifest,
            &ShuffleSpec {
                max_shuffle_objects: 4,
                ..spec
            },
        ),
        Err(DatasetError::ReadLimitExceeded)
    );
}

#[test]
fn sample_shuffle_windows_and_cursor_resume_are_stable() {
    let snapshot = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![],
        },
        samples: (0..7)
            .map(|index| SampleRecord {
                sample_id: format!("sample-{index}").into_bytes(),
                fields: vec![],
            })
            .collect(),
    };
    let plan = ReadPlan {
        snapshot,
        selection: Selection::Prefix(b"sample-".to_vec()),
        ordering: Ordering::SampleId,
        projection: vec![],
        batch_size: 2,
    };
    let spec = ShuffleSpec {
        seed: 5,
        epoch: 9,
        ..ShuffleSpec::default()
    };
    let windows = SampleShuffle::execute_windows(&plan, &manifest, &spec).unwrap();
    assert_eq!(windows.len(), 4);
    assert_eq!(windows[0].len(), 2);
    let cursor = ReadCursor::start(&plan).confirm_batch(&plan, 0, 2).unwrap();
    let remaining = SampleShuffle::resume(&plan, &manifest, &spec, &cursor).unwrap();
    let expected = windows.iter().skip(1).flatten().cloned().collect::<Vec<_>>();
    assert_eq!(remaining, expected);
    assert_eq!(
        SampleShuffle::execute_groups(
            &plan,
            &manifest,
            &spec,
            &GroupShuffle::Prefix(b"sample-".to_vec())
        )
        .unwrap()
        .iter()
        .flatten()
        .count(),
        7
    );
}

#[test]
fn unsupported_grouping_is_rejected_before_reads() {
    assert_eq!(
        GroupShuffle::MetadataKey("session".into()).validate(),
        Err(DatasetError::UnsupportedGrouping)
    );
    assert_eq!(
        GroupShuffle::Prefix(vec![]).validate(),
        Err(DatasetError::InvalidManifest)
    );
}

#[test]
fn cursor_is_bound_to_plan_and_advances_only_on_confirmation() {
    let snapshot = SnapshotId::random();
    let plan = ReadPlan {
        snapshot,
        selection: Selection::Prefix(b"sample".to_vec()),
        ordering: Ordering::SampleId,
        projection: vec![],
        batch_size: 2,
    };
    let cursor = ReadCursor::start(&plan);
    assert_eq!((cursor.group, cursor.offset), (0, 0));
    let next = cursor.confirm_batch(&plan, 0, 2).unwrap();
    assert_eq!((next.group, next.offset), (0, 2));
    let wrong = ReadPlan {
        snapshot: SnapshotId::random(),
        ..plan
    };
    assert_eq!(next.validate_for(&wrong), Err(DatasetError::CursorMismatch));
}

#[test]
fn read_lease_expires_after_inactivity_and_release() {
    let lease = ReadLease::new(100);
    assert!(!lease.expired(159));
    assert!(lease.expired(160));
    lease.touch(200);
    assert!(!lease.expired(259));
    lease.release();
    assert!(lease.expired(200));
}

#[test]
fn manifest_partitions_repeat_schema_and_keep_snapshot_binding() {
    let snapshot = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "label".into(),
                required: false,
            }],
        },
        samples: (0..3)
            .map(|index| SampleRecord {
                sample_id: vec![index],
                fields: vec![],
            })
            .collect(),
    };
    let partitions = manifest.partition(2).unwrap();
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].samples.len(), 2);
    assert_eq!(partitions[1].samples.len(), 1);
    assert_eq!(partitions[0].snapshot, snapshot);
    assert_eq!(partitions[0].schema, partitions[1].schema);
    assert_eq!(manifest.partition(0), Err(DatasetError::InvalidPartitionSize));
}

#[tokio::test]
async fn authority_persists_snapshot_bound_manifest_partitions_idempotently() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store);
    let identity = DatasetIdentity::new(NamespacePath::root(), "manifest-store").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, vec![7])
        .await
        .unwrap();
    assert_eq!(
        authority
            .reclaim_progress(&identity, snapshot)
            .await
            .unwrap()
            .state,
        crowdb_access_dataset::ReclaimState::Pending
    );
    authority
        .acquire_snapshot_read(&identity, snapshot, 10, 60)
        .await
        .unwrap();
    assert!(matches!(
        authority.reclaim_snapshot_metadata(&identity, snapshot).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    authority
        .release_snapshot_read(&identity, snapshot)
        .await
        .unwrap();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "label".into(),
                required: false,
            }],
        },
        samples: (0..3)
            .map(|index| SampleRecord {
                sample_id: vec![index],
                fields: vec![],
            })
            .collect(),
    };
    assert_eq!(
        authority
            .persist_manifest(&identity, snapshot, &manifest, 2)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        authority
            .persist_manifest(&identity, snapshot, &manifest, 2)
            .await
            .unwrap(),
        2
    );
    let partition = authority
        .get_manifest_partition(&identity, snapshot, 1)
        .await
        .unwrap();
    assert_eq!(partition.samples.len(), 1);
    let wrong = ManifestRecord {
        snapshot: SnapshotId::random(),
        ..manifest
    };
    assert!(matches!(
        authority.persist_manifest(&identity, snapshot, &wrong, 2).await,
        Err(AuthorityError::Invalid(DatasetError::InvalidManifest))
    ));
}

#[tokio::test]
async fn field_resolution_follows_parent_and_tombstone_overrides() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store);
    let identity = DatasetIdentity::new(NamespacePath::root(), "field-history").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let root = authority
        .publish_snapshot(&identity, None, vec![7])
        .await
        .unwrap();
    let mut old_digest = Md5::new();
    old_digest.update(b"old");
    let old_md5: [u8; 16] = old_digest.finalize().into();
    let root_manifest = ManifestRecord {
        version: 1,
        snapshot: root,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "caption".into(),
                required: false,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: b"sample".to_vec(),
            fields: vec![FieldRecord {
                name: "caption".into(),
                value: FieldLocator::Inline {
                    value: b"old".to_vec(),
                    md5: old_md5,
                },
            }],
        }],
    };
    authority
        .persist_manifest(&identity, root, &root_manifest, 10)
        .await
        .unwrap();
    let child = authority
        .publish_snapshot(&identity, Some(root), vec![2])
        .await
        .unwrap();
    let child_manifest = ManifestRecord {
        version: 1,
        snapshot: child,
        schema: root_manifest.schema.clone(),
        samples: vec![SampleRecord {
            sample_id: b"sample".to_vec(),
            fields: vec![FieldRecord {
                name: "caption".into(),
                value: FieldLocator::Tombstone,
            }],
        }],
    };
    authority
        .persist_manifest(&identity, child, &child_manifest, 10)
        .await
        .unwrap();
    assert!(matches!(
        authority
            .resolve_field(&identity, root, b"sample", "caption")
            .await
            .unwrap(),
        Some(FieldLocator::Inline { .. })
    ));
    assert_eq!(
        authority
            .resolve_field(&identity, child, b"sample", "caption")
            .await
            .unwrap(),
        None
    );
}

#[test]
fn publication_validates_parent_and_advances_once() {
    let snapshot = SnapshotId::random();
    let mut publication = SnapshotPublication {
        snapshot,
        parent: Some(snapshot),
        operation: OperationId::random(),
        manifest: vec![1],
        state: PublicationState::Prepared,
    };
    assert_eq!(publication.validate(), Err(DatasetError::SelfParent));
    publication.parent = None;
    publication.publish().unwrap();
    assert_eq!(publication.state, PublicationState::Published);
    assert_eq!(publication.publish(), Err(DatasetError::InvalidPublicationState));
}

#[tokio::test]
async fn authority_requires_default_namespace_and_publishes_complete_head() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store.clone());
    let non_default = DatasetIdentity::new(NamespacePath::new(["tenant"]).unwrap(), "images").unwrap();
    assert!(matches!(
        authority.create_dataset(non_default).await,
        Err(AuthorityError::Invalid(DatasetError::InvalidDefaultNamespace))
    ));

    let identity = DatasetIdentity::new(NamespacePath::root(), "images").unwrap();
    let id = authority.create_dataset(identity.clone()).await.unwrap();
    let record = authority.get_dataset(&identity).await.unwrap();
    assert_eq!(record.id, id);
    let snapshot = authority
        .publish_snapshot(&identity, None, publication_manifest(1))
        .await
        .unwrap();
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(snapshot));
    assert!(record.latest.is_none());
    let chain = authority.snapshot_chain(&identity, snapshot).await.unwrap();
    assert_eq!(chain[0].publication.state, PublicationState::Published);

    let operation = OperationId::random();
    let first = authority
        .publish_snapshot_with_operation(&identity, Some(snapshot), publication_manifest(2), operation)
        .await
        .unwrap();
    let recovered = authority
        .publish_snapshot_with_operation(&identity, Some(snapshot), publication_manifest(2), operation)
        .await
        .unwrap();
    assert_eq!(recovered, first);
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(first));
    assert!(matches!(
        authority
            .publish_snapshot(&identity, Some(snapshot), publication_manifest(3))
            .await,
        Err(AuthorityError::ParentConflict)
    ));
    assert!(matches!(
        authority.create_dataset(identity.clone()).await,
        Err(AuthorityError::AlreadyExists)
    ));

    let restart_operation = OperationId::random();
    let restarted_snapshot = authority
        .publish_snapshot_with_operation(&identity, Some(first), publication_manifest(4), restart_operation)
        .await
        .unwrap();
    let restarted_authority = crowdb_access_dataset::DatasetAuthority::new(store);
    assert_eq!(
        restarted_authority
            .publish_snapshot_with_operation(
                &identity,
                Some(first),
                publication_manifest(4),
                restart_operation
            )
            .await
            .unwrap(),
        restarted_snapshot
    );
    assert_eq!(
        restarted_authority.latest(&identity).await.unwrap(),
        Some(restarted_snapshot)
    );
}

#[tokio::test]
async fn operation_token_recovers_after_snapshot_write_failure() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "recovery").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let operation = OperationId::random();
    store.fail_next_snapshot_write();
    assert!(matches!(
        authority
            .publish_snapshot_with_operation(&identity, None, publication_manifest(9), operation)
            .await,
        Err(AuthorityError::Store(crowdb_access_dataset::StoreError::Rejected))
    ));
    let snapshot = authority
        .publish_snapshot_with_operation(&identity, None, publication_manifest(9), operation)
        .await
        .unwrap();
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(snapshot));
}

#[tokio::test]
async fn prepared_publication_is_invisible_until_manifest_completion() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store);
    let identity = DatasetIdentity::new(NamespacePath::root(), "prepared").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();

    let snapshot = authority
        .publish_snapshot(&identity, None, b"opaque-manifest-reference".to_vec())
        .await
        .unwrap();
    assert_eq!(authority.latest(&identity).await.unwrap(), None);
    assert!(matches!(
        authority.snapshot_chain(&identity, snapshot).await,
        Err(AuthorityError::SnapshotNotFound)
    ));

    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![],
        },
        samples: vec![SampleRecord {
            sample_id: b"sample".to_vec(),
            fields: vec![],
        }],
    };
    authority
        .persist_manifest(&identity, snapshot, &manifest, 1)
        .await
        .unwrap();
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(snapshot));
    assert_eq!(
        authority.snapshot_chain(&identity, snapshot).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn snapshot_chain_follows_declared_parents() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store);
    let identity = DatasetIdentity::new(NamespacePath::root(), "ancestry").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let root = authority
        .publish_snapshot(&identity, None, publication_manifest(1))
        .await
        .unwrap();
    let child = authority
        .publish_snapshot(&identity, Some(root), publication_manifest(2))
        .await
        .unwrap();
    let grandchild = authority
        .publish_snapshot(&identity, Some(child), publication_manifest(3))
        .await
        .unwrap();
    let chain = authority.snapshot_chain(&identity, grandchild).await.unwrap();
    let ids: Vec<_> = chain.iter().map(|record| record.publication.snapshot).collect();
    assert_eq!(ids, vec![grandchild, child, root]);
    assert_eq!(chain[0].publication.parent, Some(child));
    assert_eq!(chain[1].publication.parent, Some(root));
    assert_eq!(chain[2].publication.parent, None);
}

#[tokio::test]
async fn external_source_reference_does_not_mutate_source_authority() {
    let store = Arc::new(MemoryStore::default());
    let source_key = b"iceberg/source/table";
    let source_value = b"source-metadata";
    store
        .compare_exchange(source_key, None, source_value)
        .await
        .unwrap();
    let authority = crowdb_access_dataset::DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "external").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    authority
        .publish_snapshot(&identity, None, source_key.to_vec())
        .await
        .unwrap();
    assert_eq!(store.get(source_key).await.unwrap().unwrap().bytes, source_value);
}

#[tokio::test]
async fn projection_omits_tombstones_and_rejects_required_schema_removal() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store);
    let identity = DatasetIdentity::new(NamespacePath::root(), "schema-history").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let root = authority
        .publish_snapshot(&identity, None, vec![1])
        .await
        .unwrap();
    let schema = SchemaRecord {
        version: 1,
        fields: vec![FieldDefinition {
            name: "required".into(),
            required: true,
        }],
    };
    let root_manifest = ManifestRecord {
        version: 1,
        snapshot: root,
        schema: schema.clone(),
        samples: vec![SampleRecord {
            sample_id: b"sample".to_vec(),
            fields: vec![FieldRecord {
                name: "required".into(),
                value: FieldLocator::Tombstone,
            }],
        }],
    };
    authority
        .persist_manifest(&identity, root, &root_manifest, 10)
        .await
        .unwrap();
    let child = authority
        .publish_snapshot(&identity, Some(root), vec![2])
        .await
        .unwrap();
    let invalid = ManifestRecord {
        version: 1,
        snapshot: child,
        schema: SchemaRecord {
            version: 2,
            fields: vec![],
        },
        samples: vec![],
    };
    assert!(matches!(
        authority.persist_manifest(&identity, child, &invalid, 10).await,
        Err(AuthorityError::Invalid(DatasetError::InvalidManifest))
    ));
    let projection = authority
        .resolve_projection(&identity, root, b"sample", &["required", "missing"])
        .await
        .unwrap();
    assert!(projection.is_empty());
}

#[tokio::test]
async fn read_batch_preserves_requested_sample_order_and_projection() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store);
    let identity = DatasetIdentity::new(NamespacePath::root(), "batch-read").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, vec![1])
        .await
        .unwrap();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "label".into(),
                required: false,
            }],
        },
        samples: vec![
            SampleRecord {
                sample_id: b"a".to_vec(),
                fields: vec![FieldRecord {
                    name: "label".into(),
                    value: FieldLocator::Tombstone,
                }],
            },
            SampleRecord {
                sample_id: b"b".to_vec(),
                fields: vec![],
            },
        ],
    };
    authority
        .persist_manifest(&identity, snapshot, &manifest, 10)
        .await
        .unwrap();
    let batch = authority
        .read_batch(&identity, snapshot, &[b"b".to_vec(), b"a".to_vec()], &["label"])
        .await
        .unwrap();
    assert_eq!(
        batch
            .iter()
            .map(|sample| sample.sample_id.as_slice())
            .collect::<Vec<_>>(),
        vec![b"b", b"a"]
    );
    assert!(batch[0].fields.is_empty());
    assert!(batch[1].fields.is_empty());
    let native = authority
        .read_batch_surface(
            ReadSurface::Native,
            &identity,
            snapshot,
            &[b"b".to_vec(), b"a".to_vec()],
            &["label"],
        )
        .await
        .unwrap();
    let http = authority
        .read_batch_surface(
            ReadSurface::Http,
            &identity,
            snapshot,
            &[b"b".to_vec(), b"a".to_vec()],
            &["label"],
        )
        .await
        .unwrap();
    assert_eq!(native, http);
    assert_eq!(native, batch);
}

#[tokio::test]
async fn chunk_payload_is_materialized_and_unknown_samples_are_rejected() {
    let store = Arc::new(MemoryStore::default());
    let payload = b"chunk-value".to_vec();
    let location = b"opaque-location".to_vec();
    let authority = crowdb_access_dataset::DatasetAuthority::with_chunk_reader(
        store,
        Arc::new(TestChunkReader {
            location: location.clone(),
            payload: payload.clone(),
        }),
    );
    let identity = DatasetIdentity::new(NamespacePath::root(), "payload-read").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, vec![1])
        .await
        .unwrap();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "blob".into(),
                required: false,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: b"member".to_vec(),
            fields: vec![FieldRecord {
                name: "blob".into(),
                value: FieldLocator::Chunk {
                    location,
                    length: payload.len() as u64,
                    md5: md5_digest(&payload),
                },
            }],
        }],
    };
    authority
        .persist_manifest(&identity, snapshot, &manifest, 10)
        .await
        .unwrap();
    let batch = authority
        .read_batch(&identity, snapshot, &[b"member".to_vec()], &["blob"])
        .await
        .unwrap();
    assert_eq!(batch[0].values["blob"], payload);
    assert!(matches!(
        authority
            .read_batch(&identity, snapshot, &[b"excluded".to_vec()], &["blob"])
            .await,
        Err(AuthorityError::Invalid(DatasetError::SampleNotFound))
    ));
}

#[tokio::test]
async fn retention_stable_and_reclaim_protect_live_snapshots() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store);
    let identity = DatasetIdentity::new(NamespacePath::root(), "retention-controls").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, publication_manifest(31))
        .await
        .unwrap();
    assert!(matches!(
        authority.reclaim_snapshot_metadata(&identity, snapshot).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    authority.retain_snapshot(&identity, snapshot).await.unwrap();
    authority.release_snapshot(&identity, snapshot).await.unwrap();
    authority.set_stable(&identity, snapshot).await.unwrap();
    assert!(matches!(
        authority.reclaim_snapshot_metadata(&identity, snapshot).await,
        Err(AuthorityError::SnapshotProtected)
    ));
}

#[tokio::test]
async fn durable_cursor_survives_authority_restart_and_rejects_stale_writes() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "cursor-store").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let plan = ReadPlan {
        snapshot: SnapshotId::random(),
        selection: Selection::Prefix(b"sample".to_vec()),
        ordering: Ordering::SampleId,
        projection: vec![],
        batch_size: 2,
    };
    let start = ReadCursor::start(&plan);
    authority
        .persist_cursor(&identity, &plan, &start, None)
        .await
        .unwrap();
    let next = start.confirm_batch(&plan, 1, 2).unwrap();
    authority
        .persist_cursor(&identity, &plan, &next, Some(&start))
        .await
        .unwrap();
    let restarted = crowdb_access_dataset::DatasetAuthority::new(store);
    assert_eq!(
        restarted.load_cursor(&identity, &plan).await.unwrap(),
        Some(next.clone())
    );
    assert!(matches!(
        authority
            .persist_cursor(&identity, &plan, &start, Some(&start))
            .await,
        Err(AuthorityError::CursorConflict)
    ));
}

#[test]
fn dataset_read_wire_contract_preserves_order_and_rejects_duplicates() {
    let snapshot = SnapshotId::random();
    let request = DatasetReadRequest {
        snapshot,
        sample_ids: vec![b"b".to_vec(), b"a".to_vec()],
        fields: vec!["label".into()],
    };
    assert!(request.validate().is_ok());
    let encoded = bincode::serialize(&request).unwrap();
    assert_eq!(
        bincode::deserialize::<DatasetReadRequest>(&encoded).unwrap(),
        request
    );
    assert_eq!(
        DatasetReadRequest {
            sample_ids: vec![b"a".to_vec(), b"a".to_vec()],
            ..request
        }
        .validate(),
        Err(DatasetError::InvalidManifest)
    );
}
