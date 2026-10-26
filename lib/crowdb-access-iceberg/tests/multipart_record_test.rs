#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/multipart.rs"]
mod fixtures;
#[path = "common/legacy_checkpoint.rs"]
mod legacy_checkpoint;

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    AssemblyPart, AssemblyProgress, FileDigest, FileIdentity, FileTreeWriter, MultipartPart, MultipartPhase,
    PartFingerprint,
};
use crowdb_access_iceberg::key::{CatalogId, CatalogScope, FileId, IcebergKey};
use crowdb_access_iceberg::record::StorageRecord;
use std::sync::Arc;

async fn legacy_progress(
    store: Arc<TestBlocks>,
    owner: FileIdentity,
    part: &AssemblyPart,
) -> AssemblyProgress {
    let prefix = b"by";
    let (checkpoint, _) = legacy_checkpoint::TestLegacyCheckpoint::create(store, owner, prefix, 3).await;
    let mut digest = FileDigest::new(part.owner);
    digest.update(prefix).unwrap();
    AssemblyProgress {
        selection: [3; 32],
        next_part: 0,
        part_offset: 2,
        completed_bytes: 2,
        writer: Some(checkpoint),
        active: Some(PartFingerprint {
            owner: part.owner,
            length: part.tree.length,
            digest: part.tree.digest,
        }),
        part_digest: Some(digest.checkpoint()),
    }
}

#[tokio::test]
async fn native_multipart_identity_round_trips_the_full_storage_path() {
    let mut session = fixtures::session();
    session.owner.table.storage_namespace = Some(crowdb_access_iceberg::key::NamespaceId::random());
    session.location = session.owner.table.file(session.location.relative_key()).unwrap();
    let record = StorageRecord::MultipartSession(Box::new(session.clone()));
    assert_eq!(
        StorageRecord::decode(&session.key(), &record.encode().unwrap()).unwrap(),
        record
    );
    session.owner.table.storage_namespace = Some(crowdb_access_iceberg::key::NamespaceId::random());
    assert!(session.validate().is_err());
}

#[tokio::test]
async fn native_partial_multipart_checkpoint_survives_record_reload() {
    let store = Arc::new(TestBlocks::default());
    let mut session = fixtures::session();
    session.owner.table.storage_namespace = Some(crowdb_access_iceberg::key::NamespaceId::random());
    session.location = session.owner.table.file("data/native.parquet").unwrap();
    let owner = FileIdentity {
        file: FileId::random(),
        ..session.owner
    };
    let mut writer = FileTreeWriter::new(store.clone(), owner, 3).unwrap();
    writer.push(b"bytes").await.unwrap();
    let part = AssemblyPart {
        ordinal: 0,
        owner,
        tree: writer.finish().await.unwrap(),
    };
    session.part_count = 1;
    session.staged_bytes = 5;
    session.phase = MultipartPhase::Completing;
    session.completion = Some(fixtures::completion(&session));
    let completion = session.completion.as_mut().unwrap();
    completion.progress = legacy_progress(store.clone(), session.owner, &part).await;
    assert_eq!(completion.progress.part_digest.as_ref().unwrap().len(), 205);
    let record = StorageRecord::MultipartSession(Box::new(session.clone()));
    assert_eq!(
        StorageRecord::decode(&session.key(), &record.encode().unwrap()).unwrap(),
        record
    );
    let completion = session.completion.as_mut().unwrap();
    completion
        .progress
        .active
        .as_mut()
        .unwrap()
        .owner
        .table
        .storage_namespace = Some(crowdb_access_iceberg::key::NamespaceId::random());
    assert!(session.validate().is_err());
}

#[tokio::test]
async fn multipart_records_round_trip_open_partial_publishing_published_and_aborted_states() {
    let store = Arc::new(TestBlocks::default());
    let mut session = fixtures::session();
    let check = |session: &crowdb_access_iceberg::file::MultipartSession| {
        let record = StorageRecord::MultipartSession(Box::new(session.clone()));
        let bytes = record.encode().unwrap();
        assert!(bytes.len() < 4096);
        assert_eq!(StorageRecord::decode(&session.key(), &bytes).unwrap(), record);
    };
    check(&session);
    let owner = FileIdentity {
        file: FileId::random(),
        ..session.owner
    };
    let mut writer = FileTreeWriter::new(store.clone(), owner, 3).unwrap();
    writer.push(b"bytes").await.unwrap();
    let part = AssemblyPart {
        ordinal: 0,
        owner,
        tree: writer.finish().await.unwrap(),
    };
    session.part_count = 1;
    session.staged_bytes = 5;
    session.phase = MultipartPhase::Completing;
    session.completion = Some(fixtures::completion(&session));
    let completion = session.completion.as_mut().unwrap();
    completion.progress = legacy_progress(store.clone(), session.owner, &part).await;
    check(&session);
    let completion = session.completion.as_mut().unwrap();
    completion.progress.writer = None;
    completion.progress.next_part = 1;
    completion.progress.part_offset = 0;
    completion.progress.completed_bytes = part.tree.length;
    completion.progress.active = None;
    completion.progress.part_digest = None;
    completion.candidate = Some(part.tree.clone());
    completion.publication = Some(completion.selection.clone());
    session.phase = MultipartPhase::Publishing;
    check(&session);
    session.phase = MultipartPhase::Published;
    session.published = Some(FileId::random());
    check(&session);
    session.phase = MultipartPhase::Aborted;
    session.published = None;
    check(&session);
    session.phase = MultipartPhase::Conflicted;
    check(&session);
    let part = MultipartPart {
        upload: session.upload,
        number: 1,
        revision: 2,
        modified_ms: 101,
        owner,
        tree: Some(part.tree),
        stream: None,
    };
    let record = StorageRecord::MultipartPart(Box::new(part.clone()));
    let bytes = record.encode().unwrap();
    assert!(bytes.len() < 1024);
    assert_eq!(StorageRecord::decode(&part.key(), &bytes).unwrap(), record);
    let mut wrong = part;
    wrong.modified_ms = 0;
    assert!(StorageRecord::MultipartPart(Box::new(wrong.clone()))
        .encode()
        .is_err());
    wrong.modified_ms = 101;
    wrong.number = 2;
    assert!(StorageRecord::decode(&wrong.key(), &bytes).is_err());
}

#[test]
fn multipart_keys_and_record_envelopes_reject_foreign_or_invalid_domains() {
    let session = fixtures::session();
    let record = StorageRecord::MultipartSession(Box::new(session.clone()));
    let bytes = record.encode().unwrap();
    let mut other = session.clone();
    other.context.catalog = CatalogId::random();
    assert!(StorageRecord::decode(&other.key(), &bytes).is_err());
    assert!(StorageRecord::MultipartSession(Box::new(other)).encode().is_err());
    for number in [0_u16, 10_001, u16::MAX] {
        let mut suffix = session.upload.as_bytes().to_vec();
        suffix.extend_from_slice(&number.to_be_bytes());
        let key = IcebergKey::Catalog {
            catalog: session.context.catalog,
            scope: CatalogScope::MultipartPart,
            suffix,
        };
        assert!(key.encode().is_err());
    }
    let encoded_key = session.key().encode().unwrap();
    assert_eq!(IcebergKey::decode(&encoded_key).unwrap(), session.key());
    let mut bytes = bytes;
    bytes.truncate(8);
    assert!(StorageRecord::decode(&session.key(), &bytes).is_err());
}

#[test]
fn multipart_decoder_rejects_unknown_phases_and_invalid_raw_revisions() {
    use crowdb_protocol::iceberg_fb::{root_as_fbiceberg_record, FBMultipartSession};
    let session = fixtures::session();
    let original = StorageRecord::MultipartSession(Box::new(session.clone()))
        .encode()
        .unwrap();
    let offsets = {
        let envelope = root_as_fbiceberg_record(&original).unwrap();
        let value = envelope.value_as_fbmultipart_session().unwrap();
        let table = value._tab;
        (
            table.loc() + usize::from(table.vtable().get(FBMultipartSession::VT_PHASE)),
            table.loc() + usize::from(table.vtable().get(FBMultipartSession::VT_REVISION)),
        )
    };
    let mut bytes = original.clone();
    bytes[offsets.0] = 255;
    assert!(StorageRecord::decode(&session.key(), &bytes).is_err());
    let mut bytes = original;
    bytes[offsets.1..offsets.1 + 8].fill(0);
    assert!(StorageRecord::decode(&session.key(), &bytes).is_err());
}
