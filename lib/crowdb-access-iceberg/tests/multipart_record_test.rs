#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/multipart.rs"]
mod fixtures;

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    AssemblyPart, FileAssembly, FileIdentity, FileTreeWriter, MultipartPart, MultipartPhase,
};
use crowdb_access_iceberg::key::{CatalogId, CatalogScope, FileId, IcebergKey};
use crowdb_access_iceberg::record::StorageRecord;
use std::sync::Arc;

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
    let assembly = FileAssembly::new(store, session.owner, [3; 32], 1, 1000, 2, 3).unwrap();
    let completion = session.completion.as_mut().unwrap();
    completion.progress = assembly.advance(&completion.progress, &part).await.unwrap();
    check(&session);
    while session.completion.as_ref().unwrap().progress.next_part == 0 {
        let completion = session.completion.as_mut().unwrap();
        completion.progress = assembly.advance(&completion.progress, &part).await.unwrap();
        check(&session);
    }
    let completion = session.completion.as_mut().unwrap();
    completion.candidate = Some(assembly.finish(&completion.progress).await.unwrap());
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
        tree: part.tree,
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
