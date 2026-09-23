#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/multipart.rs"]
mod fixtures;

use blocks::TestBlocks;
use crowdb_access_iceberg::file::FileTreeWriter;
use crowdb_access_iceberg::file::{FileIdentity, FileTree, MultipartPart, MultipartPhase};
use crowdb_access_iceberg::key::{CatalogId, FileId, OperationId};
use fixtures::{completion, session};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[tokio::test]
async fn multipart_session_phases_require_frozen_completion_and_never_claim_an_aborted_publication() {
    let mut session = session();
    session.validate().unwrap();
    session.phase = MultipartPhase::Completing;
    assert!(session.validate().is_err());
    session.part_count = 1;
    session.completion = Some(completion(&session));
    session.validate().unwrap();
    session.phase = MultipartPhase::Publishing;
    assert!(session.validate().is_err());
    let mut writer = FileTreeWriter::new(Arc::new(TestBlocks::default()), session.owner, 8).unwrap();
    let completion = session.completion.as_mut().unwrap();
    completion.progress.next_part = 1;
    completion.progress.writer = Some(writer.checkpoint().await.unwrap());
    completion.candidate = Some(writer.finish().await.unwrap());
    session.validate().unwrap();
    session.phase = MultipartPhase::Published;
    assert!(session.validate().is_err());
    session.published = Some(FileId::random());
    session.validate().unwrap();
    session.phase = MultipartPhase::Aborted;
    session.published = None;
    session.validate().unwrap();
    assert!(session.completion.is_some());
    session.published = Some(FileId::random());
    assert!(session.validate().is_err());
}

#[test]
fn multipart_limits_timestamps_identity_and_counts_are_independent() {
    let original = session();
    for change in [0, 1, 2, 3, 4, 5, 6] {
        let mut session = original.clone();
        match change {
            0 => session.expires_ms += 1,
            1 => session.revision = 0,
            2 => session.part_count = 11,
            3 => session.staged_bytes = 1501,
            4 => session.limits.max_part_bytes = 1001,
            5 => session.limits.ttl_ms = 0,
            _ => session.owner.table.catalog = CatalogId::random(),
        }
        assert!(session.validate().is_err());
    }
}

#[test]
fn completion_payload_and_cursor_cannot_cross_upload_or_selection_identity() {
    let mut session = session();
    session.part_count = 1;
    session.phase = MultipartPhase::Completing;
    session.completion = Some(completion(&session));
    for change in [0, 1, 2, 3, 4] {
        let mut changed = session.clone();
        let completion = changed.completion.as_mut().unwrap();
        match change {
            0 => completion.selection.operation = OperationId::random(),
            1 => completion.progress.selection = [4; 32],
            2 => completion.progress.next_part = 2,
            3 => completion.progress.part_offset = 1,
            _ => completion.progress.completed_bytes = 1,
        }
        assert!(changed.validate().is_err());
    }
}

#[test]
fn staged_parts_are_physical_bytes_bound_to_one_upload_table_and_revision() {
    let session = session();
    let mut part = MultipartPart {
        upload: session.upload,
        number: 1,
        revision: 1,
        owner: FileIdentity {
            file: FileId::random(),
            ..session.owner
        },
        tree: FileTree {
            root: None,
            length: 0,
            digest: Sha256::digest([]).into(),
        },
    };
    part.validate_for(&session).unwrap();
    part.number = 0;
    assert!(part.validate_for(&session).is_err());
    part.number = 1;
    part.owner.file = session.owner.file;
    assert!(part.validate_for(&session).is_err());
    part.owner.file = FileId::random();
    part.upload = OperationId::random();
    assert!(part.validate_for(&session).is_err());
}
