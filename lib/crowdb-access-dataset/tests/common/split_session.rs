// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::*;
pub(super) fn spec(name: &str) -> SessionSpecV2 {
    SessionSpecV2 {
        read: SessionSpec {
            id: OperationId::random(),
            snapshot: "latest".into(),
            selection: Selection::Not(Box::new(Selection::Explicit(vec![]))),
            fields: vec!["marker".into()],
            batch_size: 7,
            partition: SessionPartition {
                worker: 0,
                workers: 1,
                rank: 0,
                world_size: 1,
            },
            shuffle: None,
        },
        split: Some(SessionSplit {
            revision: None,
            name: split::SplitName::new(name).unwrap(),
        }),
    }
}
pub(super) async fn prepare(
    sessions: &DatasetSessions,
    identity: &DatasetIdentity,
    spec: &SessionSpecV2,
) -> ReadPreparation {
    for page in 0..100 {
        let result = sessions
            .prepare_split_read(identity, spec, page, 10, &ReadCancellation::new())
            .await
            .unwrap();
        if result.complete {
            return result;
        }
    }
    panic!("read preparation did not finish")
}
pub(super) async fn drain(
    sessions: &DatasetSessions,
    identity: &DatasetIdentity,
    session: ReadSession,
    id: OperationId,
) -> Vec<Vec<u8>> {
    let mut cursor = session.acknowledged;
    let mut ids = Vec::new();
    loop {
        let batch = sessions.next(identity, id, 11).await.unwrap();
        ids.extend(batch.ids);
        cursor = sessions
            .acknowledge(identity, id, &cursor, &batch.next, 12)
            .await
            .unwrap()
            .acknowledged;
        if batch.end {
            return ids;
        }
    }
}
