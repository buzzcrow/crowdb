// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::*;
pub(super) fn spec(name: &str) -> ReadSpec {
    ReadSpec {
        snapshot: "latest".into(),
        selection: Selection::Not(Box::new(Selection::Explicit(vec![]))),
        fields: vec!["marker".into()],
        batch_size: 7,
        partition: ReadPartition {
            worker: 0,
            workers: 1,
            rank: 0,
            world_size: 1,
        },
        shuffle: None,
        split: Some(ReadSplit {
            revision: None,
            name: split::SplitName::new(name).unwrap(),
        }),
    }
}
pub(super) async fn drain(
    reads: &DatasetReads,
    identity: &DatasetIdentity,
    binding: ReadBinding,
) -> Vec<Vec<u8>> {
    let mut cursor = binding.cursor;
    let mut ids = Vec::new();
    for _ in 0..10000 {
        let page = reads
            .next(identity, &cursor, &ReadCancellation::new())
            .await
            .unwrap();
        ids.extend(page.ids);
        match page.next {
            Some(next) => cursor = next,
            None => return ids,
        }
    }
    panic!("read failed to finish");
}
