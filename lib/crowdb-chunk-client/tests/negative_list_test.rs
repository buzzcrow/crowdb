// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_chunk_client::FailedDiskList;
use crowdb_protocol::common::DiskId;

#[tokio::test]
async fn failed_disk_list_is_shared_refreshable_and_expires_by_ttl() {
    let list = FailedDiskList::new(Duration::from_millis(200));
    let first = DiskId { high: 1, low: 2 };
    let second = DiskId { high: 3, low: 4 };
    list.insert(first);
    assert_eq!(list.live(), vec![first]);

    tokio::time::sleep(Duration::from_millis(100)).await;
    list.insert(first);
    list.insert(second);
    tokio::time::sleep(Duration::from_millis(120)).await;
    let mut live = list.live();
    live.sort_unstable_by_key(|disk| (disk.high, disk.low));
    assert_eq!(live, vec![first, second]);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(list.live().is_empty());
}
