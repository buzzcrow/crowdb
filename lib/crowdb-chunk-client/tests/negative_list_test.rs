// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use crowdb_chunk_client::FailedDiskList;
use crowdb_protocol::common::DiskId;

#[test]
fn failed_disk_list_is_shared_refreshable_and_expires_with_backoff() {
    let ttl = Duration::from_secs(60);
    let list = FailedDiskList::new(ttl);
    let first = DiskId { high: 1, low: 2 };
    let second = DiskId { high: 3, low: 4 };
    let start = Instant::now();
    list.insert_at_for_tests(first, start);
    assert_eq!(
        list.live_at_for_tests(start + Duration::from_secs(59)),
        vec![first]
    );
    assert_eq!(list.live_at_for_tests(start + ttl), Vec::<DiskId>::new());

    list.insert_at_for_tests(first, start + Duration::from_secs(70));
    list.insert_at_for_tests(first, start + Duration::from_secs(80));
    list.insert_at_for_tests(first, start + Duration::from_secs(90));
    list.insert_at_for_tests(second, start + Duration::from_secs(90));
    let mut live = list.live_at_for_tests(start + Duration::from_secs(149));
    live.sort_unstable_by_key(|disk| (disk.high, disk.low));
    assert_eq!(live, vec![first, second]);
    assert_eq!(
        list.live_at_for_tests(start + Duration::from_secs(151)),
        vec![first]
    );
    assert!(list
        .live_at_for_tests(start + Duration::from_secs(330))
        .is_empty());

    list.insert_at_for_tests(first, start + Duration::from_secs(400));
    assert_eq!(
        list.live_at_for_tests(start + Duration::from_secs(459)),
        vec![first]
    );
    assert!(list
        .live_at_for_tests(start + Duration::from_secs(460))
        .is_empty());
}
