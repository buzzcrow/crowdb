// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskdb::recovery::compaction::CompactionScheduleForTests;

#[test]
fn frequent_threshold_polls_do_not_starve_small_zone_backlogs() {
    let schedule = CompactionScheduleForTests::default();
    for elapsed in (5..300).step_by(5) {
        assert_eq!(schedule.threshold(elapsed, 300, 4096), 4096);
    }
    // A 256-block zone cannot accumulate 4096 distinct frees. The deadline
    // must still admit its smallest nonempty backlog to the compaction pass.
    assert_eq!(schedule.threshold(300, 300, 4096), 1);
    assert_eq!(schedule.threshold(305, 300, 4096), 4096);
    assert_eq!(schedule.threshold(600, 300, 4096), 1);
}

#[test]
fn live_cadence_changes_and_delayed_wakeups_keep_periodic_cleanup() {
    let schedule = CompactionScheduleForTests::default();
    assert_eq!(schedule.threshold(20, 300, 64), 64);
    assert_eq!(schedule.threshold(30, 30, 64), 1);
    assert_eq!(schedule.threshold(1000, 30, 64), 1);
    assert_eq!(schedule.threshold(1005, 60, 128), 128);
    assert_eq!(schedule.threshold(1060, 60, 128), 1);
}
