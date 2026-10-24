// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_common::metrics::system::{flush_system, C6Residency, SystemCollector, SystemMetrics};

#[test]
fn collect_returns_snapshot() {
    let mut collector = SystemCollector::new();
    let snap = collector.collect();
    // CPU deltas should be small but non-negative.
    // RSS should be non-zero (the test process has memory).
    assert!(snap.rss_kb > 0, "RSS should be non-zero, got {}", snap.rss_kb);
}

#[test]
fn collect_delta_is_non_negative() {
    let mut collector = SystemCollector::new();
    let _snap1 = collector.collect();
    let snap2 = collector.collect();
    // Utilization should be non-negative (can exceed 100% on multi-core).
    assert!(snap2.cpu_user_pct <= 100 * 1024);
}

#[test]
fn flush_system_writes_all_fields() {
    let snap = SystemMetrics {
        cpu_user_pct: 42,
        cpu_sys_pct: 17,
        rss_kb: 4096,
        tcp_retransmits: 3,
        tcp_lost: 1,
        dram_read_mib: Some(512.5),
        dram_write_mib: Some(128.3),
        dram_total_mib: Some(640.8),
        cpu_core_c6: vec![
            C6Residency {
                cpu: 0,
                pct: Some(83.25),
            },
            C6Residency {
                cpu: 2,
                pct: Some(0.0),
            },
        ],
        cpu_package_c6: vec![C6Residency { cpu: 0, pct: None }],
    };
    let mut buf = Vec::new();
    flush_system(&mut buf, &snap);
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("cpu.user=42%"));
    assert!(out.contains("cpu.sys=17%"));
    assert!(out.contains("rss_gb=0.00"));
    assert!(out.contains("tcp_retrans=3"));
    assert!(out.contains("tcp_lost=1"));
    assert!(out.contains("bw_read_mib=512.5"));
    assert!(out.contains("bw_write_mib=128.3"));
    assert!(out.contains("bw_total_mib=640.8"));
    assert!(out.contains("cpu.c6.core@0=83.2% cpu.c6.core@2=0.0% cpu.c6.package@0=unsupported"));
}

#[test]
fn flush_system_writes_unsupported_when_none() {
    let snap = SystemMetrics {
        cpu_user_pct: 0,
        cpu_sys_pct: 0,
        rss_kb: 0,
        tcp_retransmits: 0,
        tcp_lost: 0,
        dram_read_mib: None,
        dram_write_mib: None,
        dram_total_mib: None,
        ..SystemMetrics::default()
    };
    let mut buf = Vec::new();
    flush_system(&mut buf, &snap);
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("bw_read_mib=unsupported"));
    assert!(out.contains("bw_total_mib=unsupported"));
    assert!(out.contains("bw_write_mib=unsupported"));
    assert!(out.contains("cpu.c6.core=unsupported cpu.c6.package=unsupported"));
}
