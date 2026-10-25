// Copyright 2026-present Gian <crow.db@outlook.com>

//! OS-level system metrics: CPU time, memory RSS, TCP retransmits, and
//! DRAM read/write bandwidth and hardware C6 residency.
//!
//! On Linux, reads `/proc/self/stat` for CPU jiffies, `/proc/self/status`
//! for RSS, `/proc/net/snmp` for TCP retransmit/lost counters, and
//! `perf_event_open` for DRAM read and write bandwidth (AMD `amd_df` or
//! Intel `uncore_imc` uncore PMU). On macOS (and other non-Linux platforms),
//! CPU and RSS are read via `ps` command output; TCP and DRAM BW are
//! stubbed (reported as 0 / unsupported).

use std::io::Write;
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use std::fs;

#[cfg(target_os = "linux")]
use super::perf::{C6Counters, DramBwCounter};

/// Hardware C6 residency of a core or package, identified by its PMU's
/// representative logical CPU. `None` means the interval could not be read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct C6Residency {
    pub cpu: i32,
    pub pct: Option<f64>,
}

/// Snapshot of system-level metrics at a single point in time.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SystemMetrics {
    /// User CPU utilization (percent) since the previous snapshot.
    pub cpu_user_pct: u64,
    /// System CPU utilization (percent) since the previous snapshot.
    pub cpu_sys_pct: u64,
    /// Resident set size in KB.
    pub rss_kb: u64,
    /// TCP retransmit count delta since previous snapshot (Linux only).
    pub tcp_retransmits: u64,
    /// TCP lost segment count delta since previous snapshot (Linux only).
    pub tcp_lost: u64,
    /// Average DRAM read bandwidth in MiB/s since the previous
    /// snapshot. `None` when the PMU is unavailable (non-Linux, missing
    /// kernel module, or insufficient permissions).
    pub dram_read_mib: Option<f64>,
    /// Average DRAM write bandwidth in MiB/s since the previous
    /// snapshot. `None` when the PMU is unavailable.
    pub dram_write_mib: Option<f64>,
    /// Average aggregate DRAM bandwidth. Available on AMD even when the
    /// platform PMU cannot distinguish reads from writes.
    pub dram_total_mib: Option<f64>,
    /// Host physical-core C6 percentages, one entry per PMU representative CPU.
    /// Empty when unsupported or access is denied; independent of process CPU%.
    pub cpu_core_c6: Vec<C6Residency>,
    /// Host package C6 percentages, keyed by PMU representative CPU.
    pub cpu_package_c6: Vec<C6Residency>,
}

/// Collects OS-level metrics by reading `/proc` (Linux) or using
/// `ps` (macOS). Maintains previous-state to compute deltas for
/// CPU time and TCP counters. On Linux, also owns a `DramBwCounter`
/// that reads the uncore PMU via `perf_event_open`. Directional bandwidth is
/// reported only when the platform exposes direction-safe events.
#[allow(clippy::struct_field_names)]
pub struct SystemCollector {
    prev_cpu_user_us: u64,
    prev_cpu_sys_us: u64,
    prev_tcp_retransmits: u64,
    prev_tcp_lost: u64,
    prev_instant: Instant,
    #[cfg(target_os = "linux")]
    dram_bw: Option<DramBwCounter>,
    #[cfg(target_os = "linux")]
    c6: C6Counters,
}

impl SystemCollector {
    /// Create a new collector. The first `collect()` call will report
    /// deltas from this baseline. On Linux, attempts to open a DRAM
    /// bandwidth PMU counter; if unavailable, `dram_read_mib` and
    /// `dram_write_mib` will be `None` in all snapshots.
    #[must_use]
    pub fn new() -> Self {
        let (user_us, sys_us) = read_cpu_times();
        let (retransmits, lost) = read_tcp_stats();
        Self {
            prev_cpu_user_us: user_us,
            prev_cpu_sys_us: sys_us,
            prev_tcp_retransmits: retransmits,
            prev_tcp_lost: lost,
            prev_instant: Instant::now(),
            #[cfg(target_os = "linux")]
            dram_bw: DramBwCounter::new(),
            #[cfg(target_os = "linux")]
            c6: C6Counters::new(),
        }
    }

    /// Collect a system snapshot, computing deltas from the previous call.
    #[must_use]
    pub fn collect(&mut self) -> SystemMetrics {
        let (user_us, sys_us) = read_cpu_times();
        let (retransmits, lost) = read_tcp_stats();
        let elapsed = self.prev_instant.elapsed();
        self.prev_instant = Instant::now();

        let delta_user_us = user_us.saturating_sub(self.prev_cpu_user_us);
        let delta_sys_us = sys_us.saturating_sub(self.prev_cpu_sys_us);
        let tcp_retransmits = retransmits.saturating_sub(self.prev_tcp_retransmits);
        let tcp_lost = lost.saturating_sub(self.prev_tcp_lost);

        self.prev_cpu_user_us = user_us;
        self.prev_cpu_sys_us = sys_us;
        self.prev_tcp_retransmits = retransmits;
        self.prev_tcp_lost = lost;

        let rss_kb = read_rss_kb();

        // CPU utilization = (delta_cpu_us / elapsed_us) * 100.
        // On multi-core systems this can exceed 100% (e.g. 300% = 3 cores).
        let elapsed_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let cpu_user_pct = delta_user_us
            .checked_mul(100)
            .and_then(|v| v.checked_div(elapsed_us))
            .unwrap_or(0);
        let cpu_sys_pct = delta_sys_us
            .checked_mul(100)
            .and_then(|v| v.checked_div(elapsed_us))
            .unwrap_or(0);

        #[cfg(target_os = "linux")]
        let (dram_read_mib, dram_write_mib, dram_total_mib) = self
            .dram_bw
            .as_mut()
            .and_then(DramBwCounter::read_bytes_per_sec)
            .map_or((None, None, None), |(r, w, total)| {
                let to_mib = |value: f64| value / 1024.0 / 1024.0;
                (r.map(to_mib), w.map(to_mib), Some(to_mib(total)))
            });
        #[cfg(not(target_os = "linux"))]
        let (dram_read_mib, dram_write_mib, dram_total_mib) = (None, None, None);

        #[cfg(target_os = "linux")]
        let (cpu_core_c6, cpu_package_c6) = self.c6.collect();
        #[cfg(not(target_os = "linux"))]
        let (cpu_core_c6, cpu_package_c6) = (Vec::new(), Vec::new());

        SystemMetrics {
            cpu_user_pct,
            cpu_sys_pct,
            rss_kb,
            tcp_retransmits,
            tcp_lost,
            dram_read_mib,
            dram_write_mib,
            dram_total_mib,
            cpu_core_c6,
            cpu_package_c6,
        }
    }
}

impl Default for SystemCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Write a system snapshot to the flush writer in the "misc" section format.
pub fn flush_system<W: Write>(writer: &mut W, snap: &SystemMetrics) {
    #[allow(clippy::cast_precision_loss)]
    let rss_gb = snap.rss_kb as f64 / 1024.0 / 1024.0;
    let (bw_read, bw_write) = match (snap.dram_read_mib, snap.dram_write_mib) {
        (Some(r), Some(w)) => (format!("{r:.1}"), format!("{w:.1}")),
        _ => ("unsupported".to_string(), "unsupported".to_string()),
    };
    let bw_total = snap
        .dram_total_mib
        .map_or_else(|| "unsupported".to_string(), |v| format!("{v:.1}"));
    let _ = write!(
        writer,
        "sys  cpu.user={}% cpu.sys={}% rss_gb={rss_gb:.2} tcp_retrans={} tcp_lost={} bw_read_mib={bw_read} bw_write_mib={bw_write} bw_total_mib={bw_total}",
        snap.cpu_user_pct, snap.cpu_sys_pct, snap.tcp_retransmits, snap.tcp_lost,
    );
    flush_c6(writer, "core", &snap.cpu_core_c6);
    flush_c6(writer, "package", &snap.cpu_package_c6);
    let _ = writeln!(writer);
}

fn flush_c6<W: Write>(writer: &mut W, scope: &str, values: &[C6Residency]) {
    if values.is_empty() {
        let _ = write!(writer, " cpu.c6.{scope}=unsupported");
    }
    for value in values {
        let _ = write!(writer, " cpu.c6.{scope}@{}=", value.cpu);
        match value.pct {
            Some(pct) => {
                let _ = write!(writer, "{pct:.1}%");
            }
            None => {
                let _ = write!(writer, "unsupported");
            }
        }
    }
}

// ── Platform-specific readers ───────────────────────────────────

#[cfg(target_os = "linux")]
fn read_cpu_times() -> (u64, u64) {
    // /proc/self/stat fields (1-based):
    //   14 = utime (clock ticks)
    //   15 = stime (clock ticks)
    // Convert to microseconds: ticks * 1_000_000 / sysconf(_SC_CLK_TCK)
    // _SC_CLK_TCK is virtually always 100 on Linux.
    let ticks_per_sec: u64 = 100;
    if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
        // The comm field is in parentheses and may contain spaces,
        // so split from the right after the last ')'.
        if let Some(pos) = stat.rfind(')') {
            let rest = &stat[pos + 2..];
            let fields: Vec<&str> = rest.split_whitespace().collect();
            // fields[0] = state, fields[1] = ppid, ...
            // After ')', the next fields are: state ppid pgrp session tty_nr
            // tpgid flags minflt cminflt majflt cmajflt utime stime ...
            // So utime is at index 11, stime at index 12 (0-based after ')').
            if fields.len() > 12 {
                let utime: u64 = fields[11].parse().unwrap_or(0);
                let stime: u64 = fields[12].parse().unwrap_or(0);
                let user_us = utime * 1_000_000 / ticks_per_sec;
                let sys_us = stime * 1_000_000 / ticks_per_sec;
                return (user_us, sys_us);
            }
        }
    }
    (0, 0)
}

#[cfg(target_os = "linux")]
fn read_rss_kb() -> u64 {
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: u64 = rest
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                return kb;
            }
        }
    }
    0
}

#[cfg(target_os = "linux")]
fn read_tcp_stats() -> (u64, u64) {
    // /proc/net/snmp line: Tcp: ... RetransSegs ...
    // The format has two lines: labels and values.
    if let Ok(snmp) = fs::read_to_string("/proc/net/snmp") {
        let mut lines = snmp.lines().filter(|l| l.starts_with("Tcp:"));
        if let (Some(labels), Some(values)) = (lines.next(), lines.next()) {
            let labels: Vec<&str> = labels.split_whitespace().collect();
            let values: Vec<&str> = values.split_whitespace().collect();
            let retrans_idx = labels.iter().position(|&l| l == "RetransSegs");
            let lost_idx = labels.iter().position(|&l| l == "InErrs" || l == "OutRsts");
            let retransmits = retrans_idx
                .and_then(|i| values.get(i))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let lost = lost_idx
                .and_then(|i| values.get(i))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            return (retransmits, lost);
        }
    }
    (0, 0)
}

#[cfg(not(target_os = "linux"))]
fn read_cpu_times() -> (u64, u64) {
    // macOS: use `ps` to get CPU time. This is a best-effort approach.
    // ps -o utime,stime -p <pid> gives times in M:SS.cc format.
    // For simplicity, report 0 deltas on non-Linux platforms.
    // A future improvement could use mach_time APIs via FFI.
    (0, 0)
}

#[cfg(not(target_os = "linux"))]
fn read_rss_kb() -> u64 {
    // macOS: use `ps` to get RSS.
    // ps -o rss -p <pid> gives RSS in KB.
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output();
    if let Ok(out) = output {
        let rss_str = String::from_utf8_lossy(&out.stdout);
        let rss: u64 = rss_str.trim().parse().unwrap_or(0);
        return rss;
    }
    0
}

#[cfg(not(target_os = "linux"))]
fn read_tcp_stats() -> (u64, u64) {
    // TCP stats are not available on macOS without private APIs.
    (0, 0)
}
