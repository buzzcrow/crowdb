// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware C6 residency, normalized by the invariant TSC rather than CPU frequency.

use std::collections::BTreeSet;

use super::imp::{read_pmu_event_config, read_pmu_type, PerfFd, PerfRead};
use crate::metrics::system::C6Residency;

fn parse_cpu_list(text: &str) -> Option<Vec<i32>> {
    let mut cpus = BTreeSet::new();
    for part in text.trim().split(',') {
        let (first, last) = part.split_once('-').unwrap_or((part, part));
        let first: i32 = first.parse().ok()?;
        let last: i32 = last.parse().ok()?;
        if first < 0 || last < first {
            return None;
        }
        cpus.extend(first..=last);
    }
    Some(cpus.into_iter().collect())
}

#[derive(Default)]
struct ResidencyWindow {
    previous: Option<(PerfRead, PerfRead)>,
}

impl ResidencyWindow {
    fn sample(&mut self, c6: Option<PerfRead>, tsc: Option<PerfRead>) -> Option<f64> {
        let previous = self.previous.take();
        let current = (c6?, tsc?);
        self.previous = Some(current);
        let previous = previous?;
        let c6 = scaled_delta(current.0, previous.0)?;
        let tsc = scaled_delta(current.1, previous.1)?;
        // Small boundary skew is possible because these PMUs are read separately.
        // A substantial excess indicates an invalid sample, not valid residency.
        (tsc > 0.0 && c6 <= tsc * 1.01).then(|| (100.0 * c6 / tsc).min(100.0))
    }
}

fn scaled_delta(current: PerfRead, previous: PerfRead) -> Option<f64> {
    let value = current.value.checked_sub(previous.value)?;
    let enabled = current.time_enabled.checked_sub(previous.time_enabled)?;
    let running = current.time_running.checked_sub(previous.time_running)?;
    (running > 0 && enabled >= running).then(|| value as f64 * enabled as f64 / running as f64)
}

struct ResidencyPair {
    cpu: i32,
    c6: PerfFd,
    tsc: PerfFd,
    window: ResidencyWindow,
}

impl ResidencyPair {
    fn collect(&mut self) -> C6Residency {
        C6Residency {
            cpu: self.cpu,
            pct: self.window.sample(self.c6.read(), self.tsc.read()),
        }
    }
}

/// System-wide hardware residency for each physical core and package.
/// PMU cpumasks select one representative logical CPU per hardware domain;
/// SMT siblings are therefore not counted twice. Missing permissions or PMUs
/// leave that scope empty. No software cpuidle fallback is substituted.
#[derive(Default)]
pub struct C6Counters {
    cores: Vec<ResidencyPair>,
    packages: Vec<ResidencyPair>,
}

impl C6Counters {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cores: open_scope("cstate_core").unwrap_or_default(),
            packages: open_scope("cstate_pkg").unwrap_or_default(),
        }
    }

    /// Percentages since the previous collection, keyed by representative CPU.
    pub fn collect(&mut self) -> (Vec<C6Residency>, Vec<C6Residency>) {
        (
            self.cores.iter_mut().map(ResidencyPair::collect).collect(),
            self.packages.iter_mut().map(ResidencyPair::collect).collect(),
        )
    }
}

fn open_scope(pmu: &str) -> Option<Vec<ResidencyPair>> {
    let pmu_type = read_pmu_type(pmu)?;
    let config = read_pmu_event_config(pmu, "c6-residency")?;
    let tsc_type = read_pmu_type("msr")?;
    let tsc_config = read_pmu_event_config("msr", "tsc")?;
    let mask = std::fs::read_to_string(format!("/sys/bus/event_source/devices/{pmu}/cpumask")).ok()?;
    let mut pairs = Vec::new();
    for cpu in parse_cpu_list(&mask)? {
        let mut pair = ResidencyPair {
            cpu,
            c6: PerfFd::open(pmu_type, config, cpu)?,
            tsc: PerfFd::open(tsc_type, tsc_config, cpu)?,
            window: ResidencyWindow::default(),
        };
        let _ = pair.collect(); // Establish both baselines before the first flush.
        pairs.push(pair);
    }
    Some(pairs)
}

#[cfg(feature = "test-util")]
#[must_use]
pub fn parse_c6_cpu_list_for_tests(text: &str) -> Option<Vec<i32>> {
    parse_cpu_list(text)
}

#[cfg(feature = "test-util")]
#[derive(Default)]
pub struct TestC6Window(ResidencyWindow);

#[cfg(feature = "test-util")]
impl TestC6Window {
    pub fn sample_for_tests(&mut self, c6: Option<[u64; 3]>, tsc: Option<[u64; 3]>) -> Option<f64> {
        let sample = |[value, time_enabled, time_running]: [u64; 3]| PerfRead {
            value,
            time_enabled,
            time_running,
        };
        self.0.sample(c6.map(sample), tsc.map(sample))
    }
}
