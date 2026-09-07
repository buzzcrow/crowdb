// Copyright 2026-present Gian <crow.db@outlook.com>

//! DRAM read/write bandwidth counter via `perf_event_open` (Linux only).
//!
//! Opens system-wide uncore PMU counters at construction time and
//! reads cumulative byte counts on each [`DramBwCounter::read_bytes_per_sec`]
//! call. It returns separate directions where the platform exposes them and
//! an aggregate total otherwise.
//!
//! Two PMU backends are auto-detected:
//! - **AMD Zen 3** — `amd_df` PMU, the kernel-provided
//!   `dram_channel_data_controller_0..7` aggregate events. These counters
//!   do not distinguish reads from writes.
//! - **Intel** — `uncore_imc` PMU, `cas_count_read` and
//!   `cas_count_write` events separately, each tick = 64 B.
//!
//! On non-Linux platforms or when the PMU is unavailable (missing
//! kernel module, insufficient permissions), [`DramBwCounter::new`]
//! returns `None` and the caller reports bandwidth as unsupported.

#![allow(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::borrow_as_ptr,
    clippy::items_after_statements
)]

#[cfg(target_os = "linux")]
mod imp {
    use std::time::Instant;

    // perf_event_attr.size — the kernel uses this to know which fields
    // are valid. 0 means "use default". We set it to the struct size.
    // The kernel ignores fields beyond what it knows.
    const PERF_ATTR_SIZE: u32 = 120; // sizeof(perf_event_attr) on modern kernels

    /// Raw perf_event_attr layout (partial — only fields we need).
    /// The kernel reads up to `.size` bytes, so unused trailing fields
    /// are zero-filled by Default.
    #[repr(C)]
    #[derive(Default)]
    struct PerfEventAttr {
        type_: u32,
        size: u32,
        config: u64,
        sample_period_or_freq: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events_or_watermark: u32,
        bp_type: u32,
        bp_addr_or_config1: u64,
        bp_len_or_config2: u64,
        branch_sample_type: u64,
        sample_regs_user: u64,
        sample_stack_user: u32,
        clockid: i32,
        sample_regs_intr: u64,
        aux_watermark: u32,
        sample_max_stack: u16,
        reserved2: u16,
        aux_sample_size: u32,
        reserved3: u32,
        sig_data: u64,
    }

    // perf_event_attr.flags bits
    const PERF_FLAG_DISABLED: u64 = 1;
    const PERF_FORMAT_TOTAL_TIME_ENABLED: u64 = 1;
    const PERF_FORMAT_TOTAL_TIME_RUNNING: u64 = 2;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct PerfRead {
        value: u64,
        time_enabled: u64,
        time_running: u64,
    }

    /// A single opened perf counter fd + its previous reading.
    struct PerfFd {
        fd: i32,
        prev: PerfRead,
    }

    impl PerfFd {
        /// Open a system-wide raw PMU event.
        /// `config` is the raw event config for the PMU.
        /// `pmu_type` is the type from /sys/bus/event_source/devices/<pmu>/type.
        /// `cpu` is the CPU from the PMU's cpumask (uncore PMUs require
        /// a specific CPU, not -1).
        fn open(pmu_type: u32, config: u64, cpu: i32) -> Option<Self> {
            let attr = PerfEventAttr {
                type_: pmu_type,
                size: PERF_ATTR_SIZE,
                config,
                flags: PERF_FLAG_DISABLED,
                read_format: PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING,
                ..Default::default()
            };

            // pid=-1 → system-wide; cpu from cpumask; PERF_FLAG_FD_CLOEXEC=8
            let fd = unsafe {
                libc::syscall(
                    libc::SYS_perf_event_open,
                    &attr as *const PerfEventAttr,
                    -1i32, // pid: system-wide
                    cpu,   // cpu: from PMU cpumask
                    -1i32, // group_fd
                    8u64,  // PERF_FLAG_FD_CLOEXEC
                )
            };
            if fd < 0 {
                return None;
            }
            let fd = fd as i32;

            // Enable the counter.
            const PERF_EVENT_IOC_ENABLE: u64 = 0x2400;
            let _ = unsafe { libc::ioctl(fd, PERF_EVENT_IOC_ENABLE as libc::c_ulong, 0u64) };

            Some(Self {
                fd,
                prev: PerfRead::default(),
            })
        }

        /// Read the current cumulative counter value.
        fn read(&self) -> Option<PerfRead> {
            let mut sample = PerfRead::default();
            let size = std::mem::size_of::<PerfRead>();
            let read = unsafe {
                libc::read(
                    self.fd,
                    (&mut sample as *mut PerfRead).cast::<libc::c_void>(),
                    size,
                )
            };
            (read == isize::try_from(size).ok()?).then_some(sample)
        }

        /// Return a multiplex-corrected event delta.
        fn read_delta(&self) -> Option<(PerfRead, f64)> {
            let current = self.read()?;
            let value = current.value.checked_sub(self.prev.value)?;
            let enabled = current.time_enabled.checked_sub(self.prev.time_enabled)?;
            let running = current.time_running.checked_sub(self.prev.time_running)?;
            if running == 0 {
                return None;
            }
            Some((current, value as f64 * enabled as f64 / running as f64))
        }
    }

    impl Drop for PerfFd {
        fn drop(&mut self) {
            if self.fd >= 0 {
                unsafe { libc::close(self.fd) };
            }
        }
    }

    /// Read a PMU type from /sys/bus/event_source/devices/<name>/type.
    fn read_pmu_type(name: &str) -> Option<u32> {
        let path = format!("/sys/bus/event_source/devices/{name}/type");
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
    }

    /// Read a PMU cpumask from /sys/bus/event_source/devices/<name>/cpumask.
    /// Uncore PMUs (amd_df, uncore_imc) require a specific CPU from this
    /// mask rather than cpu=-1. Returns 0 if the file is missing.
    fn read_pmu_cpumask(name: &str) -> i32 {
        let path = format!("/sys/bus/event_source/devices/{name}/cpumask");
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    /// Read a PMU event's raw config from
    /// /sys/bus/event_source/devices/<pmu>/events/<event>.
    /// Format: "event=0xNN,umask=0xNN" or just "event=0xNN".
    /// Returns None if the file doesn't exist (some PMUs like amd_df
    /// don't expose events/ in sysfs — their encodings come from perf's
    /// JSON metric database and must be hardcoded by the caller).
    fn read_pmu_event_config(pmu: &str, event: &str) -> Option<u64> {
        let path = format!("/sys/bus/event_source/devices/{pmu}/events/{event}");
        let content = std::fs::read_to_string(path).ok()?;
        let mut event_val: u64 = 0;
        let mut umask_val: u64 = 0;
        for part in content.trim().split(',') {
            if let Some(v) = part.strip_prefix("event=") {
                event_val = u64::from_str_radix(v.trim_start_matches("0x"), 16).ok()?;
            } else if let Some(v) = part.strip_prefix("umask=") {
                umask_val = u64::from_str_radix(v.trim_start_matches("0x"), 16).ok()?;
            }
        }
        // config = umask<<8 | event
        Some((umask_val << 8) | event_val)
    }

    fn intel_imc_names() -> Vec<String> {
        let mut names: Vec<_> = std::fs::read_dir("/sys/bus/event_source/devices")
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name == "uncore_imc" || name.starts_with("uncore_imc_"))
            .collect();
        names.sort_unstable();
        names
    }

    /// Build an AMD DF config value from event + umask, respecting the
    /// bit layout: event occupies config bits 0-7, 32-35, 59-60;
    /// umask occupies config bits 8-15.
    const fn amd_df_config(event: u64, umask: u64) -> u64 {
        // event bits 0-7 → config bits 0-7
        let low = event & 0xFF;
        // event bits 8-11 → config bits 32-35
        let mid = (event >> 8) & 0x0F;
        // event bits 12-13 → config bits 59-60
        let high = (event >> 12) & 0x03;
        (umask << 8) | low | (mid << 32) | (high << 59)
    }

    /// Aggregate request-with-data counters exported by Linux perf for Zen 3.
    const AMD_DF_TOTAL_EVENTS: [u64; 8] = [
        amd_df_config(0x07, 0x38),
        amd_df_config(0x47, 0x38),
        amd_df_config(0x87, 0x38),
        amd_df_config(0xc7, 0x38),
        amd_df_config(0x107, 0x38),
        amd_df_config(0x147, 0x38),
        amd_df_config(0x187, 0x38),
        amd_df_config(0x1c7, 0x38),
    ];

    /// DRAM read/write bandwidth counter. Auto-detects AMD vs Intel PMU.
    /// Returns `None` if no suitable PMU is available.
    pub struct DramBwCounter {
        // AMD Zen 3: 8 aggregate channel fds, each tick = 64 B.
        // Intel: 1 read + 1 write fd, each tick = 64 B.
        read_fds: Vec<PerfFd>,
        write_fds: Vec<PerfFd>,
        total_fds: Vec<PerfFd>,
        // Scale factor: multiply raw delta sum by this to get bytes.
        scale: f64,
        sampled_at: Instant,
    }

    impl DramBwCounter {
        /// Try to create a DRAM bandwidth counter.
        /// Detects the platform and opens the appropriate PMU events.
        #[must_use]
        pub fn new() -> Option<Self> {
            // Try AMD first: amd_df aggregate DRAM channel data.
            if let Some(counter) = Self::new_amd() {
                return Some(counter);
            }
            // Try Intel: uncore_imc with cas_count_read + cas_count_write.
            if let Some(counter) = Self::new_intel() {
                return Some(counter);
            }
            None
        }

        /// AMD Zen 3 exposes only aggregate request-with-data counters.
        fn new_amd() -> Option<Self> {
            let pmu_type = read_pmu_type("amd_df")?;
            let cpu = read_pmu_cpumask("amd_df");
            let mut total_fds = Vec::with_capacity(8);
            for &config in &AMD_DF_TOTAL_EVENTS {
                let fd = PerfFd::open(pmu_type, config, cpu)?;
                total_fds.push(fd);
            }
            // Each tick = 64 bytes of DRAM data.
            let scale = 64.0;
            Some(Self {
                read_fds: Vec::new(),
                write_fds: Vec::new(),
                total_fds,
                scale,
                sampled_at: Instant::now(),
            })
        }

        /// Intel: open cas_count_read + cas_count_write on uncore_imc.
        /// Each tick = 64 bytes (one cache line / DRAM beat).
        /// For multi-socket, perf expands to all uncore_imc instances,
        /// but we open one fd per event (system-wide covers all).
        fn new_intel() -> Option<Self> {
            let names = intel_imc_names();
            if names.is_empty() {
                return None;
            }
            let mut read_fds = Vec::with_capacity(names.len());
            let mut write_fds = Vec::with_capacity(names.len());
            for name in names {
                let pmu_type = read_pmu_type(&name)?;
                let cpu = read_pmu_cpumask(&name);
                let read_config = read_pmu_event_config(&name, "cas_count_read")?;
                let write_config = read_pmu_event_config(&name, "cas_count_write")?;
                read_fds.push(PerfFd::open(pmu_type, read_config, cpu)?);
                write_fds.push(PerfFd::open(pmu_type, write_config, cpu)?);
            }
            // Each tick = 64 bytes.
            let scale = 64.0;
            Some(Self {
                read_fds,
                write_fds,
                total_fds: Vec::new(),
                scale,
                sampled_at: Instant::now(),
            })
        }

        /// Read DRAM read and write bandwidth in bytes/sec since the last call.
        /// Returns `None` if the read fails.
        pub fn read_bytes_per_sec(&mut self) -> Option<(Option<f64>, Option<f64>, f64)> {
            if !self.total_fds.is_empty() {
                let (delta, samples) = sample_deltas(&self.total_fds)?;
                let now = Instant::now();
                let elapsed = now.duration_since(self.sampled_at).as_secs_f64();
                if elapsed <= 0.0 {
                    return None;
                }
                commit_samples(&mut self.total_fds, samples);
                self.sampled_at = now;
                return Some((None, None, delta * self.scale / elapsed));
            }
            if self.read_fds.is_empty() || self.write_fds.is_empty() {
                return None;
            }
            let (read_delta, read_samples) = sample_deltas(&self.read_fds)?;
            let (write_delta, write_samples) = sample_deltas(&self.write_fds)?;
            let now = Instant::now();
            let elapsed = now.duration_since(self.sampled_at).as_secs_f64();
            if elapsed <= 0.0 {
                return None;
            }
            commit_samples(&mut self.read_fds, read_samples);
            commit_samples(&mut self.write_fds, write_samples);
            self.sampled_at = now;
            let read = read_delta * self.scale / elapsed;
            let write = write_delta * self.scale / elapsed;
            Some((Some(read), Some(write), read + write))
        }
    }

    fn sample_deltas(fds: &[PerfFd]) -> Option<(f64, Vec<PerfRead>)> {
        let mut total_delta = 0.0;
        let mut samples = Vec::with_capacity(fds.len());
        for fd in fds {
            let (sample, delta) = fd.read_delta()?;
            samples.push(sample);
            total_delta += delta;
        }
        Some((total_delta, samples))
    }

    fn commit_samples(fds: &mut [PerfFd], samples: Vec<PerfRead>) {
        for (fd, sample) in fds.iter_mut().zip(samples) {
            fd.prev = sample;
        }
    }

    impl Default for DramBwCounter {
        fn default() -> Self {
            Self::new().unwrap_or(Self {
                read_fds: Vec::new(),
                write_fds: Vec::new(),
                total_fds: Vec::new(),
                scale: 0.0,
                sampled_at: Instant::now(),
            })
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    /// Stub on non-Linux platforms — DRAM BW is always unsupported.
    pub struct DramBwCounter;

    impl DramBwCounter {
        #[must_use]
        pub fn new() -> Option<Self> {
            None
        }
        pub fn read_bytes_per_sec(&mut self) -> Option<(Option<f64>, Option<f64>, f64)> {
            None
        }
    }

    impl Default for DramBwCounter {
        fn default() -> Self {
            Self
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub use imp::DramBwCounter;
#[cfg(target_os = "linux")]
pub use imp::DramBwCounter;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dram_bw_counter_creates_or_none() {
        // On a machine with perf_event_paranoid=-1 and amd_uncore loaded,
        // this should create a counter. Otherwise it returns None.
        // Either outcome is valid — we just check it doesn't panic.
        let _ = DramBwCounter::new();
    }
}
