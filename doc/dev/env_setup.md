<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Dev Environment Setup

One-time host setup for perf counters and CROWDB benchmarks on Ubuntu
24.04. Two target machines:

- **AMD** — Ryzen 9 5950X (Zen 3). DRAM bandwidth via `amd_df` PMU +
  8 raw `dram_channel_data_controller_*` aggregate events (request-with-
  data, read + write combined). Each tick = 64 B.
- **Intel** — DRAM bandwidth via `uncore_imc` PMU + `cas_count_read` /
  `cas_count_write` events, each tick = 64 B. Some platforms (Skylake-X,
  HEDT/server) expose per-channel `uncore_imc_0` … `uncore_imc_N` instead
  of a single `uncore_imc`; both forms are supported.

## Run

```bash
sudo bash tools/setup-perf.sh
```

The script auto-detects AMD vs Intel, applies every setting below, and
prints `[ok]` / `[FAIL]` per step with the expected value. Safe to
re-run. Exits non-zero on the first failure.

## What it sets

### 1. perf tooling

Installs `linux-tools-$(uname -r)` if `perf` is missing. If the stub
`/usr/bin/perf` from `linux-tools-generic` is present but broken,
symlinks `/usr/local/bin/perf` to the real
`/usr/lib/linux-tools/$(uname -r)/perf`.

### 2. `perf_event_paranoid = -1`

Writes `/etc/sysctl.d/99-perf.conf` and applies it. This is the single
most common perf failure on Ubuntu 24: the default `1` blocks **all**
CPU/uncore events for non-root users — not just DRAM bandwidth. Every
counter reads `<not supported>`:

- `perf stat -e cycles true` → `<not supported>`
- `perf stat -e cache-misses true` → `<not supported>`
- `perf stat -M nps1_die_to_dram ...` → all 8 channels `<not supported>`
- `perf stat -e uncore_imc/cas_count_read/ ...` → `<not supported>`

So this step is required for **any** perf-based profiling, not only the
DRAM-bandwidth metric. `-1` = allow (almost) all events by all users.
Do **not** set this on production hosts.

### 3. AMD: `amd_uncore` module → `amd_df` PMU

On Zen CPUs the Data Fabric PMU is driven by the `amd_uncore` module.
Ubuntu 24 ships it as `=m` but does **not** auto-load it, so `amd_df`
is absent from `/sys/bus/event_source/devices/` and perf reports
`Cannot find PMU 'amd_df'`. The script runs `modprobe amd_uncore` and
writes `/etc/modules-load.d/amd-uncore.conf` so it survives reboot.

No BIOS change is needed. Zen exposes DF counters by default; the only
gates are this module + paranoid.

### 4. Intel: `uncore_imc` PMU

Intel integrated memory controller counters are exposed by the in-tree
`uncore_imc` PMU, usually built-in on Ubuntu 24 (`=y`, no module load
needed). The script runs `modprobe intel_uncore` as a no-op fallback in
case the kernel built it as `=m`.

Some platforms (Skylake-X, many HEDT/server parts) expose per-channel
`uncore_imc_0` … `uncore_imc_N` devices instead of a single `uncore_imc`.
The script accepts either form. `perf list` and `perf stat -e uncore_imc/…/`
expand the base PMU name across all per-channel instances automatically.

### 5. Non-zero read check

Runs a 1-second `perf stat` against the host's bandwidth metric/event
and verifies the value is non-zero and not `<not supported>`:

- AMD: `perf stat -a -M nps1_die_to_dram -- sleep 1`
- Intel: `perf stat -a -e 'uncore_imc/cas_count_read/' -- sleep 1`

Both PMUs are system-wide uncore counters and cannot attach to a single
process. The `-a` (system-wide) flag is **required** — without it every
counter reads `<not supported>` (the metric does not auto-fallback to
system-wide). On AMD you will also see `50%` multiplexing: Zen 3 exposes
8 `dram_channel_data_controller_*` events but only 4 hardware counters,
so perf time-shares them.

## How CROWDB reads DRAM bandwidth in-process

CROWDB services (`crowdb-cli`, `crowdb-kv-server`, `crowdb-diskdb`,
`crowdb-chunkdb`, `crowdb-diskio`) read the **same hardware uncore
counters** directly via `perf_event_open` — no `perf` CLI or `pcm`
needed at runtime. Two implementations exist (kept in sync):

- Rust: `lib/crowdb-common/rust/src/metrics/perf.rs` — `DramBwCounter`.
- C++: `lib/crowdb-common/cpp/src/metrics/system_metrics.cpp` —
  `SystemCollector::DramBwImpl`.

Both auto-detect the platform (AMD tried first, then Intel) and open
system-wide fds at startup. If any fd fails to open (missing module,
paranoid too high), the counter is `None` and the metrics log prints
`bw_*_mib=unsupported`. After `setup-perf.sh` has run, non-root
processes can open the counters and the `mem_*_mib` columns in
regression TSVs show real DRAM bandwidth.

### AMD in-process encoding

Opens 8 raw `amd_df` events with configs `0x3807, 0x3847, 0x3887,
0x38c7, 0x100003807, 0x100003847, 0x100003887, 0x1000038c7` —
`umask=0x38` (request-with-data) across all 8 channel select values.
Each tick = 64 B; read and write are **not** separated (total only).

### Intel in-process encoding

Opens `cas_count_read` (event=0x04, umask=0x03 → config=0x304) and
`cas_count_write` (event=0x04, umask=0x0c → config=0xc04) on each
`uncore_imc` / `uncore_imc_N` instance. Each tick = 64 B; read and
write are reported separately.

## Common errors and what they mean

- `Cannot find PMU 'amd_df'. Missing kernel support?` — `amd_uncore`
  not loaded. Run the script (step 3).
- `unknown term 'nps1_die_to_dram' for pmu 'amd_df'` — wrong syntax.
  `nps1_die_to_dram` is a **metric**, not a raw event. Use
  `perf stat -M nps1_die_to_dram`, never `-e amd_df/nps1_die_to_dram/`.
- Every event shows `<not supported>` — paranoid still `1`. Run the
  script (step 2) or `sudo sysctl -w kernel.perf_event_paranoid=-1`.
- `bw_*_mib=unsupported` in a CROWDB metrics log — the in-process
  `perf_event_open` call failed. Same root cause as above: run
  `setup-perf.sh`, then restart the service. On Intel, also check that
  `uncore_imc` or `uncore_imc_N` devices exist under
  `/sys/bus/event_source/devices/`.
- `Error: failed to open tracing events directory` from `perf list` —
  tracefs is root-only (`/sys/kernel/tracing` is `drwx------`). This
  only affects tracepoint **listing**, not PMU hardware counters. Use
  `sudo perf list` to silence it, or ignore it.

## Reading the counters from the CLI

AMD — `nps1_die_to_dram` sums `dram_channel_data_controller_0..7` and
scales by `6.1e-5 MiB` per tick. Output is already in MiB. This is the
`perf` CLI metric; the in-process code uses the raw events with a
64 B/tick scale instead (see above).

Intel — each `cas_count_*` tick = 64 B. Bandwidth in B/s:
`count * 64 / elapsed_seconds`. For multi-socket boxes, sum across all
`uncore_imc_*` instances (perf expands the PMU name to all instances by
default).

Wrap a CROWDB benchmark in bandwidth measurement:

```bash
perf stat -a -M nps1_die_to_dram -- \
  pixi run -- crowdb-cli bench kv write        # AMD
perf stat -a -e 'uncore_imc/cas_count_read/,uncore_imc/cas_count_write/' -- \
  pixi run -- crowdb-cli bench kv write        # Intel
```

With paranoid `= -1` set, no `sudo` is needed for `perf`.

## CROWDB benchmark prereqs

Per `AGENTS.md`, run everything through `pixi`. Build the binaries each
sentinel needs first:

```bash
# KV / RPC / diskdb / chunkdb sentinels
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server
# chunkio write sentinel also needs diskdb + chunkdb + diskio (C++)
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server \
    -p crowdb-diskdb -p crowdb-chunkdb
pixi run build-cpp
```

Regression sentinels under `tools/`:

- `tools/bench-kv-read-regression.sh`
- `tools/bench-kv-write-regression.sh`
- `tools/bench-kv-scan-regression.sh`
- `tools/bench-rpc-regression.sh`
- `tools/bench-diskdb-regression.sh`
- `tools/bench-chunkdb-regression.sh`
- `tools/bench-chunkio-write-regression.sh`
