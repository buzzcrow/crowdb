<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC transport tests

This tool measures the real C++ echo transport independently of application
cluster lifecycle. `run.py` starts one echo server on an available port, runs the
client, and waits for graceful server shutdown. It owns only that child process.

```bash
pixi run cargo build -p crowdb-rpc-test
pixi run cmake --build lib/crowdb-rpc/build --target crowdb-rpc-fb-server -j 4
pixi run python tools/rpc-test/run.py --client target/debug/crowdb-rpc-test \
  --log-dir .crowdb-runtime/artifacts/rpc-example --duration-secs 5 --mode coroutine
```

- `--mode tokio` selects the other client scheduler.
- `--io-engines`, `--io-workers`, `--loader-num`, `--connections` and
  `--enable-nagle` retain the transport regression sweep controls.
- JSON stdout reports measured operations, errors, duration and latency.
  Server and client diagnostics remain under `--log-dir`.
- `pixi run cargo test -p crowdb-rpc-test` exercises both schedulers against the
  real echo server and validates resource rejection before process creation.
- `pixi run bash tools/benchmark/bench-rpc-regression.sh` builds and runs the
  standard sweep. `RPC_BENCH_DURATION` and `RPC_BENCH_CASES` select a focused run.
- The sanitizer regression builds the server and Rust FFI with address
  sanitization, then supplies `--asan-runtime` and `--asan-log-prefix`. Only the
  two measured child processes receive the sanitizer environment. Reports from
  either process fail the regression.

The client binary can also attach to an explicitly managed echo server through
`--server-port`. Application workloads continue to use `crowdb-cli --cluster
NAME bench ...` against an already running cluster.
