<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console E2E diagnosis

- Reproduce the exact test, then its spec. If isolated execution passes, run
  the original ordered selection and inspect shared state, IDs, ports, and
  leaked processes; compare serial execution before changing code.
- Locate the first divergence with the trace, `error-context.md`, console
  errors, raw APIs, and `stepTimer`. Separate backend mutation from DOM refresh.
- Scope strict locators to the named region. Never hide ambiguity with
  page-level `.first()` or `.last()`.
- Reuse one `APIRequestContext` per poll phase. Poll resulting API state for
  asynchronous services; use an explicit test-mode cadence only when the real
  production interval dominates runtime.
- Verify lifecycle through both API state and the OS process list.

Useful log signals:

- `new standalone instance created`: transport was not shared.
- `no mgmt seeds configured`: invalid empty seed set.
- `no KV servers deployed`: cluster is not initialized.
- `no rpc endpoint resolved` or `no group-0 endpoint found`: no group-0 route.
- `group-0 query failed`: RPC failed and config fallback was used.
- `topology refresh failed` or `topology discovery failed`: verify whether the
  seed is a store listen address or node RPC address.
