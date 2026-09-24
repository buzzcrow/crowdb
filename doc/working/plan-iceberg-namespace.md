# Iceberg Namespace Plan

Upstream: [R179](../backlog/R179-access-iceberg-namespace.md).
Current integration: [functional catalog plan](plan-iceberg-functional-catalog.md).

Goal: finish namespace acceptance without weakening identity, admission or recovery.

## Completed summary

- Independent writer role, bounded multipart identifiers/properties, authority and
  name mappings, parent-scoped scans and conditional stale-index cleanup.
- Durable payloads and create/update/drop journals, reserve-before-parent-admit,
  shared bounded helping, exact terminal outcomes and response-loss recovery.
- Qualified reads, authenticated pagination, bounded complete-response spooling,
  HTTP mutations and retry ledgers, background recovery and index repair.
- Table-create admission is integrated, including namespace-drop races and
  interrupted immediate/staged publication. Rename-in is not yet implemented.
- Official PyIceberg CRUD passes against two listeners before and after native
  Chunk-KV/listener restart in a separate functional profile. A retained namespace
  and its exact properties survive the restart and resolve through both listeners.
- The original clear/restart test passes with its 500-ms bound retained across
  repository reconstruction and fault injection. Its client checks remain reads;
  the complete CRUD assertions execute in the separate test, not disappear.
  Both real-stack tests pass serially and under default test concurrency
  (2026-09-24). No production performance
  changes or added client retries were made. R179 still awaits rename-in coverage.

## Remaining execution

- [ ] **Rename-in admission seam**: after R181 rename exists, test destination
  reservation, parent drop, lost head-CAS reply, recreated names and recovery.
  Preserve table-create/drop regression coverage rather than replacing it.
  Files: namespace probes/helping, table lifecycle and native/library tests.
- [ ] **Final gates and closure**: map remaining R179 acceptance to executable
  evidence, run tests/fmt/clippy, update affected permanent design, then remove
  the requirement, index entry and this plan. Do not close on a partial CRUD pass.

## Resolved latency blocker and performance evidence

R177 OI-1 is confirmed: functional acceptance and performance are separate.
The dedicated CRUD fixture uses the existing runtime request ceiling of 300,000 ms
with delegation disabled because it tests namespace-only behavior. Existing raw
HTTP client five-second timeouts and CRUD assertions are unchanged. This ceiling
is not a latency claim. The maintenance fixture retains exactly 500 ms, including
its reconstructed/fault-injecting repositories; default repository bounds must
not accidentally expand it through the new monotonic clear-bound behavior.

Historical diagnostics are retained for the eventual performance backlog:

- Recorded command:
  `pixi run clean-env && RUST_LOG=crowdb_access_server=debug CROWDB_RUNTIME_ROOT="$PWD/.crowdb-runtime/ephemeral/iceberg-e2e" CROWDB_ICEBERG_E2E_PYTHON="$PWD/.pixi/envs/iceberg-e2e/bin/python" pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_full_stack_test -- --nocapture`.
- Setup: two real listeners, durable Chunk-KV, persisted 500-ms request bound
  inherited from clear/restart tests, and PyIceberg CRUD without test-side retries.
- First divergence: root or nested create returns 503. Recorded server diagnostics
  identify request deadline exhaustion, not validation/publication corruption.
- Five diagnostic/fix runs: initial CRUD integration; structured deadline errors;
  per-phase timing (about 45–75 ms per durable phase, body read about 100 μs);
  authoritative read-before-put for immutable payloads; no-op checks before
  terminal marker/reservation cleanup.
- Redundant writes were removed without changing publication CAS, and focused
  loss/replay tests pass. One complete CRUD pass was followed by another client's
  root create exceeding 500 ms. Recorded failure:
  `catalog_recovery_survives_real_chunk_kv_restart` at its official-client check,
  `ServiceUnavailableError: ServiceUnavailableException: Catalog is not ready`.
- Instrumentation was removed. The split fixtures now pass; they do not establish
  that all namespace mutations meet 500 ms. Broader critical-path/batching work
  goes into the functional plan's performance inventory and later consolidated
  backlog; fix obvious bugs only with measured root causes and regression tests.
- The maintenance run logs rejected repair attempts for synthetic reserved
  mappings without journals seeded by `verify_name_index`; the separate CRUD
  fixture did not show these errors. Do not treat this fixture setup as evidence
  of production corruption or hide the diagnostics to improve the result.

## Verification

- Library: `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- Server: `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`.
- Official client: `pixi run -e iceberg-e2e test-pyiceberg-e2e`.
- Gates: `pixi run -- cargo fmt --all -- --check`; `pixi run rs-lint`;
  `pixi run -- cargo clippy -p crowdb-access-server --features iceberg-e2e --all-targets -- -D warnings`.
- Use an isolated runtime root for native tests, preserving unrelated persistent
  port claims. Do not clean another running fixture's state.
