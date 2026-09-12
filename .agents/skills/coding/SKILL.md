---
name: coding
description: Apply CROWDB conventions while changing production or test code; not for read-only questions or reviews.
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Coding

Inspect the touched module and its callers. Read `doc/doc_index.md` and the one
matched design section only when the change alters documented behavior,
crosses module boundaries, or leaves architectural intent unclear.

## Logging

- `critical!`: broken invariant; include `next step:`.
- `error!`: recoverable failure and its handling.
- `warn!`: actionable anomaly or transient failure.
- `info!`: lifecycle and topology changes only; never per request.
- `debug!`: requests, hot-path decisions, and routine detail.
- Do not leave production `trace!` calls.

Use structured Paxos fields when available: `store_id`, `group_id`,
`replica_l_id`, `replica_r_id`, `slot`, `ballot`. Instrument public Paxos
object methods so fields propagate. Defaults remain file=`debug`, console=`info`.

## Tests and status

- Put integration tests in `<crate>/tests/*_test.rs`; helpers belong in
  `tests/common/` and use `Test*` names. Do not add inline test modules,
  `tests/testkit/`, or fixtures under `src/`.
- Keep Paxos tests under `lib/crowdb-kv/tests/paxos_test/`, entered by
  `lib/crowdb-kv/tests/paxos_test.rs`.
- Use `CROWDB_TEST_LOG=1` and `tests/common/logging.rs` for test tracing.
- Expose useful new state through `StatusLevel` or the relevant `*Status`
  type in `lib/crowdb-kv/src/cluster/status.rs`; shared wire types live in
  `lib/crowdb-protocol/src/mgmt.rs`.
- Track new TODO/FIXME items in `doc/todo_code.md`; remove both together.

## Layout

- Keep roots for entry points, facades, ABI boundaries, configuration, and
  genuinely shared primitives. Put implementation under its owning product
  domain; avoid catch-all `handlers`, `types`, and `utils` folders.
- Use the non-`mod.rs` layout: `foo.rs` + `foo/`; `foo.rs` contains module docs,
  declarations, and deliberate re-exports. Keep internal children private
  unless callers need them.
- Mirror public C++ subsystem ownership under `include/<library>/` and `src/`;
  keep private headers with their implementation. Avoid forwarding headers
  unless compatibility requires them.
- Use established domain names consistently. Keep one nameable responsibility
  per module and separate domain invariants from runtime wiring.
- Improve nearby layout only when cohesive with the requested change.
- Keep code files near 300 lines; split before adding to one over 1000. Keep
  functions near 40 lines, at most 80 for orchestration; split over 150.
- Use the narrowest visibility. Test hooks require `test-util` and `_for_tests`.
- Do not add lint suppressions.

For visible console UI or Playwright work, also apply `/console-ui-e2e`.
