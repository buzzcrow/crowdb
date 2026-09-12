---
name: review
description: Review a CROWDB diff when code review or pre-push review is requested; not an automatic coding handoff step.
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Review

Review the diff, callers, and tests. Report concrete findings before style.
Apply repository code, test-placement, and module-ownership rules without
loading another skill solely for review.

Treat `propose`, `accept`, `learn`, `kv_get`, `kv_put`, `kv_delete`,
and `kv_batch_write` as hot paths:

- Borrow instead of copying; justify non-trivial clones.
- Pre-size collections or use stack storage.
- Do not add locks. `std::sync::Mutex` is limited to lifecycle work;
  `tokio::sync::Mutex` requires state held across `.await` and user approval.
- Avoid nested `Arc`; retain it only when data outlives its parent. Never move
  a borrow into a task that may outlive it.

For crowdb-rpc flatbuffers, read fields from the frame. Do not create owned
intermediates or use allocating accessors on hot paths. Shared wrappers belong
in `crowdb-protocol`; copy payload only beyond frame lifetime. See
`doc/design/rpc/design-crowdb-rpc.md` section 6.

Check correctness invariants, errors, shutdown, async lifetimes, status
exposure, dead code, dependencies, visibility, `Debug`, module cohesion,
integration-test placement, and performance regressions. Avoid non-test panic.
Prefer enum dispatch unless implementations are open across crates. Use
`get_or_try_init` for fallible initialization.

For module cohesion, flag implementation left at roots, catch-all folders,
inconsistent Rust/C++ ownership, or unclear names. Recommend a concrete owner,
but keep relocation findings scoped to the changed area.

`async_fn_in_trait` is the only documented clippy exception. Add any new
exception here before suppressing it.
