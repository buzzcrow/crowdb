---
name: review
description: Review CROWDB changes for correctness, module cohesion, and hot-path cost.
subagent: true
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Review

Review the diff, callers, and tests. Report concrete findings before style.
Apply `/coding` layout and test rules.

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

Review directory ownership as part of module cohesion:

- Flag new implementation files left at a crate or library root when an owning
  domain module exists or the concept warrants a submodule.
- Flag large flat directories that the change expands instead of grouping the
  touched files by domain responsibility. Recommend a concrete destination,
  using `crowdb-kv/src/{cluster,paxos,wal}/` and
  `crowdb-tree/src/{btree,maptable,memtable,snapshot,backend}/` as reference
  shapes.
- Audit the changed crate/library root as a whole. Flag related touched files
  left behind when the patch creates or extends their owning domain folder;
  public headers still need visible subsystem ownership.
- Verify Rust module roots use `foo.rs` + `foo/` with deliberate re-exports;
  verify C++ public include paths and private source paths express the same
  subsystem ownership where applicable.
- Flag unclear or inconsistent domain abbreviations and source/include folder
  names that do not match.
- Flag catch-all folders and redundant forwarding headers that obscure the
  canonical owner. Accept a root umbrella or compatibility shim only when its
  public purpose is explicit.
- Keep relocation findings scoped to the changed area; do not require an
  unrelated repository-wide move in the same patch.

`async_fn_in_trait` is the only documented clippy exception. Add any new
exception here before suppressing it.
