<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Hyper Fork Management

CROWDB maintains a Hyper fork to support request-body buffers allocated from a
CROWDB-selected system or registered-memory pool. The fork is included at
`third-party/hyper` as a Git submodule and is compiled as part of the normal
Rust dependency graph.

This document is the source of truth for fork ownership, upstream
synchronization, submodule updates, Cargo integration, validation, and release
recovery. The data-path design remains in the
[Access Server S3 design](../design/accessserver/design-crowdb-access-server-s3.md).

Upstream references:

- [Hyper repository](https://github.com/hyperium/hyper)
- [Hyper releases](https://github.com/hyperium/hyper/releases)
- [Hyper changelog](https://github.com/hyperium/hyper/blob/master/CHANGELOG.md)

## 1. Why CROWDB maintains a fork

Upstream Hyper owns the HTTP/1 receive allocation as an internal `BytesMut`.
The public server API can bound that buffer but cannot select its allocator.
The custom I/O interface receives memory already selected by Hyper, so an I/O
adapter cannot replace it with a CROWDB buffer.

CROWDB needs more control for large PUT requests:

- Allocate body payload into a selected system or registered-memory pool.
- Retain native owner, allocation-base, registration, and key metadata.
- Fill provider-selected payload regions across partial socket reads.
- Transfer body bytes read with the headers as immutable prefetched views.
- Freeze each buffer before it enters chunk, checksum, EC, RPC, or RDMA paths.
- Stop reading when buffer credits are exhausted and propagate backpressure.
- Release or recycle memory when its final asynchronous owner is dropped.
- Measure every unavoidable header-prefix or coalescing copy.

Wrapping an upstream `Bytes` after reception preserves its bytes but cannot
recover the native registration identity that was never used for allocation.
A global allocator also cannot safely choose a per-request registration domain
or attach MR lifetime metadata.

Writing a new HTTP implementation would add request parsing, chunked framing,
trailers, keep-alive, cancellation, timeout, and request-smuggling security work
that is unrelated to CROWDB storage. Moving the server into the C++ RPC stack
would retain buffer control but give up Hyper's mature HTTP implementation and
Rust's async orchestration. A narrow Hyper fork preserves both properties.

GET does not by itself require a fork. Hyper response bodies already accept a
`Buf`, so a native immutable owner can remain alive until the ordinary TCP
write accepts its bytes. The fork is required for control of PUT receive
allocation.

## 2. Why the fork is a submodule

The fork remains a separate repository with upstream Git history. The CROWDB
repository records one exact fork commit through a submodule pointer.

This provides:

- Reproducibility: one CROWDB commit selects one reviewed Hyper source commit.
- Offline builds after checkout: Cargo reads the local path and does not fetch
  the fork during a build.
- Auditable upgrades: a CROWDB diff shows the submodule pointer change, while
  the fork shows the complete upstream and CROWDB patch history.
- Independent maintenance: upstream merges and fork tests do not pollute the
  CROWDB source history.
- Atomic rollout: the submodule pointer, Cargo lockfile, and CROWDB adapter
  changes land together.
- Simple recovery: a follow-up CROWDB change can select the previous tested
  fork commit without rewriting either repository's history.

The costs are an additional repository checkout, detached submodule worktrees,
and a two-repository update procedure. CI and developer setup must initialize
the submodule explicitly.

## 3. Repository and branch model

The organization fork is `https://github.com/buzzcrow/hyper.git`, forked from
`hyperium/hyper`. Its remotes are:

```text
origin    https://github.com/buzzcrow/hyper.git
upstream  https://github.com/hyperium/hyper.git
```

The fork uses these branches and tags:

- `crowdb-main`: protected integration branch used by the CROWDB submodule.
- `sync/vX.Y.Z`: temporary branch for merging and validating an upstream tag.
- `feature/<topic>`: temporary branch for one CROWDB extension change.
- `crowdb-vX.Y.Z-N`: immutable tested tag, where `X.Y.Z` is the upstream base
  and `N` is the CROWDB patch revision on that base.

As of 2026-09-14, the fork's default `master` branch points to upstream commit
`c6dca2078ce223050dc0832be7c9ab07baa6c4bf`; `crowdb-main` has not yet been
created. Create `crowdb-main` from the selected implementation base when the
first CROWDB patch starts. Do not use the mirror `master` branch for CROWDB
changes.

Do not commit CROWDB changes to a mirror of upstream's default branch. Do not
force-push or rebase published `crowdb-main` history. Merge stable upstream tags
with merge commits so later audits can identify both parents.

Keep CROWDB changes as a small ordered patch series:

1. Protocol-neutral body-buffer contracts and body data type.
2. Opt-in HTTP/1 server receive integration.
3. Fork unit, differential, cancellation, and backpressure tests.

Avoid unrelated formatting, renaming, dependency updates, or cleanup. A small
diff is the main control on future merge cost.

## 4. Fork implementation boundary

The extension is compiled for HTTP/1 servers and remains opt-in per request.
Without a provider, the original `BytesMut` receive path and public behavior
remain unchanged.

The fork may change or add code only around:

```text
src/body/
  provider, mutable receive-buffer, and Incoming selection contracts

src/proto/h1/io.rs
  payload-buffer acquisition and socket fill

src/proto/h1/decode.rs
  separation of HTTP framing reads from decoded payload reads

src/proto/h1/conn.rs
  pooled decoded-body frame propagation

src/proto/h1/dispatch.rs
  provider selection, readiness, cancellation, and body delivery

```

The upstream `Incoming<Data = Bytes>` contract remains unchanged. An admitted
HTTP/1 request may install one `Http1BodyReceiveProvider` before its first body
poll. Hyper retains the IO and HTTP decoder. It asks the provider for writable
payload regions, fills them directly, and calls `on_data_ready`; a body prefix
already present after header or chunk-metadata parsing is transferred through
`on_prefetched_data` as an owned `Bytes` view. HTTP/2, Hyper clients, header
parsing, and response writing are not made generic for this feature.

The fork must not depend on CROWDB crates or expose CROWDB chunk, RPC, FFI, or
RDMA types. It defines only safe provider, mutable-receive, frozen-owner, and
`Buf` contracts. The access service supplies the CROWDB adapter.

Allocation credit, free-buffer publication, and wakeup must remain lock-free or
worker-sharded on the receive hot path. Introducing a shared lock requires a
separate contention and ordering review before implementation.

The `bytes` crate is not forked. `Bytes::from_owner` keeps the native owner and
its registration metadata alive while exposing only its immutable payload
view; the provider retains object-scoped control needed to finalize or submit
the associated physical owner.

## 5. Upstream change assessment

Hyper's public major version is stable: 1.0 was released in November 2023 and
the project remains on 1.x. Its HTTP/1 internals are more active. Recent stable
releases moved from v1.9.0 in March 2026 through v1.10 and v1.11 to v1.11.1 in
August 2026.

An audit from v1.9.0 through 2026-09-14 found:

- 13 commits touching HTTP/1 I/O.
- 8 touching the decoder.
- 9 touching dispatch.
- 10 touching the HTTP/1 connection state machine.
- 5 touching `Incoming`.
- 5 touching the HTTP/1 server builder.

Many changes were mechanical, but the interval also contained fixes for body
cancellation, large bodies, buffer limits, trailers, half-close, missed
wakeups, and flushing. These are not changes the fork can safely skip.

The architecture has remained recognizable and has useful seams:

- `MemRead` separates decoding from connection I/O.
- Dispatch explicitly transfers decoded frames to the incoming-body channel.
- The server builder centralizes connection configuration.
- Response data is already generic over `Buf`.

The receive seam is incomplete for CROWDB:

- `Buffered` owns a concrete `BytesMut`.
- `MemRead` returns a concrete `Bytes`.
- The shared `Incoming` fixes its data type to `Bytes` for HTTP/1 and HTTP/2.
- Chunked framing and decoded payload use the same memory-read interface.

This makes an additive pooled HTTP/1 path practical, but makes a global
replacement of `Incoming` or `BytesMut` expensive to merge. Expected routine
maintenance is **Medium**; correctness risk for a bad merge is **High**.

At implementation time, select the latest suitable upstream commit rather than
blindly using the newest published tag. In September 2026, upstream refactored
the `Incoming` channel immediately after v1.11.1. Starting below that refactor
would create a known first-sync conflict in one of the fork's main extension
points.

## 6. Submodule layout and checkout

The CROWDB repository records:

```ini
[submodule "third-party/hyper"]
    path = third-party/hyper
    url = https://github.com/buzzcrow/hyper.git
```

The submodule pointer, not `.gitmodules` `branch`, selects production source.
Do not use `git submodule update --remote` in builds or CI because it makes the
result depend on mutable remote state.

After the organization fork exists, introduce it once from the CROWDB root:

```bash
pixi run -- git submodule add \
    https://github.com/buzzcrow/hyper.git third-party/hyper
pixi run -- git -C third-party/hyper remote add \
    upstream https://github.com/hyperium/hyper.git
pixi run -- git -C third-party/hyper fetch upstream --tags
pixi run -- git -C third-party/hyper switch \
    -c crowdb-main <upstream-base-commit>
pixi run -- git -C third-party/hyper push -u origin crowdb-main
```

The upstream remote is local clone configuration and is not propagated by the
submodule entry. A maintainer adds or verifies it before performing a sync.
The CROWDB introduction change records `.gitmodules`, the initial submodule
pointer, Cargo integration, and `Cargo.lock` together.

Initialize an existing checkout from the CROWDB root:

```bash
pixi run -- git submodule sync --recursive
pixi run -- git submodule update --init --recursive third-party/hyper
pixi run -- git submodule status --recursive
```

A detached HEAD inside `third-party/hyper` is normal for consumers. Before
developing the fork, explicitly switch the submodule worktree to a feature or
sync branch. Never create a fork commit while accidentally detached.

CI fails before compilation when the submodule directory or expected commit is
missing. CI does not repair or advance the pointer automatically.

## 7. Cargo and build integration

Hyper keeps its upstream package name and compatible upstream package version.
Do not rename it to `crowdb-hyper` or add a SemVer prerelease suffix in
`Cargo.toml`; doing so can create a second incompatible Hyper type universe for
`hyper-util`, TLS adapters, or other transitive dependencies. The CROWDB tag
and Git commit identify the fork revision.

The root workspace uses the local path for direct dependencies and patches
crates.io resolution so compatible transitive users select the same package:

```toml
[workspace.dependencies]
hyper = { path = "third-party/hyper", features = [
    "crowdb-body-pool",
    "http1",
    "server",
] }

[patch.crates-io]
hyper = { path = "third-party/hyper" }
```

The Hyper submodule remains its own Cargo workspace and is not added to
CROWDB's workspace `members`. CROWDB applications build it as a path
dependency. `Cargo.lock` is committed whenever its resolution changes.

The ordinary project build therefore includes the pinned fork:

```bash
pixi run build
```

CI checks `cargo tree` and rejects multiple Hyper packages or sources in the
access-server dependency graph. Duplicate versions would make body and service
types incompatible and could silently route some users through upstream Hyper
instead of the fork.

No build script fetches, switches, resets, or updates the submodule. Network
state must never select build input.

## 8. Upstream synchronization

Review upstream monthly, merge every stable release, and process security,
request-smuggling, parser, body, cancellation, and connection-state fixes
immediately.

Start a stable-tag update from the CROWDB root:

```bash
pixi run -- git -C third-party/hyper fetch upstream --tags
pixi run -- git -C third-party/hyper switch crowdb-main
pixi run -- git -C third-party/hyper switch -c sync/vX.Y.Z
pixi run -- git -C third-party/hyper merge --no-ff --no-edit vX.Y.Z
```

Before resolving a conflict, inspect all upstream changes since the previous
base, including apparently mechanical changes in the affected paths. Do not
resolve by restoring a whole CROWDB or upstream version of a file. Reapply the
provider invariant to the new state machine and preserve each upstream fix.

Give extra review to:

- `src/proto/h1/io.rs`
- `src/proto/h1/decode.rs`
- `src/proto/h1/dispatch.rs`
- `src/proto/h1/conn.rs`
- `src/body/`
- `src/server/conn/http1.rs`
- Hyper runtime `Read`, `ReadBuf`, and `Write` contracts

When upstream changes the `Incoming` channel or HTTP/1 decoder structurally,
first port the unmodified upstream behavior, then reapply the pooled path as a
separate change. Never combine an upstream semantic change and a CROWDB design
change without distinct tests and commits.

After validation, merge the sync branch into `crowdb-main`, create the next
`crowdb-vX.Y.Z-N` tag, and push both before updating the CROWDB submodule
pointer. A main-repository commit must never reference a fork commit that is
only present in a developer's local clone.

## 9. Required fork validation

Run commands from the CROWDB root through pixi. At minimum:

```bash
pixi run -- cargo fmt \
    --manifest-path third-party/hyper/Cargo.toml --all -- --check
pixi run -- cargo clippy \
    --manifest-path third-party/hyper/Cargo.toml \
    --all-targets --all-features -- -D warnings
pixi run -- cargo test \
    --manifest-path third-party/hyper/Cargo.toml --all-features
```

The fork CI also follows the feature and platform matrix used by its upstream
base. CROWDB-specific tests must cover:

- Default provider versus upstream behavior for identical HTTP transcripts.
- Content-Length and chunked PUT with partial reads at every framing boundary.
- Body bytes read together with headers and the bounded prefix copy.
- Trailers and `Expect: 100-continue`.
- Pool exhaustion, wakeup, backpressure, and slow clients.
- Handler cancellation before allocation, while filling, and after delivery.
- Peer half-close, connection error, timeout, and shutdown.
- Buffer freeze, last-owner release, and pool lifetime.
- System and registered provider selection before the first body poll.
- HTTP pipelining without assigning bytes to the wrong request or provider.
- Maximum body size, descriptor count, and allocation-credit enforcement.

After fork tests pass, run the CROWDB access-server, chunk-client, RPC, and FFI
tests affected by the change, followed by the normal Rust formatting and clippy
gates. Performance-sensitive revisions also compare upstream/default receive,
system-pool receive, and registered-pool receive while reporting every copied
byte.

## 10. Updating the CROWDB pointer

Only update the main repository after the fork commit is reviewed, tagged,
pushed, and reproducibly testable.

```bash
pixi run -- git -C third-party/hyper fetch origin --tags
pixi run -- git -C third-party/hyper checkout crowdb-vX.Y.Z-N
pixi run -- cargo update -p hyper
pixi run -- cargo tree -d
pixi run build
```

The CROWDB review includes:

- The old and new submodule commits and upstream bases.
- Every CROWDB patch carried by the new fork revision.
- Upstream security and behavior changes in the interval.
- `Cargo.lock` and dependency-tree changes.
- Fork and affected CROWDB test results.
- Copy, allocation, latency, and throughput differences when relevant.

The submodule pointer, dependency changes, compatibility adaptations, and
lockfile update form one coherent CROWDB change. Do not advance the pointer in
an unrelated feature change.

## 11. Recovery and emergency fixes

If a new fork revision fails before release, fix it on the sync branch or
select the previously tested tag before merging the CROWDB pointer update.

If a deployed revision must be withdrawn:

1. Stop advancing the affected fork tag; tags remain immutable.
2. Create a normal CROWDB change selecting the previous tested submodule
   commit, or select a new forward-fix fork tag.
3. Rebuild and run the affected gates.
4. Record whether the failure came from upstream behavior, the pooled receive
   extension, or its CROWDB adapter.

Do not force-move a tag, rewrite `crowdb-main`, or hard-reset either repository.
A security fix that cannot wait for the next upstream tag is cherry-picked to
an emergency branch, tested with the same matrix, merged normally, and released
as the next `crowdb-vX.Y.Z-N` tag.

## 12. Ownership and licensing

Every fork update needs review from the data-access owner and from an owner of
the buffer/FFI or transport code affected by the change. Parser, framing, or
request-smuggling changes require explicit security review.

Hyper remains MIT-licensed. Preserve upstream copyright and license files and
mark CROWDB-specific behavior in code and release notes without replacing
upstream attribution. The Apache-2.0 CROWDB repository consumes the fork as a
separately licensed third-party component.
