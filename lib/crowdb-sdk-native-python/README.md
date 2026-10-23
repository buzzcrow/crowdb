<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Native Dataset Python

`crowdb_dataset_native` embeds the trusted main-workspace storage runtime.
Internal management and storage endpoints must be reachable. It accepts no
HTTP token and performs no automatic HTTP fallback. The independently packaged
`crowdb_dataset` remains the portable HTTP SDK.

Build and install a wheel in an isolated consumer:

```sh
pixi run -- python tools/sdk/native_package.py --destination /tmp/crowdb-native-python
```

The Linux wheel bundles native dependencies except `libibverbs.so.1` and
`librdmacm.so.1`; install those host libraries with matching RDMA providers.
It retains the host-specific `linux` wheel tag. RDMA additionally requires a
working host HCA and device access. `NativeRuntime(seeds)` explicitly selects
TCP; `options` accepts JSON containing `data_rdma`, `metadata_rdma` and
`aggregate_rdma` overrides using the Rust `RdmaConfig` schema. Set `enabled` and
`allow_tcp_fallback` separately per group; metadata RDMA requires its control/data
class. Options also accept `held_bytes`, `producer_bytes` and `requests`. Runtime lifetime
belongs to the caller. Create a new runtime in each worker process; inherited
clients are rejected after fork.

`runtime.dataset(name)` uses the shared default `dataset-ns` namespace, matching
HTTP and Rust. Pass `namespace=[...]` to select an explicit namespace.

```python
import json
import crowdb_dataset_native as native

runtime = native.NativeRuntime(["http://127.0.0.1:7001"])
dataset = runtime.dataset("training")
metadata = json.loads(dataset.call('{"op":"open"}'))
# IDs passed to read/upload methods are immutable bytes; JSON IDs are arrays
# of 16 bytes, matching the shared Rust authority schema.
batch = dataset.read_batch(snapshot_id, [list(b"sample")], ["image"])
while (lease := batch.next()) is not None:
    consume(lease.field, lease.logical_offset, lease.to_bytes())
batch.close()
dataset.close()
runtime.close()
```

- `Batch.header` is immutable JSON. `next()` yields a `Lease`, or `None` after
  verified EOF. It never acknowledges processing. A batch supports one concurrent
  consumer; independent batches can run concurrently. Blocking calls release
  Python execution ownership so completion workers and other callers progress.
- `Lease.slice(start, end)` retains the same immutable allocation. Capacity and
  registration identity are inspectable. `to_bytes()` explicitly copies into
  Python-owned memory; that new allocation is outside SDK held-buffer accounting.
  No Python buffer-protocol pointer or new unsafe lifetime adapter is exposed.
- `Batch.cancel()`/`close()` interrupts production without acknowledging. Held
  leases remain readable after cancellation, facade close and runtime close.
  Closing a facade rejects new calls only on that facade; existing batches and
  other facades retain their independently owned lifetimes.
- `runtime.status()` reports request and memory admission, high water and
  compatibility-copy bytes. `runtime.topology(refresh=True)` observes metadata
  and data transport state; these hints cannot override routing authority.
- `dataset.call(json, timeout=30)` uses a tagged `op` command and the existing
  Rust Dataset JSON schema. Commands cover create/open/snapshot/head/commit
  status, composition begin/status/edit/evaluate/prepare/publish/abandon/cleanup,
  prepare/resume/acknowledge/close read, and upload create/complete/status/abort.
  `upload_location` returns the authority's opaque location for a composition
  field. No command is retried automatically.
- `prepare_read` accepts `spec: SessionSpecV2` and `page`; continue its returned
  page until complete. `next_batch(read_id)` fetches the issued batch. Call
  `acknowledge` with the previously acknowledged cursor and `Batch.header.next`
  only after application processing succeeds. Split/worker binding uses the
  same shared session service as Rust and HTTP.
- `upload_part(upload_id, index, bytes)` copies Python input once into final
  transport frame slots across allocations of at most 1 MiB and freezes them
  before submission. A logical upload part can still contain 4 MiB. Publication uses
  upload-managed compositions and preserves durable attempt identity.
- Typed exceptions include capacity, integrity, cancellation, closed runtime,
  deadline, conflict and uncertain mutation. An uncertain mutation does not
  authorize replay with a new operation identity.

`tests/real_consumer.py` validates the installed binding against real services;
`tests/workload.py` includes explicit Python byte-copy costs. The comparison is
run with `pixi run -- python tools/sdk/dataset_workload.py` after building the
current shared service image. It uses one snapshot for HTTP and native clients,
then validates native publication after storage restart.
Use `--semantics-only` to run installed-binding semantics and restart separately;
that mode produces no performance comparison.
