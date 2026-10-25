<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowDB native SDK

`crowdb-sdk-native` embeds trusted internal S3 and Dataset access through Chunk-KV and
ChunkIO/DiskIO. It belongs to the main Rust workspace and links the native RPC
stack. Supply management seeds, a tenant and per-operation context. The
embedding process has the same internal-service trust as Access Server; this
API performs no SigV4 verification or user-secret lookup.

The [native runtime design](../../doc/design/access-server/design-crowdb-native-sdk.md)
defines ownership and routing. The separately packaged
[Dataset HTTP SDK](../../sdk/README.md) is the portable Rust/Python distribution.

## Connect and read

The embedding application supplies Tokio. Clone one `ClientRuntime` across
handles to share connection groups, topology caches and budgets:

```rust
use std::num::NonZeroU64;
use std::time::{Duration, Instant};
use crowdb_access_s3::metadata::TenantId;
use crowdb_sdk_native::{ClientRuntime, OperationContext, RuntimeConfig};
use crowdb_sdk_native::s3::{GetOptions, S3Client};

async fn read_object(seeds: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RuntimeConfig::new(seeds);
    config.data_rdma = Some(crowdb_rpc_ffi::RdmaConfig {
        enabled: false,
        ..Default::default()
    });
    let runtime = ClientRuntime::connect(config).await?;
    let client = S3Client::new(runtime.clone(), TenantId::new(b"tenant".to_vec())?)?;
    let context = OperationContext::new(
        NonZeroU64::new(1).unwrap(),
        Instant::now() + Duration::from_secs(10),
    );
    let mut object = client.get_object(context, b"bucket", b"key", &GetOptions::default()).await?;
    while let Some(lease) = object.body.next().await? {
        let bytes = lease.as_slice();
        // Consume bytes here, or retain the lease until an asynchronous copy completes.
        assert_eq!(bytes.len(), lease.metadata().length);
    }
    assert!(object.body.is_complete());
    runtime.shutdown().await?;
    Ok(())
}
```

- Give each operation a nonzero request ID and deadline; its cancellation
  token belongs to that operation. GET remains lazy until the body is polled.
- Only `next() == Ok(None)` establishes verified completion. A valid prefix
  does not establish full-read success. Terminal errors remain sticky on
  subsequent polls; dropping a body early cancels it.
- `GetOptions` supports conditions and a logical half-open `ByteRange`.
  `resolve_object` captures one published generation; `get_resolved` reads
  that selection after an overwrite without looking the mutable key up again.
- The S3 facade supports bucket operations, HEAD/GET, PUT, paged listing,
  ordered batch deletion, copy and multipart begin/parts/list/complete/abort.
  Typed S3 failures and `NotModified` do not require HTTP/XML parsing.

## Produce directly into native buffers

- `put_vec` accepts bounded ordinary input and charges the Vec's actual
  allocation capacity. Large input can instead be fed through `begin_put`
  with `write`, using leases from `runtime.resources()` and views of at most
  1 MiB per call. A small view can retain a larger allocation and its charge.
- `allocate_producer(capacity)` returns unique writable storage from the
  runtime's native allocator. Under RDMA this can be registered memory;
  inspect the resulting registration ID rather than inferring it from config.
- `initialize_framed(length, initial, fill)` exposes logical payload regions
  within their final storage frame slots. `write_framed` consumes the frozen
  owner; callers cannot mutate submitted bytes. Capacity must include storage
  framing. Each 64 KiB frame carries at most `MAX_FRAME_PAYLOAD_BYTES` bytes;
  each submitted physical buffer is bounded by 1 MiB. Finish the logical
  payload directly in these regions to avoid a later payload staging copy.
- `prepare_framed(bytes)` copies existing input once into a chain of final
  allocations of at most 1 MiB. `FrozenObjectBuffer::concatenate` combines
  prepared owners without copying; all interior frames must be full and all
  owners must share one runtime budget. Credits follow each allocation until
  its last view is released. A chain reports registration ID zero if its
  allocations have different registration identities.
- `write`/`write_framed` input acceptance is separate from object visibility.
  `finish` waits for durability, checksums and publication. Multipart complete
  binds the selected part generations and preserves repeat-completion results.
- `UncertainMutation` means a final outcome is unknown. Preserve request and
  operation identity; do not blindly replay the mutation with a new identity.
- Small writer pools can retain input owners for open-strip repair after a
  PUT completes. Pool drain/seal releases them; budget this lifetime.

## Dataset

- Attach `dataset::DatasetClient::new(&runtime, identity)` with an explicit
  `DatasetIdentity`. Its Dataset writer pool shares transport and admission
  with S3 while preserving the Dataset chunk type. Dropping the facade does
  not close the shared runtime.
- `read_batch` binds an immutable snapshot, sample IDs and ordered projection.
  `ReadBatch.header` describes values, missing fields and tombstones;
  `body.next()` returns a field index and retained `BufferLease`. Each field
  has its own logical offset. Read through verified EOF before reporting a
  complete result; no contiguous field assembly is required.
- `bind_read(context, ReadSpec)` returns a portable `ReadBinding.cursor` pinned
  to Snapshot and optional Split revision. `next_batch(context, &cursor)` replays
  that position; save `header.next` only after processing succeeds. `None` means
  end. There is no server-side progress, ack, TTL, resume or close operation.
- Reads check snapshot deletion state at admission and perform no persistent
  reader registration, renewal or release. Deletion rejects new requests and
  defers cleanup for twenty minutes. Admitted transfers may finish; overdue reads
  can fail when their remaining data is reclaimed. EOF/cancel/drop return local
  request credits without a remote cleanup wait. Caller-held buffers remain
  valid and charged independently.
- Upload parts accept runtime-owned `BufferLease` or `FrozenObjectBuffer`.
  Complete verifies stored parts before resource preparation. `prepare_snapshot`
  accepts a client-selected Snapshot ID and complete `CompositionRequest`, writes
  the immutable result and claims its final upload owners. `prepare_split_revision`
  accepts complete frozen definitions. Repeat the same ID/input after an uncertain
  outcome; no editing or evaluation progress is stored by these helpers.
- `update_snapshot_head` and `update_split_head` publish pointers separately with
  explicit base conditions. A complete resource is readable by ID before it becomes
  a head. `Error::SnapshotHeadConflict` and `Error::SplitHeadConflict` preserve the
  observed pointer and generation; use that observation for a deliberate retry
  without another head query. Helpers never rebase or retry automatically.
- Advanced authority operations and inspection run through
  `control(context, callback)`. Its scoped authority, uploads and identity share
  the same admission and typed contracts. Keep the complete input, resource ID
  and original publication conditions when a write's outcome is uncertain.
- The optional [native Python extension](../crowdb-sdk-native-python/README.md)
  calls this same data plane. It is separate from the portable HTTP package.

## Budgets, topology and shutdown

- Defaults: 1024 requests, 64 MiB of producers, 256 MiB of caller-held buffers.
  These limits are independent of the transport's registered-memory budget,
  posted receive pools, SQ depth, in-flight byte window and writer policy.
- Shared allocation views charge capacity once until the final owner releases
  it. Finishing an RPC or stream does not return caller-held credits. A read
  waiting for held capacity resumes after release or returns its deadline/
  cancellation outcome. Unknown external owners use an accounted bounded copy.
- `resources().status()` reports live charges, high water and compatibility
  copy bytes. `topology()` exposes current transport counters and separately
  versioned catalog/disk observations. `refresh_topology(context)` explicitly
  refreshes authorities; it does not create a globally atomic snapshot.
- `object_locality` resolves bounded extents, layout revisions and fragment
  locations for a selected object. Observations aid scheduling and prefetch;
  current routing/fencing still decides execution. Old views grant no placement
  authority and cannot force an expired layout into a read.
- `connect` and `adopt_clients` own worker shutdown. `from_clients` borrows
  existing groups and leaves their shutdown to the original owner.
  `shutdown()` closes admission, drains owned writers and joins owned RPC
  workers. Cancellation of one shutdown waiter does not cancel cleanup.
- Leases remain readable after shutdown until their final release. For a GPU
  host-copy operation, retain the host lease until copy completion. The base
  API exposes host memory; it does not provide GPU pointers or GPUDirect.

## TCP and RDMA

`RuntimeConfig::data_rdma` selects the DiskIO group; `chunk_kv_transport` owns
metadata transport policy. `None` retains the existing service defaults.
Explicitly disabled RDMA uses TCP. An enabled `RdmaConfig` supplies device,
source address, queue depths, post batches, receive class and registration
budget; use `with_control_data_capacity` to select a supported control/data
class. Setup fallback follows `allow_tcp_fallback`. An accepted or partial
RDMA read fails terminally instead of appending a TCP suffix or retrying another
replica; ambiguous mutations are never replayed by the SDK.

Eligible receive allocations reach `BufferLease` without a second SDK payload
copy. Ordinary unregistered input, EC construction/reconstruction, alignment
and control decoding can still require copies. The opaque RPC C ABI uses
single final release; releasing a freed handle again is invalid.

## Verification

```sh
pixi run -- cargo test -p crowdb-sdk-native --all-targets
pixi run clean-env && pixi run -- python e2e-test/run.py --layer s3 --scope s3.native --concurrency 1 --cpus 4 --memory-mib 8192 --image crowdb-node:dev
pixi run clean-env && pixi run -- python tools/rdma/run-native-s3.py --address 10.200.1.1 --device rocep35s0f0
```

Build `crowdb-node:dev` from current server sources first. The explicit HCA
profile uses one host HCA, twelve independent service containers and native/
unprivileged OCI consumers. Services have separate persistent directories,
loopback endpoints and CM ports. It validates mixed sizes, registered producers,
held-capacity pressure, lease lifetime and restart; it is a local-loop functional
profile, not a cross-host performance measurement. The TCP profile includes a
separate HTTP/native semantic comparison and OCI consumers with no RDMA device.
