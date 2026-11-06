# Accounted owner control allocations

This directory starts from bytes 1.11.1. `crowdb-upstream.json` records SHA-256
hashes of the original files. Upstream licenses and tests are retained.

The additive `Bytes::try_from_owner_accounted` constructor accepts an
`OwnerAllocation` implementation. It claims the exact `Layout` of the allocation
containing both the immutable owner and accounting value. Clones and slices
reuse that allocation. The accounting value is moved to a stack guard at final
release; the immutable owner is destroyed at its pinned address, its box is
deallocated, and only then does the guard refund the charge. The guard preserves
that order if owner destruction unwinds. An `AsRef` panic also destroys the
already-owned control allocation.

The original `Bytes::from_owner` behavior is unchanged. The internal vtable gains
an optional accounting callback; existing constructors leave it absent. Conversion from an
accounted owner into mutable storage or a `Vec` performs the same explicit copy
as the upstream owner constructor. Accounting methods must not panic.

`test_accounted_owner` observes the actual global allocator deallocation. It
covers admission refusal, clones, slices, cross-thread final release, conversion
to `Vec`, and panics in owner `AsRef` or destruction. One test owns the allocator
observation slot so independent test execution cannot overwrite its evidence.

The additive owner allocation lease/context APIs let a downstream native or Rust
control allocation use the same admitted budget. A lease retains the immutable
owner without allocating its own control block. Consumers mark successful
allocation and release the lease only after freeing its associated storage.
Borrowed contexts require the immutable byte owner to outlive all their claims.
