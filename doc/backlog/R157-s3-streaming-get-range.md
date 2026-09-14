<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R157: access server / S3 — Streaming GetObject and single-range reads

## Problem

Large GET and range GET must stream chunk-client native buffers without
collecting an object or copying every block into `Bytes`. The server also needs
an exact release boundary: slow or disconnected clients must retain buffers
safely but cannot cause unbounded prefetch or destroy a pool before outstanding
views return.

The read and ownership design is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §§4 and 5.

## Solution

1. Resolve and retain one immutable generation, then translate an absent range
   or one RFC byte range into an exact chunk-client interval. Reject multi-range
   syntax as unsupported because the S3 `GetObject` API defines only one
   contiguous range; future scatter/gather reads belong to Dataset.
2. Add an immutable native owner and offset/length view implementing `Buf`.
   Its drop releases one C++ ref; the pool remains owned until all outstanding
   buffers return. A chain drops fully consumed front owners promptly.
3. Implement a concrete Hyper response body that fetches the next chunk window
   only when polled and while response-byte credits exist. Set exact body size
   and HTTP range headers before yielding payload.
4. Force the tested HTTP/1 vectored-write strategy for plaintext TCP. Hyper
   advances buffers by bytes accepted by the socket and drops exhausted data;
   ordinary TCP may then recycle it without waiting for remote ACK.
5. On cancellation, timeout, read error, or disconnect, drop queued response
   owners and stop chunk prefetch. Return no fallback generation mid-response.

## Dependencies

- Depends on R152, R153, and chunk-client reader APIs.
- Reuses R155 owner/view/chain primitives where landed; otherwise implements
  their read-only subset without changing ownership semantics.
- R161 supplies service-wide response credits and R163 supplies range errors.
- R170 owns cuObject negotiation, remote-buffer ownership, and RDMA completion;
  none of those are implemented by this requirement.

## Acceptance

- Given full, first, middle, tail, empty, suffix, open-ended, unsatisfiable, and
  multi-range requests across chunk and EC boundaries, when GET runs, assert
  exact status, headers, length, and bytes. Invariant: HTTP range semantics
  equal the retained generation interval. E2E test.
- Given a slow client and an object larger than memory, when GET streams, assert
  outstanding native bytes stay within credits and chunk reads pause.
  Invariant: client speed bounds retained storage buffers. Integration test.
- Given partial vectored writes, disconnect, timeout, and cancellation, when
  Hyper advances or drops the body, assert each native owner returns exactly
  once after its last consumer. Invariant: no buffer is reused early or leaked.
  Integration test.
- Given an overwrite during GET, when the stream crosses later chunks, assert
  it continues reading the originally resolved generation. Invariant: one GET
  never mixes object generations. E2E test.
- Given a valid multipart byte-range header, when GET parses it, assert the
  stable S3 error is returned before any chunk read. Invariant: CROWDB does not
  invent a non-compatible S3 multi-range extension. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run test-rpc-ffi`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
