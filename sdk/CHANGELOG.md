<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Dataset SDK releases

## 0.1.0

- Initial independently packaged Rust Dataset REST v1 client and Python native
  wrapper, with synchronous/asynchronous file, batch and control operations.
- Pooled connections, configurable deadlines and bounded request/byte admission;
  stable mutation IDs, scoped cancellation and integrity-checked streams.
- `DatasetIterable` preserves its import name and yields batches requiring
  explicit acknowledgement. Server-side worker partition precedes payload reads.
- Verified wheels: Linux x86_64, CPython 3.14 and 3.14t. Source distribution
  builds outside the server repository; wheel installation requires no compiler.
- Real Rust/Python acceptance covers multi-part publication, retained payloads,
  restart recovery and acknowledged/unacknowledged read progress.
- Local cache/checkpoint staging, direct chunk transports and RDMA are deferred.
  No benchmark driver, baseline or performance claim is included.
