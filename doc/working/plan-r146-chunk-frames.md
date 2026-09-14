<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R146 Chunk Frames Completion Plan

## Current audit

- [x] Public Rust/C++ frame codecs, locations, shared vectors, repo and
  stream framing, frame finalization, and verified client reads are present.
- [x] Tree packs use variable public frames and compact locations; a large
  pack remains one location and is read only after all frames verify.
- [x] An Active owner renews its one `FinalizeChunk` task every twelve minutes
  even when it has no writes: repo and stream workers use their idle timer;
  the tree RPC transport owns a lock-free background cadence and self-fence.
- [~] Re-audit each persistent owner (repo, stream, tree) for an owned,
  restart-safe heartbeat and local self-fence rather than treating write-path
  renewal as a substitute. The implementation is present; acceptance coverage
  still needs a controllable cadence test.
- [ ] Restore R146 cleanup only after the cadence and all acceptance evidence
  are complete. The backlog entry and final cleanup must not be removed while
  this plan has unchecked work.

## Next implementation sequence

1. Add an explicit owner liveness-renewal operation that does not advance a
   chunk cursor, including a durable-task recovery path after chunkdb restart.
2. Add owner-side cadence/self-fencing for repo, stream, and tree without a
   hot-path lock; test idle renewal, claimed-task conflict, and restart.
3. Run the requirement gates, fix in-scope failures, then restore permanent
   documentation cleanup and remove this plan.
