<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R95: chunkdb — Chunk Range Delete

**Problem**: Shared chunks need partial deletion capability for individual object deletion. Without range delete, entire shared chunks cannot be reclaimed efficiently.

**Solution**: Define `DeleteChunkRange(chunk_id, offset, size)` in the chunkdb
protocol, client, and server dispatch now. The initial server implementation
returns an explicit not-implemented result without mutation. The full R95
implementation adds range validation, used-bitmap management, idempotency, and
in-chunk GC integration before any caller may treat success as reclamation.

**Scope**: Placeholder - detailed design to be refined before implementation.
