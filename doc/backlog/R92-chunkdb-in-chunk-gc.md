<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R92: chunkdb — In-Chunk GC Operations

**Problem**: Shared chunks accumulate unused space as objects are deleted.
Without in-chunk GC, this space is never reclaimed, leading to storage
waste. CROWDB needs localized GC operations confined to individual chunks
to avoid global merge overhead.

**Solution**: Implement in-chunk GC operations (ReclaimStrip, CollapseStrip,
MergeStrips) for shared chunks. Add logical-to-physical offset mapping
to support GC while keeping chunk IDs stable. Add a ChunkDB orphan scanner for
chunks and shared ranges allocated by access uploads that crash or fail before
their complete file/object descriptor is published. R190 intentionally does not
write per-chunk catalog intents or upload-owner records on its write hot path.
The scanner must compare candidates with authoritative published S3 and Iceberg
references and reader protection before reclaiming, and must not infer orphan
status merely from age or a missing intermediate upload record. Account for
in-flight writers and delayed publication so physical ranges are never reused
while a writer or reader can still own them. Report candidate and reclaimed
bytes separately. Include Iceberg MPU Complete's frozen selection payload as
an authoritative reference while completion is in progress: it stores the
selected parts' exact chunk locations, which remain live even if a concurrent
UploadPart replaces the same part number before publication. After publication,
the immutable file descriptor is the authoritative reference.

**Scope**: Placeholder - detailed design to be refined before implementation.
