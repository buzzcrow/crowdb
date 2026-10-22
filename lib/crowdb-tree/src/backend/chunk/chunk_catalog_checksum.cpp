// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_catalog_codec.h"
#include "crowdb-common/crc32c.h"

namespace crowdb::tree::detail
{
namespace
{
uint32_t update_u64(uint32_t crc, uint64_t value)
{
    return crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&value), sizeof(value));
}

} // namespace

uint64_t reference_segment_owner(const ChunkManifest &manifest, const ChunkReferenceSegment &segment)
{
    return manifest.format_version == 0 ? manifest.tree_id : segment.owner_tree_id;
}

uint32_t reference_segment_checksum(const ChunkReferenceSegmentImage &segment)
{
    uint32_t crc = update_u64(0, segment.object_id);
    crc          = update_u64(crc, segment.first_ordinal);
    for (const ChunkPageRef &ref : segment.refs) {
        crc                           = update_u64(crc, ref.chunk_id.high);
        crc                           = update_u64(crc, ref.chunk_id.low);
        crc                           = update_u64(crc, ref.offset);
        const uint32_t encoded_length = encoded_page_ref_length(ref);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&encoded_length),
                                            sizeof(encoded_length));
    }
    return crc;
}

uint32_t manifest_checksum(const ChunkManifest &manifest)
{
    uint32_t crc = 0;
    if (manifest.format_version != 0) {
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&manifest.format_version),
                                            sizeof(manifest.format_version));
    }
    crc = update_u64(crc, manifest.tree_id);
    crc = update_u64(crc, manifest.generation);
    crc = update_u64(crc, manifest.owner_epoch);
    crc = update_u64(crc, manifest.logical_size);
    crc = update_u64(crc, manifest.published_at_ms);
    crc = update_u64(crc, manifest.packs_reused);
    crc = update_u64(crc, manifest.pack_bytes_reused);
    if (manifest.format_version >= 4) {
        crc = update_u64(crc, manifest.wal_replay_offset);
    }
    for (const ChunkPagePack &pack : manifest.packs) {
        if (manifest.format_version >= 2) {
            crc = update_u64(crc, pack.owner_tree_id);
        }
        crc                           = update_u64(crc, pack.ordinal);
        crc                           = update_u64(crc, pack.logical_offset);
        crc                           = update_u64(crc, pack.ref.chunk_id.high);
        crc                           = update_u64(crc, pack.ref.chunk_id.low);
        crc                           = update_u64(crc, pack.ref.offset);
        const uint32_t encoded_length = encoded_page_ref_length(pack.ref);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&encoded_length),
                                            sizeof(encoded_length));
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&pack.reused), sizeof(pack.reused));
    }
    for (const ChunkReferenceSegment &segment : manifest.reference_segments) {
        if (manifest.format_version != 0) {
            crc = update_u64(crc, reference_segment_owner(manifest, segment));
        }
        crc = update_u64(crc, segment.object_id);
        crc = update_u64(crc, segment.first_ordinal);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&segment.ref_count),
                                            sizeof(segment.ref_count));
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&segment.checksum),
                                            sizeof(segment.checksum));
        if (manifest.format_version != 0) {
            crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&segment.reused),
                                                sizeof(segment.reused));
        }
    }
    return crc;
}

} // namespace crowdb::tree::detail
