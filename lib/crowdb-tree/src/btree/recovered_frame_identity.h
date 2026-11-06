// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/backing_ref.h"
#include "crowdb-tree/maptable/frame_page.h"

namespace crowdb::tree
{
// Legacy local page builders stamped the not-yet-assigned page id into their
// frames. Recovery's validated anchor/segment or captured overflow descriptor
// supplies the real identity. Chunk packs and imported resident frames always
// require a concrete id; an unrelated concrete id is never a legacy marker.
[[nodiscard]] inline bool recovered_frame_identity_matches(const BackingRef &backing, page_type type, uint64_t stored,
                                                           uint64_t expected) noexcept
{
    const bool legacy_type =
        type == page_type::kLeafBase || type == page_type::kInnerBase || type == page_type::kOverflowFrame;
    return expected != kInvalidPageId && (stored == expected || (backing.storage == BackingStorage::kDurableBlob &&
                                                                 legacy_type && stored == kInvalidPageId));
}
} // namespace crowdb::tree
