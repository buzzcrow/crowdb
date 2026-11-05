// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "logical_page.h"

#include <compare>

namespace crowdb::tree
{
enum class PageCacheKind : uint8_t { Leaf, Inner };

struct PageCacheKey
{
    PageCacheKind           kind            = PageCacheKind::Leaf;
    uint64_t                backend         = 0;
    uint64_t                address         = 0;
    uint64_t                length          = 0;
    uint64_t                content         = 0;
    uint64_t                generation      = 0;
    uint64_t                checksum        = 0;
    uint32_t                encoding        = 0;
    uint64_t                base            = 0;
    uint64_t                base_generation = 0;
    uintptr_t               legacy_owner    = 0;
    BackingStorage          storage         = BackingStorage::kDurableBlob;
    std::array<uint64_t, 8> chunk{};
    auto                    operator<=>(const PageCacheKey &) const = default;
};

inline PageCacheKey page_cache_key(PageCacheKind kind, const BaseRef &base)
{
    const auto &backing = *base.backing;
    return {.kind            = kind,
            .backend         = backing.backend_id,
            .address         = backing.address,
            .length          = backing.length,
            .content         = backing.content_id,
            .generation      = backing.generation,
            .checksum        = backing.checksum,
            .encoding        = backing.encoding,
            .base            = kind == PageCacheKind::Leaf ? base.identity : 0,
            .base_generation = kind == PageCacheKind::Leaf ? base.generation : 0,
            .legacy_owner    = backing.backend_id == 0 ? reinterpret_cast<uintptr_t>(backing.owner.get()) : 0,
            .storage         = backing.storage,
            .chunk           = backing.chunk ? chunk_backing_identity(*backing.chunk) : std::array<uint64_t, 8>{}};
}
} // namespace crowdb::tree
