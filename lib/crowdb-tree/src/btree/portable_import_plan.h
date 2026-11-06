// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "native_import.h"

namespace crowdb::tree
{
// Allocation-free first pass over the exact fixed selection. Packing follows
// the builder's byte, entry-count, and key-byte limits; no payload is retained.
class PortableImportPlan
{
  public:
    PortableImportPlan(const Config &config, PageLoadLimits loads) : config_(config), loads_(loads)
    {
        shape_.max_frame_bytes = config.frame_bytes;
        for (const auto *key : {&config.key_range.start(), &config.key_range.end()}) {
            if (*key) {
                shape_.max_key_bytes = std::max(shape_.max_key_bytes, (*key)->size());
            }
        }
    }

    Status                                         append(size_t key_bytes, size_t value_bytes, bool tombstone);
    [[nodiscard]] std::optional<NativeImportShape> finish() const noexcept;

  private:
    const Config     &config_;
    PageLoadLimits    loads_;
    NativeImportShape shape_;
    size_t            leaf_used_  = 0;
    size_t            leaf_keys_  = 0;
    size_t            leaf_count_ = 0;
};
} // namespace crowdb::tree
