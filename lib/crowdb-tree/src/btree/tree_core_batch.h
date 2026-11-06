// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "apply_frontier.h"
#include "crowdb-tree/btree/tree.h"
#include "inline_function.h"
#include "runtime/tree_operation_lifetime.h"
#include "source_admission.h"

namespace crowdb::tree
{
class ApplyProgress;
class SourceRotation;

struct CoreWriteInput
{
    Slice   key;
    Slice   value;
    buffer *movable = nullptr;
    uint8_t flags   = 0;
    bool    encoded = false;
};

using CoreWriteReader = detail::InlineFunction<CoreWriteInput(size_t)>;

// Validate both the primary leaf cell and a possible inner separator before
// admitting an input whose configured key/inline limits may exceed a frame.
[[nodiscard]] bool primary_write_fits(const Config &config, size_t key_bytes, size_t value_bytes,
                                      bool tombstone) noexcept;

Status apply_source_batch(const Config &config, SourceAdmission &sources, ApplyFrontier &frontier,
                          TreeOperationLifetime &lifetime, size_t max_value_bytes,
                          std::optional<uint64_t> supplied_slot, std::atomic<uint64_t> &automatic, size_t count,
                          const CoreWriteReader &read, ApplyProgress *progress = nullptr,
                          SourceRotation *rotation = nullptr);
} // namespace crowdb::tree
