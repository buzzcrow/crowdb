// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/config.h"
#include "crowdb-tree/snapshot/native_frame.h"
#include "page_load.h"
#include "snapshot/readable_page_catalog.h"

namespace crowdb::tree
{
struct NativeImportLimits
{
    size_t max_frames           = 1024 * 1024;
    size_t max_dependency_steps = 1024 * 1024;
};

// Upper bounds for one complete immutable import, derived before staging.
struct NativeImportShape
{
    size_t frames            = 0;
    size_t primary_pages     = 0;
    size_t leaf_pages        = 0;
    size_t stored_references = 0;
    size_t max_leaf_entries  = 0;
    size_t dependency_steps  = 0;
    size_t max_frame_bytes   = 0;
    size_t max_key_bytes     = 0;
    size_t max_value_bytes   = 0;
};

[[nodiscard]] std::optional<size_t> native_import_allocation_bound(const NativeImportShape &shape) noexcept;
[[nodiscard]] PageLoadLimits        resident_import_load_limits(PageLoadLimits limits, size_t frame_bytes) noexcept;

Status append_native_frame(ReadablePageCatalog &catalog, uint64_t pid, std::span<const uint8_t> frame,
                           const Config &config, const PageLoadLimits &loads, NativeImportLimits limits,
                           const std::shared_ptr<detail::AllocationReservation> &metadata);
Status build_native_catalog(std::span<const NativeFrame> frames, NativeStreamInfo info, const Config &config,
                            const PageLoadLimits &loads, NativeImportLimits limits,
                            const std::shared_ptr<detail::AllocationReservation> &metadata,
                            std::shared_ptr<ReadablePageCatalog>                 *out);
Status attach_native_dependencies(ReadablePageCatalog &catalog, NativeImportLimits limits,
                                  const std::shared_ptr<detail::AllocationReservation> &metadata);
} // namespace crowdb::tree
