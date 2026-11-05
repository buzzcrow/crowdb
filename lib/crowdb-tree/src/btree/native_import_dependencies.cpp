// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "native_import.h"
#include "overflow_backing.h"

namespace crowdb::tree
{
Status attach_native_dependencies(ReadablePageCatalog &catalog, NativeImportLimits limits,
                                  const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    size_t steps = 0;
    for (auto &[pid, primary] : catalog.frames) {
        (void)pid;
        if (primary.type != page_type::kLeafBase || !primary.stored_overflow || primary.stored_overflow->empty()) {
            continue;
        }
        auto backing      = detail::reserved_shared<BackingRef>(metadata, *primary.backing);
        auto dependencies = detail::reserved_shared<OverflowBackingSet>(metadata, metadata);
        for (const auto &reference : *primary.stored_overflow) {
            auto next      = reference.head;
            auto remaining = reference.bytes;
            while (remaining != 0) {
                if (steps++ == limits.max_dependency_steps) {
                    return Status::resource_exhausted("native import dependency work exceeds configured limit");
                }
                const auto found = catalog.frames.find(next);
                if (found == catalog.frames.end() || found->second.type != page_type::kOverflowFrame) {
                    return Status::corruption("native import overflow chain names a missing or invalid frame");
                }
                const auto &frame = found->second;
                if (frame.overflow_bytes == 0 || frame.overflow_bytes > remaining ||
                    (frame.next_overflow == kInvalidPageId) != (frame.overflow_bytes == remaining)) {
                    return Status::corruption("native import overflow topology differs from the stored length");
                }
                dependencies->pages.emplace(next, OverflowBackingPage{.backing       = frame.backing,
                                                                      .next          = frame.next_overflow,
                                                                      .bytes         = frame.overflow_bytes,
                                                                      .frame_bytes   = frame.frame_bytes,
                                                                      .logical_bytes = frame.logical_bytes});
                remaining -= frame.overflow_bytes;
                next = frame.next_overflow;
            }
        }
        backing->overflow = std::move(dependencies);
        primary.backing   = std::move(backing);
    }
    return Status::Ok();
}
} // namespace crowdb::tree
