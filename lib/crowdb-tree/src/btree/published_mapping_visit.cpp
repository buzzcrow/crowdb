// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "published_state.h"

namespace crowdb::tree
{
void MappingView::visit_directory(const Directory &directory, unsigned shift, uint64_t prefix, void *context,
                                  Visitor visitor)
{
    for (size_t i = 0; i < kFanout; ++i) {
        const auto page_prefix = prefix | (i << shift);
        if (shift > 6) {
            if (directory.children[i]) {
                visit_directory(*directory.children[i], shift - 6, page_prefix, context, visitor);
            }
        }
        else if (directory.segments[i]) {
            const auto &segment = *directory.segments[i];
            for (size_t j = 0; j < kFanout; ++j) {
                if (segment.pages[j]) {
                    visitor(context, page_prefix | j, segment.pages[j]);
                }
            }
        }
    }
}

void MappingView::visit(void *context, Visitor visitor) const
{
    if (root_) {
        visit_directory(*root_, 24, 0, context, visitor);
    }
}

bool MappingView::visit_from(uint64_t &next_pid, size_t max_slots, void *context, Visitor visitor) const
{
    if (!root_) {
        next_pid = kPageLimit;
        return true;
    }
    for (size_t visited = 0; visited < max_slots && next_pid < kPageLimit; ++visited) {
        const auto *directory = root_.get();
        unsigned    shift     = 24;
        while (shift > 6) {
            const auto &child = directory->children[(next_pid >> shift) & (kFanout - 1)];
            if (!child) {
                break;
            }
            directory = child.get();
            shift -= 6;
        }
        if (shift > 6) {
            next_pid = ((next_pid >> shift) + 1) << shift;
            continue;
        }
        const auto &segment = directory->segments[(next_pid >> 6) & (kFanout - 1)];
        if (!segment) {
            next_pid = ((next_pid >> 6) + 1) << 6;
            continue;
        }
        const auto  pid  = next_pid++;
        const auto &page = segment->pages[pid & (kFanout - 1)];
        if (page) {
            visitor(context, pid, page);
        }
    }
    return next_pid >= kPageLimit;
}
} // namespace crowdb::tree
