// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "captured_frames_internal.h"

namespace crowdb::tree
{
void CapturedFrameStream::State::load_base(const BaseLoadRequest &request)
{
    if (!loader) {
        finish(Status::unavailable("snapshot needs a captured leaf loader"), true);
        return;
    }
    auto event  = detail::reserved_shared<Load>(limits.encode.metadata);
    event->base = request.base_index;
    load        = event;
    ++counters.loads;
    auto       self     = shared_from_this();
    const auto physical = stack.back().page->leaf->bases.at(request.base_index);
    loader->load_leaf(physical, [self, event](Status status, auto leaf) {
        if (event->claimed.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        event->status = std::move(status);
        event->leaf   = std::move(leaf);
        event->ready.store(true, std::memory_order_release);
        self->wake();
    });
}

void CapturedFrameStream::State::load_inner()
{
    const auto &backing = stack.back().page->inner_backing;
    if (!backing || !backing->readable()) {
        finish(Status::corruption("captured inner has no reloadable backing"), true);
        return;
    }
    if (!loader) {
        finish(Status::unavailable("snapshot needs a captured index loader"), true);
        return;
    }
    auto event      = detail::reserved_shared<Load>(limits.encode.metadata);
    event->is_inner = true;
    load            = event;
    ++counters.loads;
    auto self = shared_from_this();
    loader->load_inner(backing, [self, event](Status status, auto inner) {
        if (event->claimed.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        event->status = std::move(status);
        event->inner  = std::move(inner);
        event->ready.store(true, std::memory_order_release);
        self->wake();
    });
}

void CapturedFrameStream::State::loaded()
{
    auto event = std::move(load);
    if (!event->status.ok()) {
        finish(std::move(event->status), true);
        return;
    }
    if ((event->is_inner && !event->inner) || (!event->is_inner && !event->leaf)) {
        finish(Status::corruption("snapshot loader returned no page"), true);
        return;
    }
    auto &path = stack.back();
    auto  page = detail::reserved_shared<PublishedPage>(limits.encode.metadata, *path.page);
    if (event->is_inner) {
        page->inner = std::move(event->inner);
    }
    else {
        auto leaf = detail::reserved_shared<PageState>(limits.encode.metadata, *page->leaf, limits.encode.metadata);
        const auto maximum = leaf->bases.at(event->base).max_slot;
        if (maximum) {
            for (size_t i = 0; i < event->leaf->count(); ++i) {
                if (CellView(event->leaf->cell(i)).slot() > *maximum) {
                    finish(Status::corruption("snapshot base exceeds its captured slot bound"), true);
                    return;
                }
            }
        }
        leaf->bases[event->base].resident = std::move(event->leaf);
        page->leaf                        = std::move(leaf);
    }
    path.page = std::move(page);
}
} // namespace crowdb::tree
