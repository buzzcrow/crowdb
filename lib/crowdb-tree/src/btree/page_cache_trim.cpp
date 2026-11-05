// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_cache_internal.h"

namespace crowdb::tree
{
void SharedPageCache::trim(PageCacheTrim &request) noexcept
{
    auto *head = state_->trims.load(std::memory_order_relaxed);
    do {
        request.next = head;
    } while (
        !state_->trims.compare_exchange_weak(head, &request, std::memory_order_release, std::memory_order_relaxed));
    state_->wake();
}

size_t SharedPageCache::State::trim_ready(PageCacheKind kind, size_t max_pages) noexcept
{
    size_t retained = 0;
    for (const auto &entry : entries) {
        retained += static_cast<size_t>(entry.phase == Phase::Ready && entry.key.kind == kind);
    }
    size_t evicted = 0;
    while (retained > max_pages) {
        Entry *oldest = nullptr;
        for (auto &entry : entries) {
            if (entry.phase == Phase::Ready && entry.key.kind == kind &&
                (oldest == nullptr || entry.priority > oldest->priority ||
                 (entry.priority == oldest->priority && entry.touched < oldest->touched))) {
                oldest = &entry;
            }
        }
        if (oldest == nullptr) {
            break;
        }
        discard(*oldest);
        --retained;
        ++evicted;
    }
    resume_requested = evicted != 0 || resume_requested;
    return evicted;
}

void SharedPageCache::State::drain_trims() noexcept
{
    auto          *list    = trims.exchange(nullptr, std::memory_order_acquire);
    PageCacheTrim *ordered = nullptr;
    while (list != nullptr) {
        auto *next = list->next;
        list->next = ordered;
        ordered    = list;
        list       = next;
    }
    while (ordered != nullptr) {
        auto *request      = ordered;
        ordered            = request->next;
        const auto evicted = trim_ready(request->kind, request->max_pages);
        if (request->complete != nullptr) {
            request->complete(request->context, evicted);
        }
    }
}
} // namespace crowdb::tree
