// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_cache_internal.h"

#include <cassert>

namespace crowdb::tree
{
SharedPageCache::State::Request *SharedPageCache::State::claim(bool accept) noexcept
{
    if (!closed.load(std::memory_order_acquire)) {
        const auto first = next_request.fetch_add(1, std::memory_order_relaxed) % requests.size();
        for (size_t offset = 0; offset < requests.size(); ++offset) {
            auto &request = requests[(first + offset) % requests.size()];
            bool  free    = false;
            if (request.occupied.compare_exchange_strong(free, true, std::memory_order_acquire)) {
                if (accept) {
                    request.owner = weak_from_this().lock();
                    assert(request.owner);
                    waiters.fetch_add(1, std::memory_order_relaxed);
                    ordinary_waiters.fetch_add(1, std::memory_order_relaxed);
                }
                return &request;
            }
        }
    }
    rejected.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
}

void SharedPageCache::State::submit(Request *request) noexcept
{
    request->registered_at_ns = tree_metrics_now_ns();
    enqueue(&request->command);
}

void SharedPageCache::State::enqueue(Command *command) noexcept
{
    auto *head = incoming.load(std::memory_order_relaxed);
    do {
        command->next = head;
    } while (!incoming.compare_exchange_weak(head, command, std::memory_order_release, std::memory_order_relaxed));
    wake();
}

void SharedPageCache::State::wake() noexcept
{
    auto owner = weak_from_this().lock();
    assert(owner);
    if (!running.exchange(true, std::memory_order_acq_rel)) {
        drain();
    }
}

void SharedPageCache::State::drain() noexcept
{
    for (;;) {
        auto    *list    = incoming.exchange(nullptr, std::memory_order_acquire);
        Command *ordered = nullptr;
        while (list != nullptr) {
            auto *next = list->next;
            list->next = ordered;
            ordered    = list;
            list       = next;
        }
        while (ordered != nullptr) {
            auto *next = ordered->next;
            if (ordered->kind == CommandKind::Request) {
                auto *request = static_cast<Request *>(ordered->context);
                try {
                    process(request);
                }
                catch (const std::bad_alloc &) {
                    deliver(request, Status::resource_exhausted());
                }
                catch (...) {
                    deliver(request, Status::internal_error());
                }
            }
            else {
                loaded(static_cast<Entry *>(ordered->context));
            }
            ordered = next;
        }
        drain_trims();
        if (evict_requested.exchange(false, std::memory_order_acq_rel)) {
            evict_ready();
        }
        if (resume_requested) {
            resume_requested = false;
            resume_pending();
        }
        // A producer that observed running publishes its command before its
        // exchange. Acquire that handoff before rechecking the incoming lists.
        running.exchange(false, std::memory_order_acq_rel);
        if ((incoming.load(std::memory_order_acquire) == nullptr && trims.load(std::memory_order_acquire) == nullptr &&
             !evict_requested.load(std::memory_order_acquire)) ||
            running.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
    }
}
} // namespace crowdb::tree
