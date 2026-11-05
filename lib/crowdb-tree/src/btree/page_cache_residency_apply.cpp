// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_cache_residency_internal.h"

#include <cassert>

namespace crowdb::tree
{
void PageCacheResidency::State::proxy(bool added) noexcept
{
    if (added) {
        proxies.fetch_add(1, std::memory_order_relaxed);
    }
    else {
        proxies.fetch_sub(1, std::memory_order_relaxed);
    }
    if (metrics) {
        const bool valid = added ? metrics->add(TreeResourceMetric::ProxyPages, 1)
                                 : metrics->remove(TreeResourceMetric::ProxyPages, 1);
        assert(valid);
        (void)valid;
    }
}

void PageCacheResidency::State::erase_unused(const PageCacheKey &key) noexcept
{
    auto found = bases.find(key);
    if (found != bases.end() && found->second->views == nullptr && found->second->ready == 0) {
        bases.erase(found);
    }
}

void PageCacheResidency::State::unlink(Page &page) noexcept
{
    if (page.cold != 0) {
        proxy(false);
    }
    for (auto &link : page.links) {
        if (link.previous != nullptr) {
            link.previous->next = link.next;
        }
        else {
            link.base->views = link.next;
        }
        if (link.next != nullptr) {
            link.next->previous = link.previous;
        }
        erase_unused(link.key);
    }
}

void PageCacheResidency::State::attach(Page &page) noexcept
{
    for (auto &link : page.links) {
        link.base = bases.find(link.key)->second.get();
        link.page = &page;
        link.next = link.base->views;
        if (link.next != nullptr) {
            link.next->previous = &link;
        }
        link.base->views = &link;
        page.cold += link.base->ready == 0 ? 1 : 0;
    }
    if (page.cold != 0) {
        proxy(true);
    }
}

void PageCacheResidency::State::apply(Update &update) noexcept
{
    // Detach all old links before adding new pages; the prepared base nodes
    // also cover bases whose last old view disappears in this transaction.
    for (const auto &[pid, replacement] : update.pages) {
        auto found = pages.find(pid);
        if (found != pages.end()) {
            unlink(*found->second);
            pages.erase(found);
        }
    }
    while (!update.bases.empty()) {
        bases.insert(update.bases.extract(update.bases.begin()));
    }
    while (!update.pages.empty()) {
        auto node = update.pages.extract(update.pages.begin());
        if (node.mapped()) {
            attach(*node.mapped());
            pages.insert(std::move(node));
        }
    }
}

void PageCacheResidency::State::change(Base &base, bool present) noexcept
{
    for (auto *link = base.views; link != nullptr; link = link->next) {
        auto &page = *link->page;
        if (present) {
            assert(page.cold != 0);
            if (--page.cold == 0) {
                proxy(false);
            }
        }
        else if (page.cold++ == 0) {
            proxy(true);
        }
    }
}

void PageCacheResidency::State::apply(Ready &ready, bool present) noexcept
{
    if (present) {
        bases.insert(ready.bases.extract(ready.bases.begin()));
        auto &base = *bases.find(ready.key)->second;
        if (base.ready++ == 0) {
            change(base, true);
        }
    }
    else {
        auto &base = *bases.find(ready.key)->second;
        assert(base.ready != 0);
        if (--base.ready == 0) {
            change(base, false);
        }
        erase_unused(ready.key);
    }
}

void PageCacheResidency::State::enqueue(Command *command) noexcept
{
    if (!enter()) {
        discard(command);
        return;
    }
    auto *head = incoming.load(std::memory_order_relaxed);
    do {
        command->next = head;
    } while (!incoming.compare_exchange_weak(head, command, std::memory_order_release, std::memory_order_relaxed));
    wake();
    leave();
}

bool PageCacheResidency::State::enter() noexcept
{
    auto value = producers.load(std::memory_order_acquire);
    do {
        if ((value & kClosed) != 0) {
            return false;
        }
    } while (!producers.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));
    return true;
}

void PageCacheResidency::State::leave() noexcept
{
    producers.fetch_sub(1, std::memory_order_acq_rel);
    producers.notify_all();
}

void PageCacheResidency::State::discard(Command *command) noexcept
{
    if (command->kind == Kind::Mapping) {
        auto owner = std::move(static_cast<Update *>(command->context)->queued_owner);
    }
    else {
        auto *ready = static_cast<Ready *>(command->context);
        auto  owner = command->kind == Kind::Ready ? std::move(ready->added_owner) : std::move(ready->removed_owner);
    }
}

void PageCacheResidency::State::wake() noexcept
{
    auto owner = weak_from_this().lock();
    assert(owner);
    progress->request(std::move(owner));
}

void PageCacheResidency::State::step(void *context) noexcept
{
    static_cast<State *>(context)->drain();
}

void PageCacheResidency::State::apply_command(Command *command) noexcept
{
    if (command->kind == Kind::Mapping) {
        auto *update = static_cast<Update *>(command->context);
        auto  owner  = std::move(update->queued_owner);
        if (!clear_requested.load(std::memory_order_acquire)) {
            apply(*update);
        }
    }
    else {
        auto      *ready   = static_cast<Ready *>(command->context);
        const bool present = command->kind == Kind::Ready;
        auto       owner   = present ? std::move(ready->added_owner) : std::move(ready->removed_owner);
        apply(*ready, present);
    }
}

void PageCacheResidency::State::drain() noexcept
{
    // Reverse at most one detached producer batch, over bounded steps. New
    // arrivals cannot extend that batch or keep this worker in one task.
    constexpr size_t kStepCommands = 32;
    if (reversing == nullptr && ordered == nullptr) {
        reversing = incoming.exchange(nullptr, std::memory_order_acquire);
    }
    for (size_t i = 0; i < kStepCommands && reversing != nullptr; ++i) {
        auto *next      = reversing->next;
        reversing->next = ordered;
        ordered         = reversing;
        reversing       = next;
    }
    for (size_t i = 0; i < kStepCommands && reversing == nullptr && ordered != nullptr; ++i) {
        auto *command = ordered;
        if (command->kind == Kind::Mapping) {
            auto *update = static_cast<Update *>(command->context);
            if (update->reset && !pages.empty()) {
                for (size_t removed = 0; removed < kStepCommands && !pages.empty(); ++removed) {
                    auto page = pages.begin();
                    unlink(*page->second);
                    pages.erase(page);
                }
                // Keep this command at the head until old live membership is
                // gone. Ready owners remain physical cache residents, and
                // later generation updates cannot overtake the reset.
                break;
            }
        }
        ordered = ordered->next;
        apply_command(command);
    }
    if (clear_requested.load(std::memory_order_acquire)) {
        for (size_t i = 0; i < kStepCommands && !pages.empty(); ++i) {
            auto page = pages.begin();
            unlink(*page->second);
            pages.erase(page);
        }
    }
    if (reversing != nullptr || ordered != nullptr || incoming.load(std::memory_order_acquire) != nullptr ||
        (clear_requested.load(std::memory_order_acquire) && !pages.empty())) {
        wake();
    }
}
} // namespace crowdb::tree
