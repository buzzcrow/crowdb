// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "page_cache_residency_internal.h"
#include "published_state.h"

#include <algorithm>
#include <cassert>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
thread_local std::shared_ptr<AllocationBudget> node_budget;

struct NodeHeader
{
    AllocationLease credit;
};

size_t header_bytes(size_t alignment)
{
    return (sizeof(NodeHeader) + alignment - 1) & ~(alignment - 1);
}
} // namespace

PageCacheNodeResource::Scope::Scope(std::shared_ptr<AllocationBudget> budget)
    : previous_(std::exchange(node_budget, std::move(budget)))
{
}

PageCacheNodeResource::Scope::~Scope()
{
    node_budget = std::move(previous_);
}

void *PageCacheNodeResource::do_allocate(size_t bytes, size_t alignment)
{
    alignment         = std::max(alignment, alignof(NodeHeader));
    const auto offset = header_bytes(alignment);
    if (bytes > SIZE_MAX - offset) {
        throw std::bad_alloc();
    }
    AllocationLease credit(node_budget, bytes + offset, AllocationKind::kMetadata);
    auto *allocation = static_cast<std::byte *>(std::pmr::get_default_resource()->allocate(bytes + offset, alignment));
    std::construct_at(reinterpret_cast<NodeHeader *>(allocation), NodeHeader{std::move(credit)});
    return allocation + offset;
}

void PageCacheNodeResource::do_deallocate(void *pointer, size_t bytes, size_t alignment)
{
    alignment             = std::max(alignment, alignof(NodeHeader));
    const auto offset     = header_bytes(alignment);
    auto      *allocation = static_cast<std::byte *>(pointer) - offset;
    auto      *header     = reinterpret_cast<NodeHeader *>(allocation);
    auto       credit     = std::move(header->credit);
    std::destroy_at(header);
    std::pmr::get_default_resource()->deallocate(allocation, bytes + offset, alignment);
}

bool PageCacheNodeResource::do_is_equal(const std::pmr::memory_resource &other) const noexcept
{
    return this == &other;
}

PageCacheResidency::State::State(std::shared_ptr<TreeResourceMetrics> resources) : metrics(std::move(resources))
{
}

PageCacheResidency::PageCacheResidency(std::shared_ptr<TreeResourceMetrics>     resources,
                                       const std::shared_ptr<AllocationBudget> &metadata, detail::FlushRuntime *runtime,
                                       std::shared_ptr<detail::FlushRuntime::Lane> lane)
    : state_(detail::reserved_shared<State>(metadata, std::move(resources)))
{
    if (runtime == nullptr || !lane) {
        throw std::invalid_argument("page residency requires a host runtime and lane");
    }
    const auto status =
        runtime->reserve_progress(std::move(lane), &State::step, state_.get(), metadata, &state_->progress);
    if (!status.ok()) {
        throw std::bad_alloc();
    }
}

PageCacheResidency::~PageCacheResidency()
{
    clear();
}

std::shared_ptr<PageCacheResidency::Update>
PageCacheResidency::prepare(std::span<const PageReplacement>         pages,
                            const std::shared_ptr<AllocationBudget> &metadata) const
{
    auto                         update = detail::reserved_shared<Update>(metadata, state_);
    PageCacheNodeResource::Scope scope(metadata);
    for (const auto &edit : pages) {
        if (edit.expected == edit.replacement) {
            continue;
        }
        State::prepare_page(*update, edit.page_id, edit.replacement, metadata);
    }
    return update;
}

void PageCacheResidency::publish(std::shared_ptr<Update> update) noexcept
{
    while (update) {
        auto  following      = std::move(update->following);
        auto *command        = &update->command;
        auto  state          = update->state;
        update->queued_owner = std::move(update);
        state->enqueue(command);
        update = std::move(following);
    }
}

std::shared_ptr<PageCacheResidency::Ready>
PageCacheResidency::prepare_ready(const PageCacheKey &key, const std::shared_ptr<AllocationBudget> &metadata) const
{
    auto                         result = detail::reserved_shared<Ready>(metadata, state_);
    PageCacheNodeResource::Scope scope(metadata);
    result->key = key;
    result->bases.emplace(key, detail::reserved_shared<State::Base>(metadata));
    return result;
}

void PageCacheResidency::ready(const std::shared_ptr<Ready> &ready) noexcept
{
    ready->added_owner = ready;
    ready->state->enqueue(&ready->added);
}

void PageCacheResidency::evict(const std::shared_ptr<Ready> &ready) noexcept
{
    ready->removed_owner = ready;
    ready->state->enqueue(&ready->removed);
}

void PageCacheResidency::clear() noexcept
{
    if (!state_->enter()) {
        return;
    }
    state_->clear_requested.store(true, std::memory_order_release);
    state_->wake();
    state_->leave();
}

void PageCacheResidency::close_and_drain() noexcept
{
    auto value = state_->producers.fetch_or(State::kClosed, std::memory_order_acq_rel) | State::kClosed;
    while ((value & ~State::kClosed) != 0) {
        state_->producers.wait(value, std::memory_order_acquire);
        value = state_->producers.load(std::memory_order_acquire);
    }
    state_->clear_requested.store(true, std::memory_order_release);
    state_->wake();
    // Internal reruns remain enabled until every bounded clear step finishes.
    state_->progress->drain();
    state_->progress->close_and_drain();
    state_->bases.clear();
}

uint64_t PageCacheResidency::proxy_pages() const noexcept
{
    return state_->proxies.load(std::memory_order_relaxed);
}

const std::shared_ptr<TreeResourceMetrics> &PageCacheResidency::resources() const noexcept
{
    return state_->metrics;
}

size_t PageCacheResidency::page_allocation_bound(size_t refs) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<Update>() +
                                   detail::shared_allocation_bound<State::Page>() + 256)
        .add_product(refs, sizeof(State::Link) + detail::shared_allocation_bound<State::Base>() + 256)
        .value()
        .value_or(SIZE_MAX);
}

size_t PageCacheResidency::ready_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<Ready>() + detail::shared_allocation_bound<State::Base>() + 256;
}

size_t PageCacheResidency::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<PageCacheResidency>() + detail::shared_allocation_bound<State>() +
           detail::FlushRuntime::progress_allocation_bound();
}
} // namespace crowdb::tree
