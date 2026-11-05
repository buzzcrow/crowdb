// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/page_load_internal.h"
#include "btree/page_load_leaf.h"
#include "btree/page_load_lifetime.h"

#include <atomic>
#include <stdexcept>

namespace crowdb::tree
{
void PageLoader::load_overflow(std::shared_ptr<const BackingRef> /*primary*/, uint64_t head, uint64_t length,
                               ValueComplete complete)
{
    load_overflow(head, length, std::move(complete));
}

void PageLoader::load_overflow(uint64_t /*head*/, uint64_t /*length*/, ValueComplete complete)
{
    complete(Status::not_supported("captured overflow backing resolver is unavailable"), {});
}

namespace
{
struct LeafLoad : PageLoadProgress, std::enable_shared_from_this<LeafLoad>
{
    LeafLoad(const PageLoadBudget &budget, uint64_t started_at_ns)
        : PageLoadProgress(budget, started_at_ns),
          metadata(budget.bytes),
          blob(detail::ReservationAllocator<uint8_t>(metadata))
    {
    }

    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<AsyncPageStore>                store;
    std::shared_ptr<detail::FlushRuntime>          runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>    lane;
    PageLoadLimits                                 limits;
    BaseRef                                        base;
    AlignedLoadBytes                               blob;
    PageLoader::LeafComplete                       complete;
    std::shared_ptr<LeafLoad>                      queued_owner;
    std::atomic<bool>                              terminal{false};

    void finish(Status status) noexcept
    {
        if (terminal.exchange(true)) {
            return;
        }
        std::shared_ptr<const LeafBase> leaf;
        try {
            if (status.ok()) {
                status = decode_loaded_leaf({blob.data(), blob.size()}, base, limits, metadata, &leaf);
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted("leaf decode allocation failed");
        }
        release_if_idle();
        (void)metrics.finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
        auto callback = std::move(complete);
        complete_page_load(callback, std::move(status), std::move(leaf));
    }

    static void decode(void *context) noexcept
    {
        auto self = std::move(static_cast<LeafLoad *>(context)->queued_owner);
        self->finish(Status::Ok());
    }
};
} // namespace

std::optional<size_t> AsyncStorePageLoader::primary_reserve_bound(const PageLoadLimits &limits, size_t depth,
                                                                  size_t workers, size_t retained_leaf_pages,
                                                                  size_t                cached_pages,
                                                                  std::optional<size_t> cached_inner_pages) noexcept
{
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    if (limits.max_inflight == 0 || limits.max_leaf_entries == 0) {
        return {};
    }
    const auto chunk_window = chunk_load_window_bound(limits);
    const auto leaf         = decoded_leaf_allocation_bound(limits);
    const auto retained     = AllocationBound(retained_leaf_pages).add(cached_pages).add(workers).add(2).value();
    const auto operations   = AllocationBound(workers).add(1).value();
    const auto decode       = AllocationBound()
                                  .add_product(2, limits.max_frame_bytes)
                                  .add_product(limits.max_leaf_entries, sizeof(uint32_t))
                                  .value();
    if (!leaf || !retained || !operations || !decode || !chunk_window) {
        return {};
    }
    return AllocationBound()
        .add(routing_reserve_bound(limits, depth, workers, cached_inner_pages.value_or(cached_pages)))
        .add_product(*retained, *leaf)
        .add_product(workers, *decode)
        .add_product(*operations, shared_allocation_bound<LeafLoad>() + shared_allocation_bound<PageReadTicket>())
        .add_product(*operations, limits.max_blob_bytes)
        .add_product(*operations, *chunk_window)
        .add_product(*operations, AlignedLoadBytes::kAlignment - 1)
        .value();
}

std::optional<size_t> AsyncStorePageLoader::read_reserve_bound(const PageLoadLimits &limits, size_t depth,
                                                               size_t workers, size_t retained_leaf_pages,
                                                               size_t retained_values, size_t value_bytes,
                                                               size_t                cached_pages,
                                                               std::optional<size_t> cached_inner_pages) noexcept
{
    return detail::AllocationBound()
        .add(primary_reserve_bound(limits, depth, workers, retained_leaf_pages, cached_pages, cached_inner_pages))
        .add(overflow_read_allocation_bound(limits, workers, retained_values, value_bytes))
        .value();
}

AsyncStorePageLoader::AsyncStorePageLoader(std::shared_ptr<AsyncPageStore>             store,
                                           std::shared_ptr<detail::FlushRuntime>       runtime,
                                           std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadLimits limits,
                                           OverflowBackingResolver overflow, PageLoadBudget budget)
{
    if (static_cast<bool>(budget.bytes) != static_cast<bool>(budget.io) ||
        (budget.bytes &&
         ((budget.bytes->category() != detail::BudgetClass::kRouting &&
           budget.bytes->category() != detail::BudgetClass::kReclaim) ||
          budget.io->category() != detail::BudgetClass::kIo || budget.bytes->account() != budget.io->account()))) {
        throw std::invalid_argument("page load needs routing/reclaim and I/O credit from the same account");
    }
    state_           = detail::reserved_shared<State>(budget.bytes);
    state_->store    = std::move(store);
    state_->runtime  = std::move(runtime);
    state_->lane     = std::move(lane);
    state_->limits   = limits;
    state_->overflow = std::move(overflow);
    state_->budget   = std::move(budget);
}

void AsyncStorePageLoader::load_leaf(BaseRef base, LeafComplete complete)
{
    const auto started_at = tree_metrics_now_ns();
    const auto reject     = [&](Status status) {
        record_page_load_terminal(state_->budget, started_at, status);
        complete(std::move(status), {});
    };
    if (base.resident) {
        record_page_load_terminal(state_->budget, started_at, Status::Ok());
        complete(Status::Ok(), std::move(base.resident));
        return;
    }
    if (!state_->runtime || !state_->lane || !base.valid() || !base.backing || base.backing->encoding != 0 ||
        (!state_->store && base.backing->storage != BackingStorage::kResidentFrame)) {
        reject(Status::invalid_argument("leaf load requires a supported independently owned backing"));
        return;
    }
    if (base.backing->length > state_->limits.max_blob_bytes || !chunk_load_allowed(*base.backing, state_->limits)) {
        reject(Status::resource_exhausted("leaf blob exceeds load budget"));
        return;
    }
    if (!acquire_page_load(state_->inflight, state_->limits.max_inflight)) {
        reject(Status::resource_exhausted("leaf load concurrency budget exhausted"));
        return;
    }
    std::shared_ptr<LeafLoad> operation;
    try {
        operation               = detail::reserved_shared<LeafLoad>(state_->budget.bytes, state_->budget, started_at);
        operation->store        = state_->store;
        operation->runtime      = state_->runtime;
        operation->lane         = state_->lane;
        operation->loader_owner = state_;
        operation->inflight     = &state_->inflight;
        operation->limits       = state_->limits;
        operation->base         = std::move(base);
        operation->complete     = std::move(complete);
        if (operation->base.backing->storage == BackingStorage::kResidentFrame) {
            queue_page_load(operation, &LeafLoad::decode);
        }
        else {
            operation->blob.resize(operation->base.backing->length);
            submit_page_read(operation, operation->base.backing, operation->blob.data(), operation->blob.size());
        }
    }
    catch (const std::bad_alloc &) {
        if (operation && operation->complete) {
            operation->finish(Status::resource_exhausted("leaf load allocation failed"));
        }
        else {
            state_->inflight.fetch_sub(1, std::memory_order_release);
            reject(Status::resource_exhausted("leaf load allocation failed"));
        }
    }
    catch (...) {
        operation->finish(Status::io_error("leaf read submission threw"));
    }
}

} // namespace crowdb::tree
