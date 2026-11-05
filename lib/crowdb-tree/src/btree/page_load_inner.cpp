// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/page_load_internal.h"
#include "btree/page_load_lifetime.h"
#include "btree/published_state.h"
#include "crowdb-tree/maptable/compressor.h"

namespace crowdb::tree
{
void PageLoader::load_inner(std::shared_ptr<const BackingRef> /*backing*/, InnerComplete complete)
{
    complete(Status::not_supported("asynchronous index loader is unavailable"), {});
}

namespace
{
Status read_inner_proof(const uint8_t *frame, uint32_t bytes, const PageLoadLimits &limits,
                        const std::shared_ptr<detail::AllocationReservation> &metadata, size_t key_bytes,
                        InnerFrameProof *proof)
{
    proof->self_page_id   = frame_u64(frame, fh::kSelfpage_id);
    proof->frame_bytes    = bytes;
    proof->has_fences     = frame_has_lower_fence(frame);
    proof->page_id_fences = frame_fences_are_page_ids(frame);
    if (proof->page_id_fences) {
        proof->lower_leaf = frame_lower_fence_page_id(frame);
        proof->upper_leaf = frame_upper_fence_page_id(frame);
    }
    else if (proof->has_fences) {
        for (const auto key : {frame_lower_fence(frame), frame_upper_fence(frame)}) {
            if (key.size() > limits.max_inner_key_bytes - key_bytes) {
                return Status::resource_exhausted("loaded index exceeds fence byte budget");
            }
            key_bytes += key.size();
        }
        proof->lower = OwnedKey(frame_lower_fence(frame), metadata);
        proof->upper = OwnedKey(frame_upper_fence(frame), metadata);
    }
    return Status::Ok();
}

Status decode_inner(const AlignedLoadBytes &blob, const BackingRef &backing, const PageLoadLimits &limits,
                    const std::shared_ptr<detail::AllocationReservation> &metadata,
                    std::shared_ptr<const InnerPageState>                *out)
{
    LoadBytes  frame{LoadBytes::allocator_type(metadata)};
    auto       status = decode_backing_frame({blob.data(), blob.size()}, backing, limits, metadata, &frame);
    const auto bytes  = static_cast<uint32_t>(frame.size());
    if (!status.ok()) {
        return status;
    }
    if (!frame_validate(frame.data(), bytes) || frame_page_type(frame.data()) != page_type::kInnerBase) {
        return Status::corruption("loaded backing is not a valid index frame");
    }
    const InnerFrameView index(frame.data(), bytes);
    if (index.num_children() > limits.max_inner_children) {
        return Status::resource_exhausted("loaded index exceeds child budget");
    }
    size_t key_bytes = 0;
    for (uint32_t i = 0; i < index.num_separators(); ++i) {
        const size_t size = index.separator_at(i).size();
        if (size > limits.max_inner_key_bytes - key_bytes) {
            return Status::resource_exhausted("loaded index exceeds separator byte budget");
        }
        key_bytes += size;
    }
    for (uint32_t i = 0; i < index.num_children(); ++i) {
        if (index.child_at(i) >= MappingView::kPageLimit) {
            return Status::corruption("loaded index contains an invalid child PID");
        }
    }
    auto inner = detail::reserved_shared<InnerPageState>(metadata, metadata);
    inner->children.reserve(index.num_children());
    inner->separators.reserve(index.num_separators());
    for (uint32_t i = 0; i < index.num_children(); ++i) {
        inner->children.push_back(index.child_at(i));
    }
    for (uint32_t i = 0; i < index.num_separators(); ++i) {
        inner->separators.emplace_back(index.separator_at(i), metadata);
    }
    status = read_inner_proof(frame.data(), bytes, limits, metadata, key_bytes, &inner->frame_proof.emplace());
    if (!status.ok()) {
        return status;
    }
    *out = std::move(inner);
    return Status::Ok();
}

struct InnerLoad : PageLoadProgress, std::enable_shared_from_this<InnerLoad>
{
    InnerLoad(const PageLoadBudget &budget, uint64_t started_at_ns)
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
    std::shared_ptr<const BackingRef>              backing;
    AlignedLoadBytes                               blob;
    PageLoader::InnerComplete                      complete;
    std::shared_ptr<InnerLoad>                     queued_owner;
    std::atomic<bool>                              terminal{false};

    void finish(Status status) noexcept
    {
        if (terminal.exchange(true)) {
            return;
        }
        std::shared_ptr<const InnerPageState> inner;
        try {
            if (status.ok()) {
                status = decode_inner(blob, *backing, limits, metadata, &inner);
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted("index decode allocation failed");
        }
        catch (...) {
            status = Status::internal_error("index decode failed");
        }
        release_if_idle();
        (void)metrics.finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
        auto callback = std::move(complete);
        complete_page_load(callback, std::move(status), std::move(inner));
    }

    static void decode(void *context) noexcept
    {
        auto self = std::move(static_cast<InnerLoad *>(context)->queued_owner);
        self->finish(Status::Ok());
    }
};
} // namespace

std::optional<size_t> AsyncStorePageLoader::routing_reserve_bound(const PageLoadLimits &limits, size_t depth,
                                                                  size_t workers, size_t cached_pages) noexcept
{
    if (depth > 64 || workers == 0 || limits.max_inner_children == 0 || limits.max_inner_children > 4096 ||
        limits.max_blob_bytes == 0 || limits.max_frame_bytes == 0) {
        return {};
    }
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    const auto chunk_window = chunk_load_window_bound(limits);
    const auto inner        = AllocationBound(InnerPageState::allocation_bound())
                                  .add_product(limits.max_inner_children,
                                               sizeof(OwnedKey) + sizeof(uint64_t) + OwnedKey::allocation_overhead())
                                  .add(limits.max_inner_key_bytes)
                                  .add_product(2, OwnedKey::allocation_overhead())
                                  .value();
    const auto retained     = AllocationBound(depth).add(workers).add(cached_pages).add(2).value();
    const auto operations   = AllocationBound(workers).add(1).value();
    if (!inner || !retained || !operations || !chunk_window) {
        return {};
    }
    return AllocationBound(shared_allocation_bound<AsyncStorePageLoader>() + shared_allocation_bound<State>())
        .add_product(*retained, *inner)
        .add_product(*operations, shared_allocation_bound<InnerLoad>() + shared_allocation_bound<PageReadTicket>())
        .add_product(*operations, limits.max_blob_bytes)
        .add_product(*operations, *chunk_window)
        .add_product(*operations, AlignedLoadBytes::kAlignment - 1)
        .add_product(workers, limits.max_frame_bytes)
        .value();
}

void AsyncStorePageLoader::load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete)
{
    const auto started_at = tree_metrics_now_ns();
    const auto reject     = [&](Status status) {
        record_page_load_terminal(state_->budget, started_at, status);
        complete(std::move(status), {});
    };
    if (!state_->runtime || !state_->lane || !backing || !backing->readable() || backing->encoding != 0 ||
        (!state_->store && backing->storage != BackingStorage::kResidentFrame)) {
        reject(Status::invalid_argument("index load requires a supported independently owned backing and runtime"));
        return;
    }
    if (backing->length > state_->limits.max_blob_bytes || !chunk_load_allowed(*backing, state_->limits)) {
        reject(Status::resource_exhausted("index blob exceeds load budget"));
        return;
    }
    if (!acquire_page_load(state_->inflight, state_->limits.max_inflight)) {
        reject(Status::resource_exhausted("index load concurrency budget exhausted"));
        return;
    }
    std::shared_ptr<InnerLoad> operation;
    try {
        operation               = detail::reserved_shared<InnerLoad>(state_->budget.bytes, state_->budget, started_at);
        operation->store        = state_->store;
        operation->runtime      = state_->runtime;
        operation->lane         = state_->lane;
        operation->loader_owner = state_;
        operation->inflight     = &state_->inflight;
        operation->limits       = state_->limits;
        operation->backing      = std::move(backing);
        operation->complete     = std::move(complete);
        if (operation->backing->storage == BackingStorage::kResidentFrame) {
            queue_page_load(operation, &InnerLoad::decode);
        }
        else {
            operation->blob.resize(operation->backing->length);
            submit_page_read(operation, operation->backing, operation->blob.data(), operation->blob.size());
        }
    }
    catch (const std::bad_alloc &) {
        if (operation && operation->complete) {
            operation->finish(Status::resource_exhausted("index load allocation failed"));
        }
        else {
            state_->inflight.fetch_sub(1, std::memory_order_release);
            reject(Status::resource_exhausted("index load allocation failed"));
        }
    }
    catch (...) {
        operation->finish(Status::io_error("index read submission threw"));
    }
}
} // namespace crowdb::tree
