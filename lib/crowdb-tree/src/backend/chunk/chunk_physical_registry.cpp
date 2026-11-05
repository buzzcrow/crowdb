// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/backing_identity.h"
#include "btree/retired_shared.h"
#include "btree/tree_metrics.h"
#include "btree/tree_work_counters.h"
#include "chunk_frame.h"
#include "chunk_physical_registry_internal.h"

namespace crowdb::tree
{
namespace
{
constexpr size_t kMaximumPacks = 1 << 24;
}

Status ChunkPhysicalRegistry::create(const std::shared_ptr<AllocationBudget> &metadata,
                                     std::shared_ptr<ChunkPhysicalRegistry>  *out)
try {
    if (!metadata || !metadata->retirement_queue() || out == nullptr || *out) {
        return Status::invalid_argument("physical registry requires a paid host retirement lane");
    }
    auto state = detail::retired_shared<State>(metadata, metadata->retirement_queue(), metadata->retirement_queue());
    state->backing_metrics =
        std::allocate_shared<TreeResourceMetrics>(MetadataAllocator<TreeResourceMetrics>(metadata));
    state->backing_scope = detail::allocate_backend_identity();
    auto *value          = MetadataAllocator<ChunkPhysicalRegistry>(metadata).allocate(1);
    ::new (static_cast<void *>(value)) ChunkPhysicalRegistry(std::move(state));
    *out = {value,
            [metadata](ChunkPhysicalRegistry *entry) noexcept {
                std::destroy_at(entry);
                MetadataAllocator<ChunkPhysicalRegistry>(metadata).deallocate(entry, 1);
            },
            MetadataAllocator<ChunkPhysicalRegistry>(metadata)};
    return {};
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("physical registry metadata unavailable");
}

bool ChunkPhysicalRegistry::uses_queue(const detail::RetirementQueue *queue) const noexcept
{
    return state_->queue.get() == queue;
}

size_t ChunkPhysicalRegistry::allocation_bound() noexcept
{
    return detail::retired_shared_allocation_bound<State>() + sizeof(ChunkPhysicalRegistry) +
           detail::shared_allocation_bound<ChunkPhysicalRegistry>() +
           detail::shared_allocation_bound<TreeResourceMetrics>();
}

const std::shared_ptr<TreeResourceMetrics> &ChunkPhysicalRegistry::backing_metrics() const noexcept
{
    return state_->backing_metrics;
}

BackingStatsSnapshot ChunkPhysicalRegistry::backing_stats() const noexcept
{
    const auto value = state_->backing_metrics->snapshot();
    return {.scope_id = state_->backing_scope, .allocated_bytes = value.allocated_backing_bytes};
}

bool ChunkPhysicalRegistry::State::retire_step() noexcept
{
    for (size_t count = 0; count < 64 && !packs.empty(); ++count) {
        packs.erase(packs.begin());
    }
    return packs.empty();
}

Status ChunkPhysicalRegistry::State::execute(const std::shared_ptr<Request> &request)
{
    request->state             = shared_from_this();
    request->queued_owner      = request;
    request->dispose           = &State::dispatch;
    request->retirement_budget = request->metadata;
    queue->retire(request.get());
    while (!request->done.load(std::memory_order_acquire)) {
        request->done.wait(false, std::memory_order_acquire);
    }
    return request->status;
}

void ChunkPhysicalRegistry::State::abort(const std::shared_ptr<void> &owner) noexcept
{
    auto request               = std::static_pointer_cast<Request>(owner);
    request->queued_owner      = request;
    request->dispose           = &State::dispatch;
    request->retirement_budget = request->metadata;
    request->state->queue->retire(request.get());
}

bool ChunkPhysicalRegistry::State::dispatch(detail::RetirementNode *node) noexcept
{
    auto request = std::move(static_cast<Request *>(node)->queued_owner);
    try {
        request->state->apply(*request);
    }
    catch (const std::bad_alloc &) {
        request->status = Status::resource_exhausted("physical registry operation exceeded paid metadata");
    }
    catch (...) {
        request->status = Status::internal_error("physical registry operation failed");
    }
    // Keep the paid command alive across both notification and the callback tail.
    // RetirementQueue copied accounting before dispatch and does not read it again.
    request->done.store(true, std::memory_order_release);
    request->done.notify_all();
    return true;
}

void ChunkPhysicalRegistry::State::apply(Request &request)
{
    const auto identity = key(request.stage, request.chunk);
    auto       found    = packs.find(identity);
    if (request.action == Action::Prepare) {
        const bool inserted = found == packs.end();
        if (inserted) {
            if (packs.size() == kMaximumPacks) {
                request.status = Status::resource_exhausted("physical registry capacity exhausted");
                return;
            }
            detail::BudgetMemoryResource::Scope scope(memory, request.metadata);
            auto owner = std::allocate_shared<ChunkPhysicalPack>(MetadataAllocator<ChunkPhysicalPack>(request.metadata),
                                                                 request.stage, request.chunk);
            found      = packs.emplace(identity, std::move(owner)).first;
        }
        if (found->second->stage != request.stage) {
            request.status = Status::corruption("physical Chunk identity already belongs to another stage");
            return;
        }
        request.pack = found->second;
        return;
    }
    if (found == packs.end()) {
        request.status = request.action == Action::Delete
                           ? Status::Ok()
                           : Status::unavailable("physical receipt is no longer registered");
        return;
    }
    auto &pack = *found->second;
    if (pack.stage != request.stage || (request.pack && request.pack != found->second)) {
        request.status = Status::corruption("physical receipt stage incarnation changed");
        return;
    }
    if (request.action == Action::Forget) {
        if (!pack.receipt) {
            packs.erase(found);
        }
        return;
    }
    if (request.receipt) {
        const auto &receipt = *request.receipt;
        if (receipt.chunk != pack.chunk || receipt.owner_epoch != pack.stage.owner_epoch || receipt.created_ms == 0 ||
            receipt.physical_bytes == 0 ||
            (pack.receipt && (pack.receipt->created_ms != receipt.created_ms ||
                              pack.receipt->physical_bytes != receipt.physical_bytes))) {
            request.status = Status::corruption("physical allocation receipt differs from its registered incarnation");
            return;
        }
    }
    if (request.action == Action::Observe) {
        if (!pack.receipt && request.receipt) {
            pack.receipt = request.receipt;
            pack.physical.activate(backing_metrics, request.receipt->physical_bytes);
        }
    }
    else {
        if (!pack.deleted && pack.receipt) {
            record_work(request.metadata, TreeWorkCounter::BackingDeletedBytes, pack.receipt->physical_bytes,
                        TreeWorkCause::GarbageCollection);
        }
        pack.deleted = true;
        pack.physical.mark_deleted();
        packs.erase(found);
    }
}

} // namespace crowdb::tree
