// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_page_store.h"
#include "chunk_pin_release_internal.h"

#include <cassert>

namespace crowdb::tree::detail
{

ChunkPinOwner::~ChunkPinOwner()
{
    if (node_ != nullptr) {
        auto keep = node_->queue;
        keep->release(node_);
    }
}

void ChunkPinOwner::arm() noexcept
{
    assert(node_ != nullptr);
    node_->armed = true;
}

Status ChunkPinReleaseQueue::create(const std::shared_ptr<AllocationBudget> &budget, size_t capacity,
                                    std::shared_ptr<const void> wake_owner, Wake wake, void *context,
                                    std::shared_ptr<ChunkPinReleaseQueue> *out)
try {
    if (!budget || capacity == 0 || capacity >= kClosed || !wake_owner || wake == nullptr || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    auto queue         = std::allocate_shared<ChunkPinReleaseQueue>(MetadataAllocator<ChunkPinReleaseQueue>(budget));
    queue->capacity_   = capacity;
    queue->wake_owner_ = std::move(wake_owner);
    queue->wake_       = wake;
    queue->context_    = context;
    *out               = std::move(queue);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

ChunkPinReleaseQueue::~ChunkPinReleaseQueue()
{
    assert((state_.load(std::memory_order_relaxed) & ~kClosed) == 0);
    assert(incoming_.load(std::memory_order_relaxed) == nullptr);
    assert(active_ == nullptr && retry_ == nullptr);
}

size_t ChunkPinReleaseQueue::allocation_bound() noexcept
{
    return shared_allocation_bound<ChunkPinReleaseQueue>();
}

Status ChunkPinReleaseQueue::prepare(const std::shared_ptr<AllocationBudget> &budget,
                                     std::shared_ptr<RootCatalog> catalog, uint64_t tree, ChunkStageId stage,
                                     ChunkStageId pin, std::shared_ptr<ChunkPinOwner> *out)
{
    const auto valid = [](ChunkStageId id) {
        return id.owner_epoch != 0 && (id.operation_high != 0 || id.operation_low != 0);
    };
    if (!budget || !catalog || catalog->protection() == nullptr || tree == 0 || !valid(stage) || !valid(pin) ||
        out == nullptr || *out) {
        return Status::invalid_argument();
    }
    return prepare_node(budget, std::move(catalog), tree, stage, pin, false, out);
}

Status ChunkPinReleaseQueue::prepare_writer(const std::shared_ptr<AllocationBudget> &budget,
                                            std::shared_ptr<RootCatalog> catalog, uint64_t tree, uint64_t epoch,
                                            std::shared_ptr<ChunkPinOwner> *out)
{
    if (!budget || !catalog || catalog->protection() == nullptr || tree == 0 || epoch == 0 || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    return prepare_node(budget, std::move(catalog), tree, {.owner_epoch = epoch}, {}, true, out);
}

Status ChunkPinReleaseQueue::prepare_node(const std::shared_ptr<AllocationBudget> &budget,
                                          std::shared_ptr<RootCatalog> catalog, uint64_t tree, ChunkStageId stage,
                                          ChunkStageId pin, bool writer, std::shared_ptr<ChunkPinOwner> *out)
{
    auto state = state_.load(std::memory_order_acquire);
    do {
        if ((state & kClosed) != 0) {
            return Status::unavailable();
        }
        if (state >= capacity_) {
            denied_.fetch_add(1, std::memory_order_relaxed);
            return Status::resource_exhausted();
        }
    } while (!state_.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel));
    ChunkPinReleaseNode                   *node = nullptr;
    MetadataAllocator<ChunkPinReleaseNode> allocator(budget);
    try {
        auto owner = std::allocate_shared<ChunkPinOwner>(MetadataAllocator<ChunkPinOwner>(budget));
        node       = allocator.allocate(1);
        std::construct_at(node);
        node->queue   = shared_from_this();
        node->budget  = budget;
        node->catalog = std::move(catalog);
        node->tree    = tree;
        node->stage   = stage;
        node->pin     = pin;
        node->writer  = writer;
        owner->node_  = node;
        *out          = std::move(owner);
        return Status::Ok();
    }
    catch (const std::bad_alloc &) {
        if (node != nullptr) {
            std::destroy_at(node);
            allocator.deallocate(node, 1);
        }
        state_.fetch_sub(1, std::memory_order_release);
        wake_(context_);
        return Status::resource_exhausted();
    }
}

void ChunkPinReleaseQueue::push(ChunkPinReleaseNode *node) noexcept
{
    auto *head = incoming_.load(std::memory_order_relaxed);
    do {
        node->next = head;
    } while (!incoming_.compare_exchange_weak(head, node, std::memory_order_release, std::memory_order_relaxed));
}

void ChunkPinReleaseQueue::release(ChunkPinReleaseNode *node) noexcept
{
    if (!node->armed) {
        finish(node);
        return;
    }
    pending_.fetch_add(1, std::memory_order_relaxed);
    push(node);
    wake_(context_);
}

void ChunkPinReleaseQueue::finish(ChunkPinReleaseNode *node) noexcept
{
    auto budget = node->budget;
    std::destroy_at(node);
    MetadataAllocator<ChunkPinReleaseNode>(budget).deallocate(node, 1);
    state_.fetch_sub(1, std::memory_order_release);
    wake_(context_);
}

ChunkPinReleaseQueue::Step ChunkPinReleaseQueue::pump(size_t limit) noexcept
{
    if (consumer_.test_and_set(std::memory_order_acquire)) {
        return {.completed = 0, .failed = 0, .more = true};
    }
    Step result{.completed = 0, .failed = 0, .more = false};
    // Keep one finite arrival snapshot across steps. Failed operations wait in
    // a separate batch, so they never repeatedly hide untouched older nodes.
    if (active_ == nullptr) {
        if (prefer_retry_ && retry_ != nullptr) {
            active_       = std::exchange(retry_, nullptr);
            prefer_retry_ = false;
        }
        else {
            active_       = incoming_.exchange(nullptr, std::memory_order_acq_rel);
            prefer_retry_ = true;
            if (active_ == nullptr) {
                active_       = std::exchange(retry_, nullptr);
                prefer_retry_ = false;
            }
        }
    }
    for (size_t visited = 0; visited < limit && active_ != nullptr; ++visited) {
        auto *node  = active_;
        active_     = node->next;
        auto status = Status::internal_error();
        try {
            auto *catalog = node->catalog->protection();
            status        = node->writer ? catalog->writer_quiesced(node->tree, node->stage.owner_epoch)
                                         : catalog->change_pin(node->tree, node->stage, node->pin, false);
        }
        catch (...) {
            status = Status::internal_error();
        }
        if (status.ok()) {
            pending_.fetch_sub(1, std::memory_order_relaxed);
            ++result.completed;
            finish(node);
        }
        else {
            node->next = retry_;
            retry_     = node;
            ++result.failed;
        }
    }
    result.more = active_ != nullptr || retry_ != nullptr || incoming_.load(std::memory_order_acquire) != nullptr;
    consumer_.clear(std::memory_order_release);
    return result;
}

void ChunkPinReleaseQueue::stop_admission() noexcept
{
    state_.fetch_or(kClosed, std::memory_order_acq_rel);
    wake_(context_);
}

ChunkPinReleaseQueue::Snapshot ChunkPinReleaseQueue::snapshot() const noexcept
{
    const auto state = state_.load(std::memory_order_acquire);
    return {.capacity   = capacity_,
            .registered = state & ~kClosed,
            .pending    = pending_.load(std::memory_order_acquire),
            .denied     = denied_.load(std::memory_order_relaxed),
            .closed     = (state & kClosed) != 0};
}

bool ChunkPinReleaseQueue::drained() const noexcept
{
    return state_.load(std::memory_order_acquire) == kClosed;
}
} // namespace crowdb::tree::detail
