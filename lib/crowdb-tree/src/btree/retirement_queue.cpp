// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "retirement_queue.h"

#include "tree_metrics.h"

#include <cassert>

namespace crowdb::tree::detail
{
void RetirementObservation::start() noexcept
{
    if ((!metrics && !resources) || started_at.load(std::memory_order_relaxed) != 0) {
        return;
    }
    const auto now    = tree_metrics_now_ns();
    uint64_t   before = 0;
    if (started_at.compare_exchange_strong(before, now, std::memory_order_release, std::memory_order_relaxed)) {
        // Every caller still owns T, so its last-owner disposal cannot race
        // this begin. Competing logical retirement markers collapse to one.
        if (metrics) {
            age_slot = metrics->begin(TreeStage::MemoryRetireWait, now);
            record_work(metrics, TreeWorkCounter::LogicalRetirementBytes, bytes, TreeWorkCause::Allocation);
        }
        if (resources) {
            const auto added = resources->add(TreeResourceMetric::RetiredBytes, bytes);
            assert(added);
            (void)added;
        }
    }
}

Status RetirementQueue::create(FlushRuntime &runtime, std::shared_ptr<FlushRuntime::Lane> lane,
                               const std::shared_ptr<AllocationBudget> &budget, std::shared_ptr<RetirementQueue> *out)
try {
    if (!lane || !budget || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    auto queue  = std::allocate_shared<RetirementQueue>(MetadataAllocator<RetirementQueue>(budget));
    auto status = runtime.reserve_progress(std::move(lane), &run, queue.get(), budget, &queue->progress_);
    if (!status.ok()) {
        return status;
    }
    *out = std::move(queue);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

size_t RetirementQueue::allocation_bound() noexcept
{
    return shared_allocation_bound<RetirementQueue>() + FlushRuntime::progress_allocation_bound();
}

RetirementQueue::~RetirementQueue()
{
    // Binding teardown closes the registration while its executor is alive.
    assert(!progress_ || state_.load(std::memory_order_acquire) == 2);
    assert(incoming_.load(std::memory_order_relaxed) == nullptr);
}

void RetirementQueue::retire(RetirementNode *node) noexcept
{
    assert(node != nullptr && node->dispose != nullptr);
    auto keep = weak_from_this().lock();
    assert(keep);
    if (node->retirement_budget) {
        node->retirement_completion = node->retirement_budget->retirement_started();
    }
    node->observation.start();
    enqueued_.fetch_add(1, std::memory_order_relaxed);
    auto *head = incoming_.load(std::memory_order_relaxed);
    do {
        node->next = head;
    } while (!incoming_.compare_exchange_weak(head, node, std::memory_order_release, std::memory_order_relaxed));
    const auto state = state_.load(std::memory_order_acquire);
    if (state == 0) {
        progress_->request(keep);
    }
    else if (state == 2) {
        drain_host();
    }
}

bool RetirementQueue::pending() const noexcept
{
    return incoming_.load(std::memory_order_acquire) != nullptr;
}

RetirementQueue::Step RetirementQueue::step(size_t limit) noexcept
{
    if (consumer_.test_and_set(std::memory_order_acquire)) {
        return Step::kBusy;
    }
    size_t visited = 0;
    while (visited < limit) {
        auto *node = incoming_.load(std::memory_order_acquire);
        while (node != nullptr && !incoming_.compare_exchange_weak(node, node->next, std::memory_order_acq_rel,
                                                                   std::memory_order_acquire)) {
        }
        if (node == nullptr) {
            break;
        }
        ++visited;
        auto       accounting = node->retirement_completion;
        auto       metrics    = node->observation.metrics;
        auto       resources  = node->observation.resources;
        const auto bytes      = node->observation.bytes;
        const auto started    = node->observation.started_at.load(std::memory_order_acquire);
        const auto age_slot   = node->observation.age_slot;
        if (node->dispose(node)) {
            if (metrics) {
                metrics->complete(TreeStage::MemoryRetireWait, TreeOutcome::Success, tree_metrics_now_ns() - started,
                                  age_slot);
            }
            metrics.reset();
            if (resources) {
                const auto removed = resources->remove(TreeResourceMetric::RetiredBytes, bytes);
                assert(removed);
                (void)removed;
            }
            resources.reset();
            freed_.fetch_add(1, std::memory_order_relaxed);
            if (accounting.finish != nullptr) {
                accounting.finish(accounting.owner.get());
            }
        }
        else {
            auto *head = incoming_.load(std::memory_order_relaxed);
            do {
                node->next = head;
            } while (
                !incoming_.compare_exchange_weak(head, node, std::memory_order_release, std::memory_order_relaxed));
        }
    }
    auto maximum = max_step_nodes_.load(std::memory_order_relaxed);
    while (maximum < visited && !max_step_nodes_.compare_exchange_weak(maximum, visited, std::memory_order_relaxed)) {
    }
    consumer_.clear(std::memory_order_release);
    return visited != 0 ? Step::kWork : Step::kEmpty;
}

void RetirementQueue::run(void *context) noexcept
{
    auto &queue = *static_cast<RetirementQueue *>(context);
    queue.cpu_steps_.fetch_add(1, std::memory_order_relaxed);
    (void)queue.step(kNodesPerStep);
    if (queue.pending() && queue.state_.load(std::memory_order_acquire) == 0) {
        queue.progress_->request(queue.weak_from_this().lock());
    }
}

void RetirementQueue::drain_host() noexcept
{
    struct Trampoline
    {
        RetirementQueue *head   = nullptr;
        bool             active = false;
    };

    static thread_local Trampoline local;
    if (fallback_queued_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    fallback_owner_ = weak_from_this().lock();
    assert(fallback_owner_);
    fallback_next_ = local.head;
    local.head     = this;
    if (local.active) {
        return;
    }
    local.active = true;
    while (local.head != nullptr) {
        auto *queue = local.head;
        local.head  = queue->fallback_next_;
        auto keep   = std::move(queue->fallback_owner_);
        assert(keep);
        while (queue->step(kNodesPerStep) == Step::kWork) {
        }
        queue->fallback_queued_.exchange(false, std::memory_order_acq_rel);
        // An arrival that saw the preceding queued flag relies on this
        // recheck. A later arrival owns another trampoline registration.
        if (queue->pending()) {
            queue->drain_host();
        }
    }
    local.active = false;
}

void RetirementQueue::close_and_drain() noexcept
{
    unsigned expected = 0;
    if (!state_.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
        assert(expected == 2 && "retirement close belongs to one host boundary");
        return;
    }
    progress_->close_and_drain();
    drain_host();
    state_.store(2, std::memory_order_release);
    drain_host();
}

RetirementQueue::Snapshot RetirementQueue::snapshot() const noexcept
{
    return {.enqueued       = enqueued_.load(std::memory_order_relaxed),
            .freed          = freed_.load(std::memory_order_relaxed),
            .cpu_steps      = cpu_steps_.load(std::memory_order_relaxed),
            .max_step_nodes = max_step_nodes_.load(std::memory_order_relaxed)};
}
} // namespace crowdb::tree::detail
