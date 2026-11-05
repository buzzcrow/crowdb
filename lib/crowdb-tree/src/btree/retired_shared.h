// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "retirement_queue.h"

#include <stdexcept>

namespace crowdb::tree::detail
{
template <class T> struct RetiredObject : RetirementNode
{
    std::shared_ptr<RetirementQueue>  queue;
    std::shared_ptr<AllocationBudget> budget;
    alignas(T) std::byte storage[sizeof(T)];

    T *value() noexcept
    {
        return std::launder(reinterpret_cast<T *>(storage));
    }

    static bool destroy(RetirementNode *entry) noexcept
    {
        auto *self = static_cast<RetiredObject *>(entry);
        if (!RetirementTraits<T>::release(*self->value())) {
            return false;
        }
        std::destroy_at(self->value());
        MetadataAllocator<RetiredObject> allocator(self->budget);
        std::destroy_at(self);
        allocator.deallocate(self, 1);
        return true;
    }
};

template <class T> size_t retired_shared_allocation_bound() noexcept
{
    return sizeof(RetiredObject<T>) + shared_allocation_bound<T>();
}

template <class T, class... Args>
std::shared_ptr<T> retired_shared(const std::shared_ptr<AllocationBudget> &budget,
                                  const std::shared_ptr<RetirementQueue>  &queue, Args &&...args)
{
    if (!budget || !queue) {
        throw std::invalid_argument("retired object requires charged storage and a retirement queue");
    }
    using Node = RetiredObject<T>;
    MetadataAllocator<Node> allocator(budget);
    auto                   *node = allocator.allocate(1);
    std::construct_at(node);
    node->budget            = budget;
    node->retirement_budget = budget;
    if constexpr (RetirementTraits<T>::observed) {
        node->observation.metrics = budget->retirement_metrics();
    }
    node->observation.resources = budget->resource_metrics();
    node->observation.bytes     = sizeof(Node);
    node->queue                 = queue;
    node->dispose               = &Node::destroy;
    try {
        std::construct_at(node->value(), std::forward<Args>(args)...);
        if constexpr (requires(T &value) { value.retirement.bind(&node->observation); }) {
            node->value()->retirement.bind(&node->observation);
        }
    }
    catch (...) {
        std::destroy_at(node);
        allocator.deallocate(node, 1);
        throw;
    }
    // If the independent control allocation fails, shared_ptr invokes this
    // deleter too. The constructed object is still retired without allocating.
    auto dispose = [node](T *) noexcept { node->queue->retire(node); };
    return {node->value(), dispose, MetadataAllocator<T>(budget)};
}
} // namespace crowdb::tree::detail
