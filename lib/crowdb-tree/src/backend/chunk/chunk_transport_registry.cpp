// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/budget_memory_resource.h"
#include "btree/retired_shared.h"
#include "chunk_physical_registry.h"
#include "chunk_transport.h"

#include <cassert>
#include <map>

namespace crowdb::tree::detail
{
namespace
{
struct RegistryPoolState : std::enable_shared_from_this<RegistryPoolState>
{
    struct Key
    {
        const RetirementQueue *host = nullptr;
        uint64_t               tree = 0;
    };

    struct Less
    {
        bool operator()(Key left, Key right) const noexcept
        {
            return left.host == right.host ? left.tree < right.tree
                                           : std::less<const RetirementQueue *>{}(left.host, right.host);
        }
    };

    struct Entry
    {
        // The scope's host identity remains owned even after its executor closes.
        std::shared_ptr<RetirementQueue>       host;
        std::shared_ptr<ChunkPhysicalRegistry> registry;
    };

    struct Request : RetirementNode
    {
        std::shared_ptr<RegistryPoolState>     state;
        std::shared_ptr<Request>               queued_owner;
        std::shared_ptr<AllocationBudget>      budget;
        std::shared_ptr<ChunkPhysicalRegistry> result;
        uint64_t                               tree = 0;
        Status                                 status;
        std::atomic<bool>                      done{false};
    };

    explicit RegistryPoolState(std::shared_ptr<RetirementQueue> executor) : queue(std::move(executor))
    {
    }

    bool retire_step() noexcept
    {
        for (size_t count = 0; count < 32 && !entries.empty(); ++count) {
            entries.erase(entries.begin());
        }
        return entries.empty();
    }

    static bool dispatch(RetirementNode *node) noexcept
    {
        auto request = std::move(static_cast<Request *>(node)->queued_owner);
        try {
            auto     &self = *request->state;
            auto      host = request->budget->retirement_queue();
            const Key key{.host = host.get(), .tree = request->tree};
            auto      found = self.entries.find(key);
            if (found == self.entries.end()) {
                std::shared_ptr<ChunkPhysicalRegistry> registry;
                request->status = ChunkPhysicalRegistry::create(request->budget, &registry);
                if (request->status.ok()) {
                    BudgetMemoryResource::Scope credit(self.memory, request->budget);
                    found = self.entries.emplace(key, Entry{.host = std::move(host), .registry = std::move(registry)})
                                .first;
                }
            }
            if (request->status.ok()) {
                request->result = found->second.registry;
            }
        }
        catch (const std::bad_alloc &) {
            request->status = Status::resource_exhausted();
        }
        catch (...) {
            request->status = Status::internal_error();
        }
        request->state.reset();
        request->done.store(true, std::memory_order_release);
        request->done.notify_all();
        return true;
    }

    Status acquire(uint64_t tree, const std::shared_ptr<AllocationBudget> &metadata,
                   std::shared_ptr<ChunkPhysicalRegistry> *out)
    {
        auto request               = std::allocate_shared<Request>(MetadataAllocator<Request>(metadata));
        request->state             = shared_from_this();
        request->queued_owner      = request;
        request->budget            = metadata;
        request->retirement_budget = metadata;
        request->tree              = tree;
        request->dispose           = &dispatch;
        queue->retire(request.get());
        while (!request->done.load(std::memory_order_acquire)) {
            request->done.wait(false, std::memory_order_acquire);
        }
        if (request->status.ok()) {
            *out = std::move(request->result);
        }
        return request->status;
    }

    std::shared_ptr<RetirementQueue> queue;
    BudgetMemoryResource             memory;
    std::pmr::map<Key, Entry, Less>  entries{&memory};
};
} // namespace

struct ChunkRegistryPool
{
    std::shared_ptr<AllocationBudget>  metadata;
    std::shared_ptr<RegistryPoolState> state;

    static void destroy(ChunkRegistryPool *pool) noexcept
    {
        if (pool != nullptr) {
            MetadataAllocator<ChunkRegistryPool> allocator(pool->metadata);
            std::destroy_at(pool);
            allocator.deallocate(pool, 1);
        }
    }
};

ChunkTransport::~ChunkTransport()
{
    ChunkRegistryPool::destroy(registry_pool_.load(std::memory_order_relaxed));
}

Status ChunkTransport::physical_registry(uint64_t tree, const std::shared_ptr<AllocationBudget> &metadata,
                                         std::shared_ptr<ChunkPhysicalRegistry> *out)
try {
    if (tree == 0 || !metadata || !metadata->retirement_queue() || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    auto *pool = registry_pool_.load(std::memory_order_acquire);
    if (pool == nullptr) {
        auto state =
            retired_shared<RegistryPoolState>(metadata, metadata->retirement_queue(), metadata->retirement_queue());
        MetadataAllocator<ChunkRegistryPool> allocator(metadata);
        auto                                *prepared = allocator.allocate(1);
        std::construct_at(prepared, ChunkRegistryPool{.metadata = metadata, .state = std::move(state)});
        if (registry_pool_.compare_exchange_strong(pool, prepared, std::memory_order_release,
                                                   std::memory_order_acquire)) {
            pool = prepared;
        }
        else {
            ChunkRegistryPool::destroy(prepared);
        }
    }
    return pool->state->acquire(tree, metadata, out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree::detail
