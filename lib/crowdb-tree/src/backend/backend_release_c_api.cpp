// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend_release_c_api.h"

#include "c_api_internal.h"
#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"

using namespace crowdb::tree;

struct ct_backend_release_service
{
    std::shared_ptr<AllocationBudget>      budget;
    std::shared_ptr<BackendReleaseService> queue;
};

namespace
{
struct WakeContext
{
    void (*wake)(void *) = nullptr;
    void (*drop)(void *) = nullptr;
    void *value          = nullptr;

    ~WakeContext()
    {
        if (drop != nullptr) {
            drop(value);
        }
    }

    static void invoke(void *context) noexcept
    {
        auto &self = *static_cast<WakeContext *>(context);
        self.wake(self.value);
    }
};
} // namespace

ct_status ct_tree_backend_release_service(ct_tree *tree, void (*wake)(void *), void (*drop)(void *), void *context,
                                          ct_backend_release_service **out)
try {
    if (tree == nullptr || wake == nullptr || drop == nullptr || out == nullptr || *out != nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto *store = dynamic_cast<BlockPageStore *>(c_api_page_store(tree));
    if (store == nullptr || !store->release_service()) {
        return static_cast<ct_status>(Code::kNotFound);
    }
    const auto &queue    = store->release_service();
    const auto &budget   = queue->allocation_budget();
    auto        callback = std::allocate_shared<WakeContext>(MetadataAllocator<WakeContext>(budget));
    callback->wake       = wake;
    callback->value      = context;
    MetadataAllocator<ct_backend_release_service> allocator(budget);
    auto                                         *handle = allocator.allocate(1);
    std::construct_at(handle, ct_backend_release_service{.budget = budget, .queue = queue});
    const auto status = queue->bind_wake(callback, &WakeContext::invoke, callback.get());
    if (!status.ok()) {
        std::destroy_at(handle);
        allocator.deallocate(handle, 1);
        return static_cast<ct_status>(status.code());
    }
    callback->drop = drop;
    *out           = handle;
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_backend_release_step ct_backend_release_service_pump(ct_backend_release_service *service, size_t limit)
{
    if (service == nullptr || limit == 0 || limit > 64) {
        return {};
    }
    const auto step = service->queue->pump(limit);
    return {.visited   = step.visited,
            .completed = step.completed,
            .more      = static_cast<uint8_t>(step.more),
            .drained   = static_cast<uint8_t>(service->queue->drained())};
}

void ct_backend_release_service_stop(ct_backend_release_service *service)
{
    if (service != nullptr) {
        service->queue->stop_admission();
    }
}

void ct_backend_release_service_free(ct_backend_release_service *service)
{
    if (service == nullptr) {
        return;
    }
    auto budget = service->budget;
    std::destroy_at(service);
    MetadataAllocator<ct_backend_release_service>(budget).deallocate(service, 1);
}
