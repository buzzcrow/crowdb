// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/chunk_pin_service_c_api.h"

#include "chunk_c_api_internal.h"
#include "chunk_pin_release.h"
#include "runtime/tree_runtime_c_api.h"

using namespace crowdb::tree;
using namespace crowdb::tree::detail;

struct ct_chunk_pin_service
{
    std::shared_ptr<AllocationBudget>     budget;
    std::shared_ptr<ChunkPinReleaseQueue> queue;
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
ct_status ct_chunk_pin_service_create(ct_runtime *runtime, size_t         capacity, void (*wake)(void *),
                                      void (*drop_context)(void *), void *context, ct_chunk_pin_service **out)
try {
    if (wake == nullptr || drop_context == nullptr || out == nullptr || *out != nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::shared_ptr<AllocationBudget> budget;
    auto                              status = runtime_host_allocation_budget(runtime, &budget);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    auto callback   = std::allocate_shared<WakeContext>(MetadataAllocator<WakeContext>(budget));
    callback->wake  = wake;
    callback->value = context;
    std::shared_ptr<ChunkPinReleaseQueue> queue;
    status = ChunkPinReleaseQueue::create(budget, capacity, callback, &WakeContext::invoke, callback.get(), &queue);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    MetadataAllocator<ct_chunk_pin_service> allocator(budget);
    auto                                   *handle = allocator.allocate(1);
    std::construct_at(handle, ct_chunk_pin_service{.budget = budget, .queue = std::move(queue)});
    callback->drop = drop_context;
    *out           = handle;
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_status ct_root_catalog_bind_pin_service(ct_root_catalog *catalog, ct_chunk_pin_service *service)
{
    if (catalog == nullptr || !catalog->catalog || catalog->catalog->protection() == nullptr || service == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    if (service->queue->snapshot().closed) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    return static_cast<ct_status>(catalog->catalog->bind_pin_release_queue(service->queue).code());
}

ct_chunk_pin_service_step ct_chunk_pin_service_pump(ct_chunk_pin_service *service, size_t limit)
{
    if (service == nullptr || limit == 0 || limit > 64) {
        return {.completed = 0, .failed = 0, .more = 0, .drained = 0};
    }
    const auto step = service->queue->pump(limit);
    return {.completed = step.completed,
            .failed    = step.failed,
            .more      = static_cast<uint8_t>(step.more),
            .drained   = static_cast<uint8_t>(service->queue->drained())};
}

void ct_chunk_pin_service_stop(ct_chunk_pin_service *service)
{
    if (service != nullptr) {
        service->queue->stop_admission();
    }
}

ct_status ct_chunk_pin_service_resource_usage(const ct_chunk_pin_service *service, ct_resource_usage_v1 *out)
{
    if (service == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    const auto value = service->queue->snapshot();
    *out             = {.limit       = value.capacity,
                        .used        = value.registered,
                        .reserved    = 0,
                        .allocated   = value.registered,
                        .denied_tree = 0,
                        .denied_host = value.denied,
                        .waits       = 0,
                        .wait_ns     = 0,
                        .waiters     = value.pending};
    return 0;
}

void ct_chunk_pin_service_free(ct_chunk_pin_service *service)
{
    if (service == nullptr) {
        return;
    }
    auto budget = service->budget;
    std::destroy_at(service);
    MetadataAllocator<ct_chunk_pin_service>(budget).deallocate(service, 1);
}
