// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "default_runtime.h"

#include "crowdb-tree/config.h"
#include "tree_runtime_internal.h"

namespace crowdb::tree
{
namespace
{
struct DefaultRuntime
{
    std::shared_ptr<TreeRuntime>      runtime;
    std::shared_ptr<AllocationBudget> metadata;

    static void destroy(DefaultRuntime *entry) noexcept
    {
        if (entry != nullptr) {
            MetadataAllocator<DefaultRuntime> allocator(entry->metadata);
            std::destroy_at(entry);
            allocator.deallocate(entry, 1);
        }
    }
};

struct ProcessRuntime
{
    ~ProcessRuntime()
    {
        DefaultRuntime::destroy(value.load(std::memory_order_relaxed));
    }

    std::atomic<DefaultRuntime *> value{nullptr};
};

ProcessRuntime process_runtime;

Status default_runtime(std::shared_ptr<TreeRuntime> *out)
{
    auto *entry = process_runtime.value.load(std::memory_order_acquire);
    if (entry == nullptr) {
        std::shared_ptr<TreeRuntime> runtime;
        auto                         status = TreeRuntime::create({}, &runtime);
        if (!status.ok()) {
            return status;
        }
        auto                              metadata = TreeRuntimeAccess::lifecycle(*runtime);
        MetadataAllocator<DefaultRuntime> allocator(metadata);
        auto                             *prepared = allocator.allocate(1);
        std::construct_at(prepared, DefaultRuntime{.runtime = std::move(runtime), .metadata = std::move(metadata)});
        if (process_runtime.value.compare_exchange_strong(entry, prepared, std::memory_order_release,
                                                          std::memory_order_acquire)) {
            entry = prepared;
        }
        else {
            DefaultRuntime::destroy(prepared);
        }
    }
    *out = entry->runtime;
    return Status::Ok();
}
} // namespace

Status configure_default_runtime(Config *config)
try {
    if (config == nullptr) {
        return Status::invalid_argument();
    }
    return config->runtime ? Status::Ok() : default_runtime(&config->runtime);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (...) {
    return Status::internal_error();
}
} // namespace crowdb::tree
