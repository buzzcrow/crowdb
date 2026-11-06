// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_c_api_internal.h"
#include "common/test_root_transport.h"
#include "common/test_runtime_catalog.h"
#include "runtime/tree_runtime_c_api.h"

using namespace crowdb::tree;
using namespace crowdb::tree::detail;
using namespace crowdb::tree::test_chunk;

namespace
{
constexpr size_t kTestPinCapacity = 4096;
}

extern "C" ct_status ct_test_chunk_runtime_create(ct_runtime *runtime, ct_root_catalog **catalog,
                                                  ct_chunk_transport **transport)
try {
    if (catalog == nullptr || *catalog != nullptr || transport == nullptr || *transport != nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::shared_ptr<AllocationBudget> budget;
    auto                              status = runtime_host_allocation_budget(runtime, &budget);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    auto state = std::make_shared<TestRuntimeCatalog>(budget);
    status     = ChunkPinReleaseQueue::create(
        budget, kTestPinCapacity, std::make_shared<int>(1), [](void *) noexcept {}, nullptr, &state->queue);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    auto io                     = std::make_shared<TestRootTransport>(state);
    auto catalog_handle         = std::make_unique<ct_root_catalog>();
    catalog_handle->catalog     = state;
    catalog_handle->transport   = io;
    auto transport_handle       = std::make_unique<ct_chunk_transport>();
    transport_handle->transport = io;
    *catalog                    = catalog_handle.release();
    *transport                  = transport_handle.release();
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

extern "C" ct_status ct_test_chunk_runtime_next(ct_root_catalog *old, ct_root_catalog **out)
try {
    if (old == nullptr || out == nullptr || *out != nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto state = std::dynamic_pointer_cast<TestRuntimeCatalog>(old->catalog);
    auto io    = std::dynamic_pointer_cast<TestRootTransport>(old->transport);
    if (!state || !io) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    // All prior Tree work must already have drained on the host thread. Pump
    // exact queued writer/pin releases before replacing its catalog incarnation.
    const auto progress = state->queue->pump(kTestPinCapacity);
    if (progress.failed != 0 || progress.more || state->writer_releases == 0) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    auto next         = state->next_writer();
    io->catalog       = next;
    auto handle       = std::make_unique<ct_root_catalog>();
    handle->catalog   = std::move(next);
    handle->transport = io;
    *out              = handle.release();
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

extern "C" ct_status ct_test_chunk_runtime_finish(ct_root_catalog *catalog)
{
    if (catalog == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto state = std::dynamic_pointer_cast<TestRuntimeCatalog>(catalog->catalog);
    if (!state) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    state->queue->stop_admission();
    const auto progress = state->queue->pump(kTestPinCapacity);
    return progress.failed == 0 && state->queue->drained() ? 0 : static_cast<ct_status>(Code::kUnavailable);
}

// A valid drop callback transfers context ownership even when installation fails.
extern "C" ct_status ct_test_chunk_runtime_root_load(ct_root_catalog *catalog, bool (*callback)(void *),
                                                     void (*drop_context)(void *), void *context)
try {
    if (drop_context == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::shared_ptr<void> owner(context, drop_context);
    if (catalog == nullptr || callback == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto state = std::dynamic_pointer_cast<TestRuntimeCatalog>(catalog->catalog);
    if (!state) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    state->before_root_load = callback;
    state->root_load_owner  = std::move(owner);
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}
