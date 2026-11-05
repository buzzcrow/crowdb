// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/chunk_runtime_c_api.h"

#include "c_api_internal.h"
#include "chunk_c_api_internal.h"
#include "chunk_runtime_page_store.h"
#include "runtime/tree_runtime_c_api.h"

using namespace crowdb::tree;
using namespace crowdb::tree::detail;

ct_status ct_chunk_runtime_page_store_open(ct_runtime *runtime, const ct_chunk_runtime_options *options,
                                           ct_root_catalog *catalog, ct_chunk_transport *transport, ct_page_store **out)
try {
    if (options == nullptr || options->version != 1 || options->struct_bytes != sizeof(*options) ||
        catalog == nullptr || !catalog->catalog || transport == nullptr || !transport->transport || out == nullptr ||
        *out != nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::shared_ptr<AllocationBudget> budget;
    const auto                        status = runtime_host_allocation_budget(runtime, &budget);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    AllocationLease handle_credit(budget, sizeof(ct_page_store), AllocationKind::kMetadata);
    AllocationLease store_credit(budget, sizeof(ChunkRuntimePageStore), AllocationKind::kMetadata);
    auto            handle = std::make_unique<ct_page_store>();
    handle->allocation     = std::move(handle_credit);
    handle->bundle         = std::allocate_shared<PageStoreBundle>(MetadataAllocator<PageStoreBundle>(budget));
    auto store             = std::make_unique<ChunkRuntimePageStore>(
        ChunkRuntimeOptions{.tree            = options->tree_id,
                            .owner_epoch     = options->owner_epoch,
                            .open_generation = options->open_generation,
                            .iu_size         = options->iu_size,
                            .mirrors         = options->mirror_copies,
                            .max_chunk_bytes = options->max_chunk_bytes},
        catalog->catalog, transport->transport, std::move(store_credit), budget.get());
    if (!store->supported()) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
    const auto writer = store->prepare_writer(budget);
    if (!writer.ok()) {
        return static_cast<ct_status>(writer.code());
    }
    handle->bundle->store         = std::move(store);
    handle->bundle->backend_label = "chunk-runtime";
    *out                          = handle.release();
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}
