// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_c_api_internal.h"
#include "chunk_protection_callback.h"

ct_status ct_callback_root_catalog_open_v3(const ct_root_catalog_callbacks            *callbacks,
                                           const ct_root_catalog_staging_callbacks    *staging,
                                           const ct_root_catalog_protection_callbacks *protection, void *context,
                                           ct_root_catalog **out)
try {
    using namespace crowdb::tree;
    using namespace crowdb::tree::detail;
    if (callbacks == nullptr || callbacks->load == nullptr || callbacks->free_blob == nullptr ||
        callbacks->store == nullptr || callbacks->publish == nullptr ||
        callbacks->allocate_reference_segment_id == nullptr || staging == nullptr || staging->version != 1 ||
        staging->struct_bytes != sizeof(*staging) || staging->capabilities != CT_CHUNK_STAGE_SCHEMA_V1 ||
        staging->load == nullptr || staging->compare == nullptr || staging->list == nullptr || protection == nullptr ||
        !protection_callback::supported(*protection) || out == nullptr || *out != nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto handle       = std::make_unique<ct_root_catalog>();
    handle->transport = std::make_shared<MemoryChunkTransport>();
    handle->catalog   = std::make_shared<CallbackRootCatalog>(*callbacks, *staging, *protection, context);
    *out              = handle.release();
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(crowdb::tree::Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(crowdb::tree::Code::kInternal);
}

uint64_t ct_root_catalog_protection_capabilities(const ct_root_catalog *catalog)
{
    return (catalog != nullptr) && catalog->catalog && (catalog->catalog->protection() != nullptr)
             ? CT_CHUNK_PROTECTION_SCHEMA_V1
             : 0;
}
