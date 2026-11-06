// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_collection_operation.h"
#include "c_api_maintenance_start.h"
using namespace crowdb::tree;
using crowdb::tree::c_api_detail::begin_report;

int32_t ct_chunk_collect_async(ct_tree *tree, ct_future **out)
{
    return begin_report<ChunkCollectionOperation>(
        tree, out, ct_future_impl::Kind::kChunkCollection,
        [](TreeCore &core, std::shared_ptr<ChunkCollectionOperation> *operation) {
            return core.prepare_chunk_collection(operation);
        });
}
