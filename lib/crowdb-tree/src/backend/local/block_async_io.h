// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/block_page_store.h"

#include <optional>

#ifdef CROWDB_HAVE_LIBURING
namespace crowdb::tree::detail
{
std::optional<size_t> block_io_allocation_bound(size_t bytes, size_t parts) noexcept;
std::optional<size_t> block_sync_allocation_bound(size_t extents) noexcept;

// Splits aligned byte ranges across extents and retains the completion owner
// until every submitted part has completed, including inline failures.
void submit_block_io(BlockPageStore &store, ::crowdb::common::DiskIOUring &uring, PageAddr address, void *read_buffer,
                     const void *write_buffer, size_t length, AsyncCompletion complete);
} // namespace crowdb::tree::detail
#endif
