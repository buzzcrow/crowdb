// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>

namespace crowdb::tree::detail
{

struct RpcChunkAllocation
{
    uint32_t granularity_kb;
    uint32_t strip_count;
};

[[nodiscard]] inline RpcChunkAllocation rpc_chunk_allocation(uint64_t capacity, uint32_t strip_bytes,
                                                             uint32_t /*copies*/)
{
    constexpr uint64_t kMiB  = 1024U * 1024U;
    uint64_t           bytes = strip_bytes;
    if (bytes == 0) {
        bytes = kMiB;
    }
    if (bytes != 0) {
        return {.granularity_kb = static_cast<uint32_t>(bytes / 1024),
                .strip_count    = static_cast<uint32_t>((capacity + bytes - 1) / bytes)};
    }
    return {.granularity_kb = static_cast<uint32_t>((capacity + 1023) / 1024), .strip_count = 1};
}

} // namespace crowdb::tree::detail
