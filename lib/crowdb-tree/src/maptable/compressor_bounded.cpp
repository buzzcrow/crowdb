// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/maptable/compressor.h"

#include <cstring>
#include <memory>

#if CROWDB_TREE_HAVE_LZ4
#    include <lz4.h>
#endif

namespace crowdb::tree
{
namespace
{
void put_u32(uint8_t *out, uint32_t value)
{
    for (unsigned i = 0; i < 4; ++i) {
        out[i] = static_cast<uint8_t>(value >> (i * 8));
    }
}
} // namespace

size_t durable_encode_scratch_bytes(compress_algo prefer) noexcept
{
#if CROWDB_TREE_HAVE_LZ4
    return prefer == compress_algo::kLz4 ? static_cast<size_t>(LZ4_sizeofState()) + 7 : 0;
#else
    (void)prefer;
    return 0;
#endif
}

Status encode_durable_page_into(std::span<const uint8_t> frame, compress_algo prefer, std::span<uint8_t> output,
                                std::span<uint8_t> scratch, uint32_t *logical_bytes)
{
    if (logical_bytes == nullptr || frame.empty() || frame.size() > UINT32_MAX - kDurableBlobHeader ||
        (prefer != compress_algo::kNone && prefer != compress_algo::kLz4)) {
        return Status::invalid_argument();
    }
    if (output.size() < kDurableBlobHeader + frame.size() || scratch.size() < durable_encode_scratch_bytes(prefer)) {
        return Status::resource_exhausted();
    }
    auto  algo   = compress_algo::kNone;
    auto  length = static_cast<uint32_t>(frame.size());
    auto *stored = output.data() + kDurableBlobHeader;
#if CROWDB_TREE_HAVE_LZ4
    if (prefer == compress_algo::kLz4) {
        if (frame.size() > LZ4_MAX_INPUT_SIZE) {
            return Status::invalid_argument();
        }
        void *state     = scratch.data();
        auto  available = scratch.size();
        if (std::align(8, static_cast<size_t>(LZ4_sizeofState()), state, available) == nullptr) {
            return Status::resource_exhausted();
        }
        // Any output that does not strictly shrink the payload uses the raw
        // representation. No second compressed-frame allocation is necessary.
        const int compressed = LZ4_compress_fast_extState(
            state, reinterpret_cast<const char *>(frame.data()), reinterpret_cast<char *>(stored),
            static_cast<int>(frame.size()), static_cast<int>(frame.size() - 1), 1);
        if (compressed > 0) {
            length = static_cast<uint32_t>(compressed);
            algo   = compress_algo::kLz4;
        }
    }
#endif
    if (algo == compress_algo::kNone) {
        std::memcpy(stored, frame.data(), frame.size());
    }
    output[0] = static_cast<uint8_t>(algo);
    put_u32(output.data() + 1, static_cast<uint32_t>(frame.size()));
    put_u32(output.data() + 5, length);
    put_u32(output.data() + 9, crowdb::common::crc32c(stored, length));
    *logical_bytes = static_cast<uint32_t>(kDurableBlobHeader) + length;
    return Status::Ok();
}
} // namespace crowdb::tree
