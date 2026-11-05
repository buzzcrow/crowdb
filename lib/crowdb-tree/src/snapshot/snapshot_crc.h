// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace crowdb::tree::snapshot_detail
{
// Concatenate two independently accumulated CRC32C values without retaining
// their bytes. Powers of the reflected byte transition bound work to 64 steps.
inline uint32_t crc_concatenate(uint32_t prefix, uint32_t suffix, uint64_t suffix_bytes) noexcept
{
    using Matrix = std::array<uint32_t, 32>;
    auto apply   = [](const Matrix &matrix, uint32_t bits) {
        uint32_t result = 0;
        for (size_t i = 0; i < matrix.size(); ++i) {
            if ((bits & (uint32_t{1} << i)) != 0) {
                result ^= matrix[i];
            }
        }
        return result;
    };
    Matrix byte{};
    for (size_t i = 0; i < byte.size(); ++i) {
        uint32_t state = uint32_t{1} << i;
        for (unsigned bit = 0; bit != 8; ++bit) {
            state = (state >> 1) ^ ((state & 1U) != 0 ? 0x82f63b78U : 0U);
        }
        byte[i] = state;
    }
    while (suffix_bytes != 0) {
        if ((suffix_bytes & 1U) != 0) {
            prefix = apply(byte, prefix);
        }
        suffix_bytes >>= 1;
        if (suffix_bytes != 0) {
            Matrix squared{};
            for (size_t i = 0; i < byte.size(); ++i) {
                squared[i] = apply(byte, byte[i]);
            }
            byte = squared;
        }
    }
    return prefix ^ suffix;
}
} // namespace crowdb::tree::snapshot_detail
