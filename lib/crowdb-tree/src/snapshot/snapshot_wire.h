// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-common/crc32c.h"
#include "snapshot/published_snapshot_entries.h"

#include <algorithm>
#include <array>
#include <span>

namespace crowdb::tree::snapshot_detail
{
inline void put_u32(uint8_t *out, uint32_t value)
{
    for (unsigned i = 0; i != 4; ++i) {
        out[i] = static_cast<uint8_t>(value >> (8 * i));
    }
}

inline void put_u64(uint8_t *out, uint64_t value)
{
    for (unsigned i = 0; i != 8; ++i) {
        out[i] = static_cast<uint8_t>(value >> (8 * i));
    }
}

inline std::span<const uint8_t> bytes(Slice value)
{
    return {reinterpret_cast<const uint8_t *>(value.data()), value.size()};
}

inline uint32_t crc_bytes(uint32_t crc, std::span<const uint8_t> bytes)
{
    // The shared SIMD CRC API takes an int length. Keep every call bounded
    // even when a configured logical-value limit exceeds that parameter.
    while (!bytes.empty()) {
        const size_t take = std::min(bytes.size(), size_t{65536});
        crc               = crowdb::common::crc32c_update(crc, bytes.data(), take);
        bytes             = bytes.subspan(take);
    }
    return crc;
}

inline Status entry_fields(const PublishedScanEntry &entry, std::array<uint8_t, 17> *fields)
{
    if (entry.key.size() > UINT32_MAX || entry.cell.value.size() > UINT32_MAX) {
        return Status::resource_exhausted("portable tuple exceeds wire length fields");
    }
    put_u32(fields->data(), static_cast<uint32_t>(entry.key.size()));
    put_u64(fields->data() + 4, entry.cell.slot);
    (*fields)[12] = entry.cell.tombstone() ? 1 : 0;
    put_u32(fields->data() + 13, static_cast<uint32_t>(entry.cell.value.size()));
    return Status::Ok();
}
} // namespace crowdb::tree::snapshot_detail
