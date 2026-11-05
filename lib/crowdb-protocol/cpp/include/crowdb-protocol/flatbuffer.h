// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <flatbuffers/base.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace crowdb::protocol
{
// Call after VerifyBuffer<Table>. RPC controls can follow an unaligned wire
// header. Read the root offset without an aligned scalar dereference; generated
// table accessors retain their normal borrowed, unaligned field reads.
template <class Table> const Table *flatbuffer_root(const uint8_t *bytes, size_t size) noexcept
{
    static_assert(alignof(Table) == 1);
    if (bytes == nullptr || size < sizeof(flatbuffers::uoffset_t)) {
        return nullptr;
    }
    flatbuffers::uoffset_t offset = 0;
    std::memcpy(&offset, bytes, sizeof(offset));
    offset = flatbuffers::EndianScalar(offset);
    return offset < size ? reinterpret_cast<const Table *>(bytes + offset) : nullptr;
}
} // namespace crowdb::protocol
