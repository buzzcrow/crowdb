// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_bound.h"
#include "crowdb-tree/btree/scan_packed.h"

#include <algorithm>

namespace crowdb::tree::detail
{
inline void reserve_scan_output(ScanPackedBuf &packed, size_t entry_bytes, size_t initial_capacity)
{
    const auto required = AllocationBound(packed.size()).add(entry_bytes).add(17).value();
    if (!required) {
        throw std::bad_alloc();
    }
    if (*required <= packed.capacity()) {
        return;
    }
    const auto doubled = AllocationBound().add_product(packed.capacity(), 2).value();
    packed.reserve(std::max({*required, initial_capacity, doubled.value_or(*required)}));
}
} // namespace crowdb::tree::detail
