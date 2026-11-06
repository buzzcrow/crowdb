// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/skip_list.h"

namespace crowdb::tree
{
std::shared_ptr<CellVersion> CellVersion::create(buffer bytes, uint64_t version, uint8_t kind,
                                                 std::shared_ptr<AllocationCounter> counter, AllocationLease credit,
                                                 const std::shared_ptr<AllocationBudget> &budget)
{
    return std::allocate_shared<CellVersion>(MetadataAllocator<CellVersion>(budget), std::move(bytes), version, kind,
                                             std::move(counter), std::move(credit));
}
} // namespace crowdb::tree
