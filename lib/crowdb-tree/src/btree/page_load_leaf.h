// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_load.h"

#include <span>

namespace crowdb::tree
{
// Fold the captured frame at its proven slot bound, preserving tombstones and
// the backing's overflow protection. This does not provide historical reads.
Status decode_loaded_leaf(std::span<const uint8_t> blob, const BaseRef &base, const PageLoadLimits &limits,
                          const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<const LeafBase> *out);
} // namespace crowdb::tree
