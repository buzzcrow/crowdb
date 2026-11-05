// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/captured_write.h"

namespace crowdb::tree
{
Status capture_stored_overflow(const LeafFrameView &leaf, size_t max_references,
                               const std::shared_ptr<detail::AllocationReservation> &metadata,
                               std::shared_ptr<const StoredOverflowReferences>      *out);
} // namespace crowdb::tree
