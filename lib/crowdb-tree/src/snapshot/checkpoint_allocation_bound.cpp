// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/local_checkpoint.h"

namespace crowdb::tree
{
size_t ProtectedCheckpoint::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<ProtectedCheckpoint>() +
           detail::shared_allocation_bound<CheckpointMetadata>();
}
} // namespace crowdb::tree
