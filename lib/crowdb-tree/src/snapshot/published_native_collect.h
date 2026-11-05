// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/published_native.h"

namespace crowdb::tree
{
// Compatibility copy boundary. During construction all capacities are charged.
// On successful publication the outer vector belongs to the caller; each raw
// frame keeps its own engine payload charge through allocation_owner.
Status collect_published_frames(const std::shared_ptr<TreeCore> &core, size_t max_bytes, std::vector<NativeFrame> *out,
                                NativeStreamInfo *info);
} // namespace crowdb::tree
