// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "published_state.h"

namespace crowdb::tree
{
class CheckpointPendingMetrics;
Status validate_checkpoint(const PublicationTransaction &transaction, const PublishedTreeState &current,
                           const CheckpointPendingMetrics &pending);
} // namespace crowdb::tree
