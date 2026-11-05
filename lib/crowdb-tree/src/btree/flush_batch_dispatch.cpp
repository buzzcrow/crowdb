// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_batch_internal.h"

namespace crowdb::tree
{
thread_local FlushBatch::State::DispatchScope *FlushBatch::State::DispatchScope::active = nullptr;

FlushBatch::State::DispatchScope::DispatchScope(State &state)
    : owner(state),
      remaining(state.options.inline_ranges_per_step),
      previous(active)
{
    active = this;
    owner.publisher.work_metrics()->dispatch.begin();
}

FlushBatch::State::DispatchScope::~DispatchScope()
{
    owner.publisher.work_metrics()->dispatch.finish();
    active = previous;
}

bool FlushBatch::State::DispatchScope::take(State &state) noexcept
{
    if (active == nullptr || &active->owner != &state || active->remaining == 0) {
        return false;
    }
    --active->remaining;
    return true;
}
} // namespace crowdb::tree
