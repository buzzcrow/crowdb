// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/c_api.h"

extern "C" {
struct ct_apply_permit;
// Copies and charges the complete raw batch, but retains no Tree close ticket.
// gap_repair selects the independent replay/follower progress partition.
int32_t ct_apply_prepare(ct_tree *tree, const ct_kv_ref *operations, size_t count, int32_t gap_repair,
                         ct_apply_permit **out);
// Call before allowing consensus to choose. Pins the prepared generation;
// dropping an unsubmitted armed permit releases its accepted-operation tickets.
int32_t ct_apply_arm(ct_apply_permit *permit);
// Transfer an armed permit into one already chosen slot. The future handle was
// allocated by prepare; freeing either handle cannot cancel accepted work.
// A future completes exactly once via ct_future_poll.
// Each handle may move between threads; calls on one handle are serialized.
int32_t ct_apply_submit(ct_apply_permit *permit, uint64_t slot, ct_future **out);
// Reader returns immutable caller-owned bytes for index < count. Calls on the
// reader are serialized, may occur on runtime workers, and must not throw.
using ct_replay_read = void (*)(void *context, size_t index, ct_kv_ref *out);
// Consume context, invoking drop_context once even on immediate errors. The
// optional owner_control_bytes is the actual allocation of the foreign bridge
// owner, charged as Metadata until drop_context frees it; input storage remains
// caller-owned until native makes its charged copy. No descriptor array is made.
// Accepted work awaits its complete gap grant even after future handle release.
int32_t ct_apply_replay(ct_tree *tree, uint64_t slot, size_t count, ct_replay_read read, void (*drop_context)(void *),
                        void *context, size_t owner_control_bytes, ct_future **out);
void    ct_apply_permit_free(ct_apply_permit *permit);
}
