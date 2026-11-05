// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/recovered_state_internal.h"

namespace crowdb::tree
{
namespace
{
bool valid_recovered_cell(Slice raw, uint64_t coverage)
{
    const CellView cell(raw);
    return cell.valid() && cell.slot() <= coverage &&
           (cell.flags() == 0 || cell.flags() == kFlagTombstone || cell.flags() == kFlagOverflow) &&
           (!cell.is_tombstone() || raw.size() == kCellHeaderSize) &&
           (!cell.is_overflow() ||
            (raw.size() == kOverflowCellSize && cell.overflow_head() != kInvalidPageId && cell.overflow_len() != 0));
}
} // namespace

void RecoveredStateOperation::State::leaf_step()
{
    auto  &top  = path.back();
    size_t work = 0;
    while (top.index < top.leaf->count() && work++ < limits.max_step_entries) {
        const auto key = top.leaf->key(top.index);
        const auto raw = top.leaf->cell(top.index);
        if (key.size() > limits.max_key_bytes - top.key_bytes) {
            finish(Status::resource_exhausted());
            return;
        }
        top.key_bytes += key.size();
        if (!top.range.contains(key) || !valid_recovered_cell(raw, captured->coverage) ||
            (top.index != 0 && top.leaf->key(top.index - 1) >= key)) {
            finish(Status::corruption());
            return;
        }
        const CellView cell(raw);
        const uint64_t primary_bytes = key.size() + raw.size() + kLeafSlotSize;
        if (primary_bytes > UINT64_MAX - top.metrics.page_size_bytes) {
            finish(Status::resource_exhausted());
            return;
        }
        top.metrics.page_size_bytes += primary_bytes;
        if (!cell.is_tombstone()) {
            const uint64_t value_bytes = cell.is_overflow() ? cell.overflow_len() : cell.value().size();
            if (key.size() > UINT64_MAX - value_bytes || key.size() + value_bytes > UINT64_MAX - top.metrics.kv_bytes) {
                finish(Status::resource_exhausted());
                return;
            }
            ++top.metrics.kv_count;
            top.summary.live_key_bytes += key.size();
            top.summary.live_value_bytes += value_bytes;
            top.metrics.kv_bytes += key.size() + value_bytes;
        }
        ++top.index;
    }
    if (top.index == top.leaf->count()) {
        if (top.index != 0) {
            top.bounds.lower      = OwnedKey(top.leaf->key(0), metadata);
            top.bounds.upper      = OwnedKey(top.leaf->key(top.index - 1), metadata);
            top.bounds.lower_leaf = top.pid;
            top.bounds.upper_leaf = top.pid;
        }
        top.metrics.page_size_estimate_bytes = top.metrics.page_size_bytes;
        top.summary.live_counts_known        = true;
        top.summary.encoded_entries          = top.index;
        top.summary.live_keys                = top.metrics.kv_count;
        top.summary.primary_used_bytes       = top.metrics.page_size_bytes;
        top.phase                            = Phase::Dependencies;
    }
    wake();
}

void RecoveredStateOperation::State::dependency_step()
{
    auto       &top        = path.back();
    const auto &references = *top.descriptor->stored_overflow;
    size_t      work       = 0;
    while (top.dependency < references.size() && work++ < limits.max_step_entries) {
        if (dependency_steps++ == limits.max_dependency_steps) {
            finish(Status::resource_exhausted());
            return;
        }
        if (top.chain_pid == kInvalidPageId) {
            const auto &reference = references[top.dependency];
            top.chain_pid         = reference.head;
            top.chain_bytes       = reference.bytes;
        }
        const auto found = captured->frames.find(top.chain_pid);
        if (top.chain_pid >= captured->next_page_id || found == captured->frames.end()) {
            finish(Status::corruption());
            return;
        }
        const auto &frame = found->second;
        if (frame.page_id != top.chain_pid || frame.type != page_type::kOverflowFrame || !frame.backing ||
            (!frame.backing->readable() || (require_durable && !frame.backing->reloadable())) ||
            frame.backing->backend_id != captured->backend_id || frame.overflow_bytes == 0 ||
            frame.overflow_bytes > top.chain_bytes ||
            (frame.next_overflow == kInvalidPageId) != (frame.overflow_bytes == top.chain_bytes)) {
            finish(Status::corruption());
            return;
        }
        reached.insert(top.chain_pid);
        top.chain_bytes -= frame.overflow_bytes;
        top.chain_pid = frame.next_overflow;
        if (top.chain_bytes == 0) {
            ++top.dependency;
        }
    }
    if (top.dependency == references.size()) {
        top.phase = Phase::Finish;
    }
    wake();
}
} // namespace crowdb::tree
