// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/crowdb-tree.h"

#include <chrono>
#include <map>

namespace crowdb::tree
{
MemTableBatch Crowdbtree::admit_batch()
{
    auto generation = generation_.enter();
    for (;;) {
        auto       table = current_active();
        const auto bound = contiguous_slot_.load(std::memory_order_acquire);
        if (table->try_enter()) {
            return {.table        = std::move(table),
                    .bound        = bound,
                    .stats        = {.measure_copy = metrics_.mt_version_copy_l != nullptr},
                    .generation   = std::move(generation),
                    .counters     = &memtable_counters_,
                    .copy_latency = metrics_.mt_version_copy_l};
        }
    }
}

void Crowdbtree::finish_batch(MemTableBatch &batch, uint64_t slot, const std::vector<std::string> &keys)
{
    note_applied_slot(slot);
    batch.completed  = true;
    const auto bound = contiguous_slot_.load(std::memory_order_acquire);
    if (!batch.table->validate_open()) {
        return;
    }
    try {
        for (const auto &key : keys) {
            batch.table->prune(Slice(key), bound, &batch.stats);
        }
    }
    catch (const std::bad_alloc &) {
        // Publication already succeeded. Optional compaction may be retried by
        // a later writer; allocation failure cannot make this slot incomplete.
        return;
    }
}

void Crowdbtree::apply_batch(uint64_t slot, const Batch &batch)
{
    // Intra-batch: last occurrence wins (all ops share `slot`).
    auto                          apply_t0 = std::chrono::steady_clock::now();
    std::map<std::string, buffer> latest; // key -> single-alloc encoded cell buffer
    for (const auto &op : batch.ops) {
        latest[op.key] = encode_cell_buf(slot, op.kind, Slice(op.value));
    }
    auto admission      = admit_batch();
    admission.stats.gap = slot > admission.bound && slot - admission.bound > 1;
    std::vector<std::string> keys;
    keys.reserve(latest.size());
    while (!latest.empty()) {
        auto node = latest.extract(latest.begin());
        keys.push_back(node.key());
        admission.table->upsert(Slice(node.key()), slot, std::move(node.mapped()), admission.bound, &admission.stats);
        mt_upsert_total_.fetch_add(1, std::memory_order_relaxed);
    }
    finish_batch(admission, slot, keys);
    if (metrics_.mt_apply_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - apply_t0).count();
        metrics_.mt_apply_l->observe(static_cast<uint64_t>(ns));
    }
}

void Crowdbtree::recompute_contiguous_locked()
{
    // Fold received slots that extend the frontier one-by-one, then prune the
    // tracker below the (possibly advanced) frontier so it stays bounded.
    uint64_t cur = contiguous_slot_.load();
    auto     it  = received_slots_.upper_bound(cur);
    while (it != received_slots_.end() && *it == cur + 1) {
        cur = *it;
        ++it;
    }
    contiguous_slot_.store(cur);
    received_slots_.erase(received_slots_.begin(), received_slots_.upper_bound(cur));
}

void Crowdbtree::note_applied_slot(uint64_t slot)
{
    {
        std::scoped_lock lk(slot_mutex_);
        max_seen_slot_ = std::max(max_seen_slot_, slot);
        received_slots_.insert(slot);
        recompute_contiguous_locked();
    }
}

Status Crowdbtree::apply(uint64_t slot, const Batch &batch)
try {
    // Reject oversized keys before any state is mutated (plan-tree #15). A key
    // this large is assumed to be a caller bug; validating up front keeps apply
    // all-or-nothing.
    const size_t key_limit = max_key_size();
    for (const auto &op : batch.ops) {
        Status range_status = validate_key(Slice(op.key));
        if (!range_status.ok()) {
            return range_status;
        }
        if (op.key.size() > key_limit) {
            return Status::invalid_argument("key exceeds max_key_size (" + std::to_string(op.key.size()) + " > " +
                                            std::to_string(key_limit) + ")");
        }
    }
    apply_batch(slot, batch);
    maybe_swap_active();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("apply allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status Crowdbtree::apply_encoded(uint64_t slot, std::vector<encoded_op> ops)
try {
    // Same guard as apply() (plan-tree #15): validate every key before any
    // state is mutated.
    const size_t key_limit = max_key_size();
    for (const encoded_op &op : ops) {
        const CellView cell(op.cell.slice());
        if (!cell.valid() || cell.slot() != slot) {
            return Status::invalid_argument("encoded cell slot must match its batch");
        }

        Status range_status = validate_key(Slice(op.key));
        if (!range_status.ok()) {
            return range_status;
        }
        if (op.key.size() > key_limit) {
            return Status::invalid_argument("key exceeds max_key_size (" + std::to_string(op.key.size()) + " > " +
                                            std::to_string(key_limit) + ")");
        }
    }
    {
        auto admission      = admit_batch();
        admission.stats.gap = slot > admission.bound && slot - admission.bound > 1;
        std::vector<std::string> keys;
        keys.reserve(ops.size());
        // Intra-batch: last occurrence (vector order) wins, same as
        // apply_batch. Cells already come in pre-encoded (single alloc at
        // the caller's boundary, e.g. the C API -- plan-tree #5 B2d) --
        // move key+cell straight down, no encode_cell_buf call here.
        std::map<std::string, buffer> latest;
        for (encoded_op &op : ops) {
            latest[std::move(op.key)] = std::move(op.cell);
        }
        while (!latest.empty()) {
            auto node = latest.extract(latest.begin());
            keys.push_back(node.key());
            admission.table->upsert(Slice(node.key()), slot, std::move(node.mapped()), admission.bound,
                                    &admission.stats);
            mt_upsert_total_.fetch_add(1, std::memory_order_relaxed);
        }
        finish_batch(admission, slot, keys);
    }
    maybe_swap_active();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("apply allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status Crowdbtree::apply_external(uint64_t slot, std::vector<external_op> ops)
try {
    // Same guard as apply_encoded: validate every key before any state mutation.
    const size_t key_limit = max_key_size();
    for (const external_op &op : ops) {
        Status range_status = validate_key(Slice(op.key));
        if (!range_status.ok()) {
            return range_status;
        }
        if (op.key.size() > key_limit) {
            return Status::invalid_argument("key exceeds max_key_size (" + std::to_string(op.key.size()) + " > " +
                                            std::to_string(key_limit) + ")");
        }
    }
    {
        auto admission      = admit_batch();
        admission.stats.gap = slot > admission.bound && slot - admission.bound > 1;
        std::vector<std::string> keys;
        keys.reserve(ops.size());
        // Intra-batch: last occurrence (vector order) wins, same as apply_encoded.
        // Track {flags, value} per key; the value buffer is moved straight down
        // (no encode_cell_buf, no value memcpy).
        std::map<std::string, std::pair<uint8_t, buffer>> latest;
        for (external_op &op : ops) {
            latest[std::move(op.key)] = {op.flags, std::move(op.value)};
        }
        while (!latest.empty()) {
            auto node = latest.extract(latest.begin());
            keys.push_back(node.key());
            admission.table->upsert_external(Slice(node.key()), slot, node.mapped().first,
                                             std::move(node.mapped().second), admission.bound, &admission.stats);
            mt_upsert_total_.fetch_add(1, std::memory_order_relaxed);
        }
        finish_batch(admission, slot, keys);
    }
    maybe_swap_active();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("apply allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

void Crowdbtree::force_advance_slot(uint64_t slot)
{
    auto admission      = admit_batch();
    admission.stats.gap = slot > admission.bound && slot - admission.bound > 1;
    {
        std::scoped_lock lk(slot_mutex_);
        max_seen_slot_ = std::max(max_seen_slot_, slot);
        // Treat any gap up to `slot` as NoOps: jump the frontier, then fold in any
        // already-received slots that are now contiguous with it.
        if (slot > contiguous_slot_.load()) {
            contiguous_slot_.store(slot);
        }
        recompute_contiguous_locked();
    }
    admission.completed = true;
    maybe_swap_active();
}

} // namespace crowdb::tree
