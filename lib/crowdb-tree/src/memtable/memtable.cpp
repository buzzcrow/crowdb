// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// B+tree memtable implementation.

#include "crowdb-tree/memtable/memtable.h"

#include <algorithm>
#include <cstring>
#include <utility>

namespace crowdb::tree
{

namespace
{
buffer buf_of(Slice s)
{
    buffer b = buffer::alloc(s.size());
    if (!s.empty()) {
        std::memcpy(b.data(), s.data(), s.size());
    }
    return b;
}

// Materialize a contiguous [header][value] cell from a CellVersion (R30 split
// cell support). Contiguous: clone. Split (kExternal): build header from
// slot/flags + copy value.
buffer materialize_cell(const CellVersion *cv)
{
    if (cv->cell.ownership() != buffer::mode::kExternal) {
        return cv->cell.clone();
    }
    size_t   vlen = cv->cell.size();
    buffer   b    = buffer::alloc(vlen, kCellHeaderSize);
    uint8_t *p    = b.data();
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<uint8_t>((cv->slot >> (8 * i)) & 0xff);
    }
    p[8] = cv->flags;
    if (vlen > 0) {
        std::memcpy(b.data() + kCellHeaderSize, cv->cell.data(), vlen);
    }
    return b;
}
} // namespace

bool MemTable::upsert(Slice key, uint64_t slot, Slice cell_payload)
{
    return upsert(key, slot, buf_of(cell_payload));
}

bool MemTable::upsert(Slice key, uint64_t slot, buffer &&cell_payload, uint64_t bound, MutationStats *stats)
{
    if (stats != nullptr) {
        stats->admission = &writers_;
    }
    if (slot <= durable_floor_.load(std::memory_order_relaxed) && !allow_old_slots_.load(std::memory_order_relaxed)) {
        return false;
    }
    CellView cv{cell_payload.slice()};
    uint64_t entry_slot = cv.valid() ? cv.slot() : slot;
    uint8_t  flags      = cv.valid() ? cv.flags() : 0;
    auto ver = std::make_shared<CellVersion>(std::move(cell_payload), entry_slot, flags, epoch_->memtable_allocation());
    if (!list_.upsert(key, std::move(ver), bound, stats)) {
        return false;
    }
    update_slot_range(entry_slot);
    return true;
}

bool MemTable::upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer &&value, uint64_t bound,
                               MutationStats *stats)
{
    if (stats != nullptr) {
        stats->admission = &writers_;
    }
    if (slot <= durable_floor_.load(std::memory_order_relaxed) && !allow_old_slots_.load(std::memory_order_relaxed)) {
        return false;
    }
    // upsert_external stores the raw value (no 9-byte cell header); the
    // header is reconstructed from slot/flags at read time. Tag non-kExternal
    // buffers as kExternal so get_view/scan treat them as split cells.
    if (value.ownership() != buffer::mode::kExternal) {
        if (!value.empty()) {
            auto *heap = new buffer(std::move(value));
            value      = buffer::wrap_external(heap->data(), heap->size(), heap,
                                               [](void *ctx) { delete static_cast<buffer *>(ctx); });
        }
        else {
            value = buffer::wrap_external(nullptr, 0, nullptr, nullptr);
        }
    }
    auto ver = std::make_shared<CellVersion>(std::move(value), slot, flags, epoch_->memtable_allocation());
    if (!list_.upsert(key, std::move(ver), bound, stats)) {
        return false;
    }
    update_slot_range(slot);
    return true;
}

void MemTable::set_durable_floor(uint64_t slot)
{
    uint64_t cur = durable_floor_.load(std::memory_order_relaxed);
    while (slot > cur && !durable_floor_.compare_exchange_weak(cur, slot, std::memory_order_relaxed)) {
        // cur is refreshed by CAS failure
    }
}

std::vector<mem_entry> MemTable::snapshot(uint64_t frontier) const
{
    auto                   guard = epoch_->enter();
    std::vector<mem_entry> out;
    auto                   cur = list_.prefix_cursor(frontier);
    while (cur.valid()) {
        const CellVersion *cv = cur.cell_version();
        if (cv != nullptr) {
            auto &entry = out.emplace_back();
            entry.key   = cur.key().to_string();
            entry.cell  = materialize_cell(cv);
            entry.slot  = cv->slot;
        }
        cur.advance();
    }
    return out;
}

} // namespace crowdb::tree
