// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "snapshot/commit_anchor.h"

#include "crowdb-common/crc32c.h"

#include <algorithm>

namespace crowdb::tree::snapshot
{
namespace
{
constexpr size_t kAnchorFixedFields = 4 + 4 + (8 * 4) + 4 + 8 + 4 + 4 + 4 + 8 + 8 + 4;

void put_u32(std::vector<uint8_t> *out, uint32_t v)
{
    for (int i = 0; i < 4; ++i) {
        out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
    }
}

void put_u64(std::vector<uint8_t> *out, uint64_t v)
{
    for (int i = 0; i < 8; ++i) {
        out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
    }
}

uint32_t get_u32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<uint32_t>(p[i]) << (8 * i);
    }
    return v;
}

uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
    return v;
}

} // namespace

void encode_anchor(const CommitAnchor &anchor, uint64_t slot_bytes, std::vector<uint8_t> *out)
{
    put_u32(out, anchor.magic);
    put_u32(out, anchor.format_version);
    put_u64(out, anchor.snapshot_seq);
    put_u64(out, anchor.root_page_id);
    put_u64(out, anchor.last_applied_slot);
    put_u64(out, anchor.next_page_id);
    put_u32(out, anchor.segment_slots);
    put_u64(out, anchor.segdir_addr);
    put_u32(out, anchor.segdir_len);
    put_u32(out, anchor.segdir_crc);
    put_u64(out, anchor.leaf_count);
    put_u64(out, anchor.inner_count);
    uint32_t crc = crowdb::common::crc32c(out->data(), out->size());
    put_u32(out, crc);
    out->resize(slot_bytes, 0); // zero pad to the IU-aligned slot size
}

namespace
{
bool decode_anchor(const uint8_t *buf, CommitAnchor *a)
{
    if (get_u32(buf) != kAnchorMagic) {
        return false;
    }
    uint32_t stored_crc = get_u32(buf + (kAnchorFixedFields - 4));
    if (crowdb::common::crc32c(buf, kAnchorFixedFields - 4) != stored_crc) {
        return false;
    }
    a->magic          = get_u32(buf);
    a->format_version = get_u32(buf + 4);
    if (a->format_version != kFormatVersion) {
        // Clean-break format: no older format to accept.
        return false;
    }
    a->snapshot_seq      = get_u64(buf + 8);
    a->root_page_id      = get_u64(buf + 16);
    a->last_applied_slot = get_u64(buf + 24);
    a->next_page_id      = get_u64(buf + 32);
    a->segment_slots     = get_u32(buf + 40);
    a->segdir_addr       = get_u64(buf + 44);
    a->segdir_len        = get_u32(buf + 52);
    a->segdir_crc        = get_u32(buf + 56);
    a->leaf_count        = get_u64(buf + 60);
    a->inner_count       = get_u64(buf + 68);
    return true;
}
} // namespace

Status read_valid_anchors(const PageStore &store, uint32_t iu, std::vector<CommitAnchor> *out)
{
    const uint64_t slot_bytes = superblock_slot_bytes(iu);
    out->clear();
    for (uint64_t slot : {static_cast<uint64_t>(0), slot_bytes}) {
        if (slot + slot_bytes > store.size()) {
            continue;
        }
        std::vector<uint8_t> buf(slot_bytes);
        Status               status = store.read_at(slot, buf.data(), buf.size());
        if (status.code() == Code::kNotFound) {
            // A growing block file may not have written the other A/B slot yet.
            continue;
        }
        if (!status.ok()) {
            return status;
        }
        CommitAnchor a;
        if (!decode_anchor(buf.data(), &a)) {
            continue;
        }
        out->push_back(a);
    }
    std::ranges::sort(*out, {}, &CommitAnchor::snapshot_seq);
    return Status::Ok();
}

} // namespace crowdb::tree::snapshot
