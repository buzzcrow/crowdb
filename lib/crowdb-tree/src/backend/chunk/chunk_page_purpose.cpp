// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_page_store.h"
#include "crowdb-protocol/frame.h"

#include <cstring>
#include <limits>

namespace crowdb::tree::detail
{
namespace
{
constexpr uint64_t kAnchorSlotBytes = 4096;
}

Status ChunkPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    return write_typed_at(PagePurpose::kBtreePage, off, buf, len);
}

Status ChunkPageStore::write_typed_at(PagePurpose purpose, uint64_t off, const uint8_t *buf, size_t len)
{
    if (!valid_page_purpose(purpose)) {
        return Status::invalid_argument("unsupported page purpose");
    }
    if (buf == nullptr && len != 0) {
        return Status::invalid_argument("chunk page write has null buffer");
    }
    if (off > std::numeric_limits<size_t>::max() || len > std::numeric_limits<size_t>::max() - off) {
        return Status::resource_exhausted("chunk page write exceeds address space");
    }
    if (!staged_initialized_) {
        Status status = materialize_active(&staged_);
        if (!status.ok()) {
            return status;
        }
        staged_purposes_.clear();
        auto base = reuse_base_manifest();
        if (base != nullptr) {
            for (const auto &pack : base->packs) {
                staged_purposes_.assign(pack.logical_offset, pack.ref.length, chunk_purpose(pack.ref.chunk_id));
            }
        }
        staged_initialized_ = true;
    }
    const size_t end = static_cast<size_t>(off) + len;
    if (end > staged_.size()) {
        staged_.resize(end, 0);
    }
    if (len != 0) {
        std::memcpy(staged_.data() + off, buf, len);
        dirty_ranges_.emplace_back(off, len);
        staged_purposes_.assign(off, len, purpose);
    }
    const uint64_t anchor_region_bytes = round_up_to_iu(kAnchorSlotBytes, config_.iu_size) * 2;
    if (end > anchor_region_bytes) {
        data_durable_ = false;
    }
    if (off < anchor_region_bytes) {
        anchor_dirty_ = true;
    }
    return Status::Ok();
}

size_t ChunkPageStore::staged_pack_length(uint64_t offset, const ChunkManifest *base) const
{
    auto length =
        staged_purposes_.span(offset, std::min(config_.pack_bytes, staged_.size() - static_cast<size_t>(offset)));
    if (base != nullptr) {
        for (const auto &pack : base->packs) {
            if (pack.logical_offset > offset) {
                length = std::min<uint64_t>(length, pack.logical_offset - offset);
                break;
            }
            if (pack.logical_offset + pack.ref.length > offset) {
                length = std::min<uint64_t>(length, pack.logical_offset + pack.ref.length - offset);
                break;
            }
        }
    }
    return length;
}

bool ChunkPageStore::range_has_pack(const ChunkManifest &base, uint64_t offset, size_t length)
{
    return std::any_of(base.packs.begin(), base.packs.end(), [&](const auto &pack) {
        return pack.logical_offset < offset + length && offset < pack.logical_offset + pack.ref.length;
    });
}

uint64_t ChunkPageStore::chunk_allocation_bytes() const
{
    const auto packs = (config_.max_chunk_bytes + config_.pack_bytes - 1) / config_.pack_bytes;
    return packs * crowdb::protocol::framed_physical_length(config_.pack_bytes);
}

} // namespace crowdb::tree::detail
