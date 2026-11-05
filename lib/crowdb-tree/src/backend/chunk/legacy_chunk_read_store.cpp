// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "legacy_chunk_read_store.h"

#include "chunk_frame.h"

#include <algorithm>
#include <bit>
#include <cstring>

namespace crowdb::tree::detail
{
namespace
{
// A stable migration pin is idempotent across lost replies and restart. It is
// released explicitly only after the migration outcome is known.
constexpr uint64_t kMigrationPin = 0x43544d4947524154ULL;
} // namespace

LegacyChunkReadStore::LegacyChunkReadStore(std::shared_ptr<RootCatalog>           catalog,
                                           std::shared_ptr<ChunkTransport>        transport,
                                           std::shared_ptr<AllocationReservation> metadata, uint64_t tree,
                                           uint64_t epoch, uint32_t iu_size, uint32_t mirrors)
    : catalog_(std::move(catalog)),
      transport_(std::move(transport)),
      metadata_(std::move(metadata)),
      manifest_(MetadataAllocator<uint8_t>(metadata_)),
      tree_(tree),
      epoch_(epoch),
      iu_size_(iu_size),
      mirrors_(mirrors)
{
}

Status LegacyChunkReadStore::open(uint64_t expected_generation)
{
    if (tree_ == 0 || epoch_ == 0 || expected_generation == 0 || expected_generation == UINT64_MAX || !catalog_ ||
        catalog_->protection() == nullptr || !transport_ || !metadata_ || !std::has_single_bit(iu_size_) ||
        mirrors_ == 0 || mirrors_ > kMaxMirrorCopies || pinned_) {
        return Status::invalid_argument();
    }
    auto *protection = catalog_->protection();
    auto  status     = protection->check_read_owner();
    if (status.ok()) {
        status = protection->load_root_owned(tree_, 0, &manifest_);
    }
    if (status.ok()) {
        status = LegacyChunkLayout::parse(manifest_, tree_, epoch_, &layout_);
    }
    if (!status.ok()) {
        return status;
    }
    if (layout_.generation() != expected_generation) {
        return Status::unavailable("legacy Chunk generation changed before migration");
    }
    status = catalog_->pin_generation(tree_, kMigrationPin, expected_generation, expected_generation);
    if (!status.ok()) {
        return status;
    }
    pinned_ = true;
    ChunkRootBytes current{MetadataAllocator<uint8_t>(metadata_)};
    status = protection->load_root_owned(tree_, 0, &current);
    if (!status.ok() || current != manifest_) {
        return status.ok() ? Status::unavailable("legacy Chunk root changed while pinning") : status;
    }
    for (size_t i = 0; i < layout_.segment_count(); ++i) {
        const auto descriptor = layout_.segment(i);
        current.clear();
        status = catalog_->load_legacy_reference_owned(descriptor.owner_tree_id, descriptor.object_id, &current);
        if (status.ok()) {
            status = layout_.validate_segment(i, current);
        }
        if (!status.ok()) {
            return status;
        }
    }
    status    = protection->check_read_owner();
    verified_ = status.ok();
    return status;
}

Status LegacyChunkReadStore::release_pin()
{
    if (!pinned_) {
        return Status::Ok();
    }
    auto status = catalog_->unpin_generation(tree_, kMigrationPin, layout_.generation());
    if (status.ok()) {
        pinned_   = false;
        verified_ = false;
    }
    return status;
}

Status LegacyChunkReadStore::read_at(uint64_t offset, uint8_t *data, size_t length) const
try {
    if (!pinned_ || !verified_ || (data == nullptr && length != 0)) {
        return Status::invalid_argument();
    }
    if (offset > size() || length > size() - offset) {
        return Status::not_found();
    }
    auto status = catalog_->protection()->check_read_owner();
    if (!status.ok() || length == 0) {
        return status;
    }
    ReservedVector<uint8_t, AllocationKind::kIo> scratch(protocol::kMaxFrameBytes,
                                                         ReservationAllocator<uint8_t, AllocationKind::kIo>(metadata_));
    size_t                                       lower = 0;
    size_t                                       upper = layout_.pack_count();
    while (lower < upper) {
        const auto middle = lower + ((upper - lower) / 2);
        const auto pack   = layout_.pack(middle);
        if (pack.logical_offset + pack.ref.length <= offset) {
            lower = middle + 1;
        }
        else {
            upper = middle;
        }
    }
    size_t copied = 0;
    for (size_t index = lower; copied < length && index < layout_.pack_count(); ++index) {
        const auto pack     = layout_.pack(index);
        const auto position = offset + copied;
        if (pack.logical_offset > position) {
            return Status::not_found();
        }
        const auto begin = position - pack.logical_offset;
        const auto bytes = std::min<uint64_t>(length - copied, pack.ref.length - begin);
        status           = read_pack(pack.ref, begin, {data + copied, static_cast<size_t>(bytes)}, scratch);
        if (!status.ok()) {
            return status;
        }
        copied += bytes;
    }
    return copied == length ? catalog_->protection()->check_read_owner() : Status::not_found();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status LegacyChunkReadStore::read_pack(const ChunkPageRef &ref, uint64_t offset, std::span<uint8_t> output,
                                       std::span<uint8_t> scratch) const
{
    while (!output.empty()) {
        const auto frame_begin = (offset / protocol::kMaxFrameV2PayloadBytes) * protocol::kMaxFrameV2PayloadBytes;
        const auto frame_bytes = std::min<uint64_t>(protocol::kMaxFrameV2PayloadBytes, ref.length - frame_begin);
        std::span<const uint8_t> payload;
        auto                     status = read_frame(ref, frame_begin, frame_bytes, scratch, &payload);
        if (!status.ok()) {
            return status;
        }
        const auto begin = offset - frame_begin;
        const auto bytes = std::min(output.size(), payload.size() - begin);
        std::memcpy(output.data(), payload.data() + begin, bytes);
        offset += bytes;
        output = output.subspan(bytes);
    }
    return Status::Ok();
}

Status LegacyChunkReadStore::read_frame(const ChunkPageRef &ref, size_t logical_offset, size_t logical_bytes,
                                        std::span<uint8_t> scratch, std::span<const uint8_t> *payload) const
{
    const auto physical_offset = (logical_offset / protocol::kMaxFrameV2PayloadBytes) * protocol::kMaxFrameBytes;
    const auto bytes           = logical_bytes + protocol::kFrameV2HeaderBytes + protocol::kFrameFooterBytes;
    bool       responded       = false;
    for (uint32_t mirror = 0; mirror < mirrors_; ++mirror) {
        auto status =
            transport_->read_mirror(ref.chunk_id, mirror, ref.offset + physical_offset, scratch.data(), bytes);
        if (!status.ok()) {
            continue;
        }
        responded = true;
        protocol::ParsedFrame frame;
        const auto            error =
            protocol::parse_frame(scratch.first(bytes), {.high = ref.chunk_id.high, .low = ref.chunk_id.low}, &frame);
        if (error == protocol::FrameError::Ok &&
            frame.header.magic == chunk_page_frame_magic(chunk_purpose(ref.chunk_id)) &&
            frame.header.payload_offset == protocol::kFrameV2HeaderBytes && frame.payload.size() == logical_bytes &&
            frame.physical_length == bytes) {
            *payload = frame.payload;
            return Status::Ok();
        }
    }
    return responded ? Status::corruption("legacy Chunk frame checksum or identity mismatch")
                     : Status::unavailable("legacy Chunk frame mirrors unavailable");
}

Status LegacyChunkReadStore::write_at(uint64_t /*offset*/, const uint8_t * /*data*/, size_t /*length*/)
{
    return Status::not_supported("legacy Chunk migration source is immutable");
}

Status LegacyChunkReadStore::sync()
{
    return Status::not_supported();
}
} // namespace crowdb::tree::detail
