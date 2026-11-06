// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_codec_internal.h"
#include "snapshot/chunk_root_recovery.h"
#include "snapshot/stored_overflow.h"

namespace crowdb::tree
{
Status ChunkRootRecovery::index()
{
    if (index_ == root_.indexes.size()) {
        if (pages_->frames.size() != root_.frame_count || references_ != root_.stored_references) {
            return Status::corruption();
        }
        leaf_             = pages_->frames.begin();
        dependency_phase_ = true;
        return Status::Ok();
    }
    const auto &expected = root_.indexes[index_];
    if (expected.pack.length > kChunkIndexWindowBytes) {
        return Status::corruption();
    }
    detail::ChunkStageId pin;
    auto                 status = next_pin(&pin);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<const ChunkPackBacking> backing;
    status = pin_chunk_backing(domain_, expected.pack, pin, metadata_, &backing);
    if (!status.ok()) {
        return status;
    }
    index_bytes_.resize(expected.pack.length);
    status = read_chunk_backing(*backing, index_bytes_, window_);
    if (!status.ok()) {
        return status;
    }
    status = decode_chunk_index_header(index_bytes_, root_.identity, index_, &left_, &offset_);
    if (!status.ok()) {
        return status;
    }
    if (left_ != expected.frame_count) {
        return Status::corruption();
    }
    return Status::Ok();
}

Status ChunkRootRecovery::frame()
{
    ChunkIndexFrame record;
    auto            status = decode_chunk_index_frame(index_bytes_, &offset_, &record);
    if (!status.ok()) {
        return status;
    }
    const auto &expected = root_.indexes[index_];
    if ((have_pid_ && record.page_id <= last_pid_) || record.page_id >= root_.identity.next_page_id ||
        (left_ == expected.frame_count && record.page_id != expected.first_page) ||
        (left_ == 1 && (record.page_id != expected.last_page || offset_ != index_bytes_.size() - 4))) {
        return Status::corruption();
    }
    if (record.pack.length > limits_.max_blob_bytes || record.frame_bytes > limits_.max_frame_bytes) {
        return Status::resource_exhausted();
    }
    detail::ChunkStageId pin;
    status = next_pin(&pin);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<const ChunkPackBacking> pack;
    status = pin_chunk_backing(domain_, record.pack, pin, metadata_, &pack);
    if (!status.ok()) {
        return status;
    }
    auto backing              = detail::reserved_shared<BackingRef>(metadata_);
    backing->backend_id       = domain_->backend_id;
    backing->length           = record.pack.length;
    backing->generation       = domain_->epoch;
    backing->checksum         = record.pack.checksum;
    backing->durability       = BackingDurability::kCheckpointCommitted;
    backing->storage          = BackingStorage::kChunkPack;
    backing->owner            = pack;
    backing->physical_owner   = pack;
    backing->physical_metrics = pack->physical_metrics;
    backing->chunk            = std::move(pack);
    DurableCapturedFrame descriptor{.page_id         = record.page_id,
                                    .type            = record.type,
                                    .frame_bytes     = record.frame_bytes,
                                    .logical_bytes   = record.logical_bytes,
                                    .next_overflow   = record.next_overflow,
                                    .overflow_bytes  = record.overflow_bytes,
                                    .backing         = std::move(backing),
                                    .stored_overflow = {},
                                    .summary         = {}};
    blob_.resize(record.pack.length);
    status = read_chunk_backing(*descriptor.backing->chunk, blob_, window_);
    if (!status.ok()) {
        return status;
    }
    status = validate_frame(descriptor, record.stored_reference_words);
    if (!status.ok()) {
        return status;
    }
    pages_->frames.emplace(record.page_id, std::move(descriptor));
    max_frame_bytes_ = std::max<size_t>(max_frame_bytes_, record.frame_bytes);
    max_blob_bytes_  = std::max<size_t>(max_blob_bytes_, record.pack.length);
    last_pid_        = record.page_id;
    have_pid_        = true;
    if (--left_ == 0) {
        ++index_;
    }
    return Status::Ok();
}

Status ChunkRootRecovery::validate_frame(DurableCapturedFrame &descriptor, std::span<const uint8_t> words)
{
    if (durable_blob_raw_len(blob_.data(), blob_.size()) != descriptor.frame_bytes ||
        durable_blob_logical_len(blob_.data(), blob_.size()) != descriptor.logical_bytes) {
        return Status::corruption();
    }
    raw_.resize(descriptor.frame_bytes);
    auto status = decode_durable_page(blob_.data(), blob_.size(), raw_.data(), raw_.size());
    if (!status.ok()) {
        return status;
    }
    if (!frame_validate(raw_.data(), raw_.size()) || frame_u64(raw_.data(), fh::kSelfpage_id) != descriptor.page_id ||
        frame_page_type(raw_.data()) != descriptor.type) {
        return Status::corruption();
    }
    if (descriptor.type == page_type::kOverflowFrame) {
        const OverflowFrameView overflow(raw_.data(), raw_.size());
        return overflow.chunk_len() == descriptor.overflow_bytes && overflow.next_page_id() == descriptor.next_overflow
                 ? Status::Ok()
                 : Status::corruption();
    }
    if (descriptor.type != page_type::kLeafBase) {
        return Status::Ok();
    }
    const LeafFrameView leaf(raw_.data(), raw_.size());
    const uint64_t      cells  = uint64_t{leaf.count()} + leaf.delta_count();
    size_t              stored = 0;
    for (uint64_t i = 0; i < cells; ++i) {
        const auto     raw = i < leaf.count() ? leaf.cell(static_cast<uint32_t>(i))
                                              : leaf.delta_cell(static_cast<uint32_t>(i - leaf.count()));
        const CellView cell(raw);
        if (!cell.valid() || cell.slot() > pages_->coverage ||
            (cell.flags() != 0 && cell.flags() != kFlagTombstone && cell.flags() != kFlagOverflow) ||
            (cell.is_tombstone() && raw.size() != kCellHeaderSize)) {
            return Status::corruption();
        }
        stored += cell.is_overflow() ? 1 : 0;
    }
    if (stored != words.size() / 16 || stored > root_.stored_references - references_) {
        return Status::corruption();
    }
    status = capture_stored_overflow(leaf, kChunkIndexReferencesPerFrame, metadata_, &descriptor.stored_overflow);
    if (!status.ok()) {
        return status;
    }
    const auto &refs = *descriptor.stored_overflow;
    if (refs.size() != words.size() / 16 || refs.size() > root_.stored_references - references_) {
        return Status::corruption();
    }
    for (size_t i = 0; i < refs.size(); ++i) {
        const auto encoded = words.subspan(i * 16, 16);
        if (refs[i].head != chunk_codec::word(encoded, 0) || refs[i].bytes != chunk_codec::word(encoded, 1)) {
            return Status::corruption();
        }
    }
    references_ += refs.size();
    return Status::Ok();
}
} // namespace crowdb::tree
