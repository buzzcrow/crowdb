// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_stage.h"
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/maptable/frame_page.h"

namespace crowdb::tree::detail
{
namespace
{
template <class Bytes> Status encode_record(const ChunkStageRecord &record, Bytes *out)
{
    if (out == nullptr) {
        return Status::invalid_argument();
    }
    auto status = validate_stage_record(record);
    if (!status.ok()) {
        return status;
    }
    out->assign(100 + (16 * record.packs.size()), 0);
    auto *data = out->data();
    frame_put_u32(data, 0, 0x47535443);
    frame_put_u32(data, 4, 1);
    const uint64_t values[] = {record.tree_id,          record.id.owner_epoch, record.id.operation_high,
                               record.id.operation_low, record.revision,       record.chunk_high,
                               record.chunk_low,        record.capacity,       record.manifest_generation};
    for (size_t i = 0; i < 9; ++i) {
        frame_put_u64(data, 8 + (i * 8), values[i]);
    }
    frame_put_u32(data, 80, record.purpose);
    frame_put_u32(data, 84, static_cast<uint32_t>(record.state));
    frame_put_u32(data, 88, static_cast<uint32_t>(record.packs.size()));
    for (size_t i = 0; i < record.packs.size(); ++i) {
        frame_put_u64(data, 96 + (16 * i), record.packs[i].offset);
        frame_put_u32(data, 104 + (16 * i), record.packs[i].length);
        frame_put_u32(data, 108 + (16 * i), record.packs[i].checksum);
    }
    frame_put_u32(data, out->size() - 4, common::crc32c(data, out->size() - 4));
    return Status::Ok();
}

} // namespace

Status encode_stage_record(const ChunkStageRecord &record, std::vector<uint8_t> *out)
{
    return encode_record(record, out);
}

Status encode_stage_record_owned(const ChunkStageRecord &record, ChunkStageBytes *out)
{
    return encode_record(record, out);
}

Status decode_stage_record(std::span<const uint8_t> bytes, ChunkStageRecord *out)
{
    const auto *data = bytes.data();
    if (out == nullptr || bytes.size() < 100 || bytes.size() > kMaxStageBytes || frame_u32(data, 0) != 0x47535443) {
        return Status::corruption("invalid stage record header");
    }
    if (frame_u32(data, 4) != 1) {
        return Status::not_supported("stage record version");
    }
    const auto count = frame_u32(data, 88);
    const auto state = frame_u32(data, 84);
    if (state < 1 || state > 7 || count > kMaxStagePacks || bytes.size() != 100 + (16 * count) ||
        frame_u32(data, 92) != 0 || common::crc32c(data, bytes.size() - 4) != frame_u32(data, bytes.size() - 4)) {
        return Status::corruption("invalid stage record checksum or length");
    }
    ChunkStageRecord record{
        .tree_id             = frame_u64(data, 8),
        .id                  = {.owner_epoch    = frame_u64(data, 16),
                                .operation_high = frame_u64(data, 24),
                                .operation_low  = frame_u64(data, 32)},
        .revision            = frame_u64(data, 40),
        .chunk_high          = frame_u64(data, 48),
        .chunk_low           = frame_u64(data, 56),
        .capacity            = frame_u64(data, 64),
        .manifest_generation = frame_u64(data, 72),
        .purpose             = frame_u32(data, 80),
        .state               = static_cast<ChunkStageState>(state),
        .packs               = decltype(ChunkStageRecord::packs)(out->packs.get_allocator())
    };
    record.packs.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        record.packs.push_back({.offset   = frame_u64(data, 96 + (16 * i)),
                                .length   = frame_u32(data, 104 + (16 * i)),
                                .checksum = frame_u32(data, 108 + (16 * i))});
    }
    if (!validate_stage_record(record).ok()) {
        return Status::corruption("invalid stage record fields");
    }
    *out = std::move(record);
    return Status::Ok();
}

Status encode_stage_list(std::span<const ChunkStageRecord> records, std::vector<uint8_t> *out)
{
    if (out == nullptr || records.size() > kMaxStageScan) {
        return Status::invalid_argument();
    }
    out->assign(4, 0);
    frame_put_u32(out->data(), 0, static_cast<uint32_t>(records.size()));
    for (const auto &record : records) {
        std::vector<uint8_t> encoded;
        auto                 status = encode_stage_record(record, &encoded);
        if (!status.ok()) {
            return status;
        }
        const auto at = out->size();
        out->resize(at + 4);
        frame_put_u32(out->data(), at, static_cast<uint32_t>(encoded.size()));
        out->insert(out->end(), encoded.begin(), encoded.end());
    }
    return Status::Ok();
}

Status decode_stage_list(std::span<const uint8_t> bytes, std::vector<ChunkStageRecord> *out)
{
    if (out == nullptr || bytes.size() < 4 || bytes.size() > 4 + (kMaxStageScan * (4 + kMaxStageBytes))) {
        return Status::corruption();
    }
    const auto count = frame_u32(bytes.data(), 0);
    if (count > kMaxStageScan) {
        return Status::corruption();
    }
    std::vector<ChunkStageRecord> records;
    records.reserve(count);
    size_t at = 4;
    for (size_t i = 0; i < count; ++i) {
        if (bytes.size() - at < 4) {
            return Status::corruption();
        }
        const auto length = frame_u32(bytes.data(), at);
        at += 4;
        if (length > bytes.size() - at) {
            return Status::corruption();
        }
        ChunkStageRecord record;
        auto             status = decode_stage_record(bytes.subspan(at, length), &record);
        if (!status.ok()) {
            return status;
        }
        records.push_back(std::move(record));
        at += length;
    }
    if (at != bytes.size()) {
        return Status::corruption();
    }
    *out = std::move(records);
    return Status::Ok();
}
} // namespace crowdb::tree::detail
