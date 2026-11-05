// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/crc32c.h"
#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/snapshot/snapshot.h"
#include "crowdb-tree/snapshot/snapshot_io.h"

#include <algorithm>
#include <array>
#include <limits>

namespace crowdb::tree
{
namespace
{
constexpr uint32_t kSnapMagic   = 0x4E535443;
constexpr uint32_t kSnapVersion = 1;
constexpr size_t   kSnapHeader  = 4 + 4 + 1 + 8 + 8;
constexpr size_t   kSnapTrailer = 4;

void encode_u32(uint32_t v, std::array<uint8_t, 4> *out)
{
    for (int i = 0; i < 4; ++i) {
        (*out)[static_cast<size_t>(i)] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
    }
}

void encode_u64(uint64_t v, std::array<uint8_t, 8> *out)
{
    for (int i = 0; i < 8; ++i) {
        (*out)[static_cast<size_t>(i)] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
    }
}

template <typename Fn> void for_each_portable_piece(const Snapshot &snapshot, Fn &&fn)
{
    std::array<uint8_t, kSnapHeader> header{};
    std::array<uint8_t, 4>           u32{};
    std::array<uint8_t, 8>           u64{};
    encode_u32(kSnapMagic, &u32);
    std::copy(u32.begin(), u32.end(), header.begin());
    encode_u32(kSnapVersion, &u32);
    std::copy(u32.begin(), u32.end(), header.begin() + 4);
    header[8] = static_cast<uint8_t>(snapshot_format::kPortable);
    encode_u64(snapshot.at_slot(), &u64);
    std::copy(u64.begin(), u64.end(), header.begin() + 9);
    encode_u64(static_cast<uint64_t>(snapshot.entries().size()), &u64);
    std::copy(u64.begin(), u64.end(), header.begin() + 17);
    fn(header.data(), header.size());

    for (const leaf_entry &entry : snapshot.entries()) {
        CellView value{Slice(entry.cell)};
        encode_u32(static_cast<uint32_t>(entry.key.size()), &u32);
        fn(u32.data(), u32.size());
        fn(reinterpret_cast<const uint8_t *>(entry.key.data()), entry.key.size());
        encode_u64(value.slot(), &u64);
        fn(u64.data(), u64.size());
        const uint8_t kind = value.is_tombstone() ? 1 : 0;
        fn(&kind, 1);
        Slice bytes = value.value();
        encode_u32(static_cast<uint32_t>(bytes.size()), &u32);
        fn(u32.data(), u32.size());
        fn(reinterpret_cast<const uint8_t *>(bytes.data()), bytes.size());
    }
}

} // namespace

SnapshotExport::SnapshotExport(std::shared_ptr<Snapshot> snapshot, size_t chunk_bytes)
    : snapshot_(std::move(snapshot)),
      chunk_bytes_(chunk_bytes == 0 ? kSnapshotChunkBytes : chunk_bytes),
      at_slot_(snapshot_->at_slot())
{
    uint32_t crc = 0;
    size_t   len = 0;
    for_each_portable_piece(*snapshot_, [&](const uint8_t *data, size_t size) {
        if (size > std::numeric_limits<size_t>::max() - len) {
            len = std::numeric_limits<size_t>::max();
            return;
        }
        len += size;
        crc = crowdb::common::crc32c_update(crc, data, size);
    });
    final_crc32c_ = crc;
    total_bytes_  = len + kSnapTrailer;
}

Status SnapshotExport::next_portable_chunk(std::string *out, bool *done)
{
    out->clear();
    out->reserve(chunk_bytes_);
    const size_t body_piece_count = 1 + (snapshot_->entries().size() * 6);
    const size_t trailer_piece    = body_piece_count;

    auto append_piece = [&](const uint8_t *data, size_t size) {
        const size_t available = chunk_bytes_ - out->size();
        const size_t remaining = size - piece_offset_;
        const size_t take      = std::min(available, remaining);
        out->append(reinterpret_cast<const char *>(data + piece_offset_), take);
        piece_offset_ += take;
        pos_ += take;
        if (piece_offset_ == size) {
            piece_offset_ = 0;
            ++piece_index_;
        }
    };

    while (out->size() < chunk_bytes_ && piece_index_ <= trailer_piece) {
        std::array<uint8_t, kSnapHeader> header{};
        std::array<uint8_t, 4>           u32{};
        std::array<uint8_t, 8>           u64{};
        const uint8_t                   *data = nullptr;
        size_t                           size = 0;

        if (piece_index_ == 0) {
            encode_u32(kSnapMagic, &u32);
            std::copy(u32.begin(), u32.end(), header.begin());
            encode_u32(kSnapVersion, &u32);
            std::copy(u32.begin(), u32.end(), header.begin() + 4);
            header[8] = static_cast<uint8_t>(snapshot_format::kPortable);
            encode_u64(at_slot_, &u64);
            std::copy(u64.begin(), u64.end(), header.begin() + 9);
            encode_u64(static_cast<uint64_t>(snapshot_->entries().size()), &u64);
            std::copy(u64.begin(), u64.end(), header.begin() + 17);
            data = header.data();
            size = header.size();
        }
        else if (piece_index_ == trailer_piece) {
            encode_u32(final_crc32c_, &u32);
            data = u32.data();
            size = u32.size();
        }
        else {
            const size_t      encoded = piece_index_ - 1;
            const leaf_entry &entry   = snapshot_->entries()[encoded / 6];
            const size_t      part    = encoded % 6;
            CellView          value{Slice(entry.cell)};
            switch (part) {
            case 0:
                encode_u32(static_cast<uint32_t>(entry.key.size()), &u32);
                data = u32.data();
                size = u32.size();
                break;
            case 1:
                data = reinterpret_cast<const uint8_t *>(entry.key.data());
                size = entry.key.size();
                break;
            case 2:
                encode_u64(value.slot(), &u64);
                data = u64.data();
                size = u64.size();
                break;
            case 3: {
                static constexpr uint8_t kPut    = 0;
                static constexpr uint8_t kDelete = 1;
                data                             = value.is_tombstone() ? &kDelete : &kPut;
                size                             = 1;
                break;
            }
            case 4: {
                Slice bytes = value.value();
                encode_u32(static_cast<uint32_t>(bytes.size()), &u32);
                data = u32.data();
                size = u32.size();
                break;
            }
            case 5: {
                Slice bytes = value.value();
                data        = reinterpret_cast<const uint8_t *>(bytes.data());
                size        = bytes.size();
                break;
            }
            default:
                return Status::corruption("snapshot export: invalid encoder state");
            }
        }
        if (size == 0) {
            ++piece_index_;
            piece_offset_ = 0;
            continue;
        }
        append_piece(data, size);
    }
    if (done != nullptr) {
        *done = piece_index_ > trailer_piece;
    }
    return Status::Ok();
}

} // namespace crowdb::tree
