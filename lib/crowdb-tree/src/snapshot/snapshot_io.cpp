// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// B+tree snapshot stream implementation.

#include "crowdb-tree/snapshot/snapshot_io.h"

#include "crowdb-common/crc32c.h"
#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/maptable/page_types.h"

#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <vector>

namespace crowdb::tree
{

namespace
{

constexpr uint32_t kSnapMagic   = 0x4E535443; // 'CTSN' little-endian
constexpr uint32_t kSnapVersion = 1;
// Portable header: magic + version + format + at_slot + entry_count.
constexpr size_t kSnapHeader  = 4 + 4 + 1 + 8 + 8;
constexpr size_t kSnapTrailer = 4; // whole-stream CRC32C

// Native header: magic + version + format + at_slot + root_page_id + frame_count.
constexpr size_t kNativeHeader = 4 + 4 + 1 + 8 + 8 + 8;

void put_u32(std::string *o, uint32_t v)
{
    for (int i = 0; i < 4; ++i) {
        o->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
}

void put_u64(std::string *o, uint64_t v)
{
    for (int i = 0; i < 8; ++i) {
        o->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
}

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

Status snapshot_export_begin_portable(Crowdbtree &tree, size_t chunk_bytes, std::unique_ptr<SnapshotExport> *out)
{
    // v1 always exports the current durable view (its last_applied_slot is recorded
    // in the stream header). An arbitrary historical pin is deferred until
    // path-copy COW RootVersions exist.
    std::shared_ptr<Snapshot> snap = tree.snapshot_view();
    auto                      exp  = std::make_unique<SnapshotExport>(std::move(snap), chunk_bytes);
    *out                           = std::move(exp);
    return Status::Ok();
}

// plan-tree #16: native format -- raw leaf/inner/overflow frame bytes
// tagged with their PID, no cell decode/tuple encode. Body:
// `[u64 page_id][u32 frame_len][frame_len bytes] * frame_count`.
Status snapshot_export_begin_native(Crowdbtree &tree, size_t chunk_bytes, std::unique_ptr<SnapshotExport> *out)
{
    std::vector<NativeFrame> frames;
    uint64_t                 root_page_id = 0;
    uint64_t                 slot         = 0;
    Status                   cs           = tree.collect_native_frames(&frames, &root_page_id, &slot);
    if (!cs.ok()) {
        return cs;
    }

    std::string s;
    put_u32(&s, kSnapMagic);
    put_u32(&s, kSnapVersion);
    s.push_back(static_cast<char>(snapshot_format::kNative));
    put_u64(&s, slot);
    put_u64(&s, root_page_id);
    put_u64(&s, static_cast<uint64_t>(frames.size()));
    for (const NativeFrame &f : frames) {
        put_u64(&s, f.page_id);
        put_u32(&s, static_cast<uint32_t>(f.frame.size()));
        s.append(reinterpret_cast<const char *>(f.frame.data()), f.frame.size());
    }
    uint32_t crc = crowdb::common::crc32c(reinterpret_cast<const uint8_t *>(s.data()), s.size());
    put_u32(&s, crc);

    auto exp = std::make_unique<SnapshotExport>(std::move(s), chunk_bytes, slot);
    *out     = std::move(exp);
    return Status::Ok();
}

} // namespace

Status snapshot_export_begin(Crowdbtree &tree, snapshot_format fmt, size_t chunk_bytes,
                             std::unique_ptr<SnapshotExport> *out)
{
    switch (fmt) {
    case snapshot_format::kPortable:
        return snapshot_export_begin_portable(tree, chunk_bytes, out);
    case snapshot_format::kNative:
        return snapshot_export_begin_native(tree, chunk_bytes, out);
    default:
        return Status::not_supported("snapshot export: unknown format");
    }
}

Status SnapshotExport::next_chunk(std::string *out, bool *done)
{
    if (snapshot_ != nullptr) {
        return next_portable_chunk(out, done);
    }
    out->clear();
    size_t remaining = stream_.size() - pos_;
    size_t n         = remaining < chunk_bytes_ ? remaining : chunk_bytes_;
    out->assign(stream_, pos_, n);
    pos_ += n;
    if (done != nullptr) {
        *done = (pos_ >= stream_.size());
    }
    return Status::Ok();
}

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

Status snapshot_dump_to_file(Crowdbtree &tree, snapshot_format fmt, const std::string &path)
{
    std::unique_ptr<SnapshotExport> exp;
    Status                          s = snapshot_export_begin(tree, fmt, kSnapshotChunkBytes, &exp);
    if (!s.ok()) {
        return s;
    }
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) {
        return Status::io_error("snapshot dump: cannot open " + path);
    }
    bool done = false;
    while (!done) {
        std::string chunk;
        Status      cs = exp->next_chunk(&chunk, &done);
        if (!cs.ok()) {
            return cs;
        }
        if (!chunk.empty()) {
            f.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        }
        if (!f) {
            return Status::io_error("snapshot dump: write failed");
        }
    }
    f.flush();
    if (!f) {
        return Status::io_error("snapshot dump: flush failed");
    }
    return Status::Ok();
}

struct SnapshotImport::PortableState
{
    enum class Part : uint8_t { kKeyLen, kKey, kSlot, kKind, kValueLen, kValue, kTrailer, kComplete };

    uint64_t                at_slot  = 0;
    uint64_t                expected = 0;
    uint64_t                parsed   = 0;
    uint32_t                crc      = 0;
    Part                    part     = Part::kKeyLen;
    std::array<uint8_t, 8>  fixed{};
    size_t                  fixed_used = 0;
    uint32_t                key_len    = 0;
    uint64_t                slot       = 0;
    uint8_t                 kind       = 0;
    uint32_t                value_len  = 0;
    std::string             key;
    std::string             value;
    std::vector<leaf_entry> entries;

    Status begin(const uint8_t *header)
    {
        if (get_u32(header) != kSnapMagic) {
            return Status::corruption("snapshot: bad magic");
        }
        if (get_u32(header + 4) != kSnapVersion) {
            return Status::not_supported("snapshot: version");
        }
        if (static_cast<snapshot_format>(header[8]) != snapshot_format::kPortable) {
            return Status::not_supported("snapshot: unknown format");
        }
        at_slot  = get_u64(header + 9);
        expected = get_u64(header + 17);
        crc      = crowdb::common::crc32c_update(0, header, kSnapHeader);
        part     = expected == 0 ? Part::kTrailer : Part::kKeyLen;
        return Status::Ok();
    }

    void reset_fixed()
    {
        fixed_used = 0;
    }

    void consume_fixed(const uint8_t **data, size_t *len, size_t need, bool include_crc)
    {
        size_t take = std::min(*len, need - fixed_used);
        std::copy_n(*data, take, fixed.data() + fixed_used);
        if (include_crc) {
            crc = crowdb::common::crc32c_update(crc, *data, take);
        }
        fixed_used += take;
        *data += take;
        *len -= take;
    }

    void consume_string(const uint8_t **data, size_t *len, std::string *out, size_t need)
    {
        size_t take = std::min(*len, need - out->size());
        out->append(reinterpret_cast<const char *>(*data), take);
        crc = crowdb::common::crc32c_update(crc, *data, take);
        *data += take;
        *len -= take;
    }

    Status feed(const uint8_t *data, size_t len)
    {
        while (len > 0) {
            switch (part) {
            case Part::kKeyLen:
                consume_fixed(&data, &len, 4, true);
                if (fixed_used == 4) {
                    key_len = get_u32(fixed.data());
                    key.clear();
                    reset_fixed();
                    part = key_len == 0 ? Part::kSlot : Part::kKey;
                }
                break;
            case Part::kKey:
                consume_string(&data, &len, &key, key_len);
                if (key.size() == key_len) {
                    part = Part::kSlot;
                }
                break;
            case Part::kSlot:
                consume_fixed(&data, &len, 8, true);
                if (fixed_used == 8) {
                    slot = get_u64(fixed.data());
                    reset_fixed();
                    part = Part::kKind;
                }
                break;
            case Part::kKind:
                consume_fixed(&data, &len, 1, true);
                if (fixed_used == 1) {
                    kind = fixed[0];
                    reset_fixed();
                    if (kind > 1) {
                        return Status::corruption("snapshot: invalid cell kind");
                    }
                    part = Part::kValueLen;
                }
                break;
            case Part::kValueLen:
                consume_fixed(&data, &len, 4, true);
                if (fixed_used == 4) {
                    value_len = get_u32(fixed.data());
                    value.clear();
                    reset_fixed();
                    part = Part::kValue;
                }
                break;
            case Part::kValue:
                consume_string(&data, &len, &value, value_len);
                if (value.size() == value_len) {
                    buffer cell = encode_cell_buf(slot, kind != 0 ? OpKind::kDelete : OpKind::kPut, Slice(value));
                    entries.push_back({.key = std::move(key), .cell = std::move(cell)});
                    value.clear();
                    ++parsed;
                    if (parsed > expected) {
                        return Status::corruption("snapshot: entry count overflow");
                    }
                    part = parsed == expected ? Part::kTrailer : Part::kKeyLen;
                }
                break;
            case Part::kTrailer:
                consume_fixed(&data, &len, kSnapTrailer, false);
                if (fixed_used == kSnapTrailer) {
                    if (get_u32(fixed.data()) != crc) {
                        return Status::corruption("snapshot: CRC mismatch");
                    }
                    reset_fixed();
                    part = Part::kComplete;
                }
                break;
            case Part::kComplete:
                return Status::corruption("snapshot: trailing bytes");
            }
        }
        return Status::Ok();
    }

    Status finish(Crowdbtree &tree, uint64_t *out_at_slot)
    {
        if (part != Part::kComplete || parsed != expected) {
            return Status::invalid_argument("snapshot: stream truncated");
        }
        Status installed = tree.install_snapshot(std::move(entries), at_slot);
        if (!installed.ok()) {
            return installed;
        }
        if (out_at_slot != nullptr) {
            *out_at_slot = at_slot;
        }
        return Status::Ok();
    }
};

SnapshotImport::SnapshotImport(Crowdbtree &tree) : tree_(tree)
{
}

SnapshotImport::~SnapshotImport() = default;

Status SnapshotImport::feed(Slice chunk)
{
    const auto *data = reinterpret_cast<const uint8_t *>(chunk.data());
    size_t      len  = chunk.size();
    if (!format_selected_) {
        size_t take = std::min(len, kSnapHeader - buf_.size());
        buf_.append(reinterpret_cast<const char *>(data), take);
        data += take;
        len -= take;
        if (buf_.size() < kSnapHeader) {
            return Status::Ok();
        }
        const auto *header = reinterpret_cast<const uint8_t *>(buf_.data());
        if (get_u32(header) != kSnapMagic) {
            return Status::corruption("snapshot: bad magic");
        }
        if (get_u32(header + 4) != kSnapVersion) {
            return Status::not_supported("snapshot: version");
        }
        auto fmt         = static_cast<snapshot_format>(header[8]);
        format_selected_ = true;
        if (fmt == snapshot_format::kNative) {
            native_ = true;
        }
        else if (fmt == snapshot_format::kPortable) {
            portable_     = std::make_unique<PortableState>();
            Status status = portable_->begin(header);
            if (!status.ok()) {
                return status;
            }
            buf_.clear();
        }
        else {
            return Status::not_supported("snapshot: unknown format");
        }
    }
    if (native_) {
        buf_.append(reinterpret_cast<const char *>(data), len);
        return Status::Ok();
    }
    return portable_->feed(data, len);
}

Status SnapshotImport::finish_native(const uint8_t *p, size_t len, uint64_t *out_at_slot)
{
    if (len < kNativeHeader + kSnapTrailer) {
        return Status::invalid_argument("snapshot: native stream too short");
    }
    // Verify the whole-stream CRC over everything but the trailing 4 bytes.
    uint32_t want_crc = get_u32(p + (len - kSnapTrailer));
    if (crowdb::common::crc32c(p, len - kSnapTrailer) != want_crc) {
        return Status::corruption("snapshot: CRC mismatch");
    }

    uint64_t at_slot      = get_u64(p + 9);
    uint64_t root_page_id = get_u64(p + 17);
    uint64_t count        = get_u64(p + 25);

    std::vector<NativeFrame> frames;
    frames.reserve(count);
    size_t       pos      = kNativeHeader;
    const size_t body_end = len - kSnapTrailer;
    for (uint64_t i = 0; i < count; ++i) {
        if (pos + 8 + 4 > body_end) {
            return Status::corruption("snapshot: truncated native frame header");
        }
        uint64_t page_id = get_u64(p + pos);
        pos += 8;
        uint32_t flen = get_u32(p + pos);
        pos += 4;
        if (pos + flen > body_end) {
            return Status::corruption("snapshot: truncated native frame body");
        }
        frames.push_back(NativeFrame{.page_id = page_id, .frame = std::vector<uint8_t>(p + pos, p + pos + flen)});
        pos += flen;
    }
    if (pos != body_end) {
        return Status::corruption("snapshot: trailing bytes");
    }

    Status is = tree_.install_snapshot_native(std::move(frames), root_page_id, at_slot);
    if (!is.ok()) {
        return is;
    }
    if (out_at_slot != nullptr) {
        *out_at_slot = at_slot;
    }
    return Status::Ok();
}

Status SnapshotImport::finish(uint64_t *out_at_slot)
{
    if (portable_ != nullptr) {
        return portable_->finish(tree_, out_at_slot);
    }
    const size_t len = buf_.size();
    if (len < kSnapHeader + kSnapTrailer) {
        return Status::invalid_argument("snapshot: stream too short");
    }
    const auto *p = reinterpret_cast<const uint8_t *>(buf_.data());

    if (get_u32(p) != kSnapMagic) {
        return Status::corruption("snapshot: bad magic");
    }
    if (get_u32(p + 4) != kSnapVersion) {
        return Status::not_supported("snapshot: version");
    }
    auto fmt = static_cast<snapshot_format>(p[8]);
    if (fmt == snapshot_format::kNative) {
        return finish_native(p, len, out_at_slot);
    }
    return Status::not_supported("snapshot: unknown format");
}

Status snapshot_load_from_file(Crowdbtree &tree, const std::string &path)
{
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        return Status::io_error("snapshot load: cannot open " + path);
    }
    SnapshotImport             imp(tree);
    std::array<char, 1U << 16> buf{};
    while (f) {
        f.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = f.gcount();
        if (got > 0) {
            Status s = imp.feed(Slice(buf.data(), static_cast<size_t>(got)));
            if (!s.ok()) {
                return s;
            }
        }
    }
    if (f.bad()) {
        return Status::io_error("snapshot load: read failed");
    }
    return imp.finish(nullptr);
}

} // namespace crowdb::tree
