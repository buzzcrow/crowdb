// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_byte_export_internal.h"
#include "snapshot/snapshot_crc.h"
#include "snapshot/snapshot_wire.h"

namespace crowdb::tree
{
Status PublishedByteExport::State::portable_preflight(const KeyRange &range)
{
    auto status = PublishedSnapshotEntries::create(snapshot, &entries, range);
    if (!status.ok()) {
        return status;
    }
    uint32_t body_crc   = 0;
    size_t   body_bytes = 0;
    bool     complete   = false;
    while (!complete) {
        std::optional<PublishedScanEntry> item;
        status = entries->next(&item, &complete);
        if (!status.ok()) {
            return status;
        }
        if (!item) {
            continue;
        }
        status = snapshot_detail::entry_fields(*item, &fields);
        if (!status.ok()) {
            return status;
        }
        const auto key   = snapshot_detail::bytes(Slice(item->key));
        const auto value = snapshot_detail::bytes(item->cell.value);
        const auto bytes = uint64_t{17} + key.size() + value.size();
        if (count == UINT64_MAX || bytes > SIZE_MAX - body_bytes - 29) {
            return Status::resource_exhausted("portable stream length overflow");
        }
        ++count;
        body_bytes += bytes;
        body_crc = snapshot_detail::crc_bytes(body_crc, std::span(fields).first(4));
        body_crc = snapshot_detail::crc_bytes(body_crc, key);
        body_crc = snapshot_detail::crc_bytes(body_crc, std::span(fields).subspan(4));
        body_crc = snapshot_detail::crc_bytes(body_crc, value);
    }
    encode_header();
    const auto header_crc = snapshot_detail::crc_bytes(0, std::span(header).first(header_size));
    info.final_crc        = snapshot_detail::crc_concatenate(header_crc, body_crc, body_bytes);
    info.total_bytes      = header_size + body_bytes + 4;
    entries.reset();
    snapshot->lane()->drain();
    return PublishedSnapshotEntries::create(snapshot, &entries, range);
}

Status PublishedByteExport::State::native_preflight()
{
    encode_header();
    uint32_t crc      = snapshot_detail::crc_bytes(0, std::span(header).first(header_size));
    size_t   length   = header_size;
    bool     complete = false;
    while (!complete) {
        std::optional<NativeFrameView> item;
        auto                           status = native->next(&item, &complete);
        if (!status.ok()) {
            return status;
        }
        if (!item) {
            continue;
        }
        if (item->bytes.size() > UINT32_MAX || length > SIZE_MAX - 16 || item->bytes.size() > SIZE_MAX - length - 16) {
            return Status::resource_exhausted("native stream length overflow");
        }
        snapshot_detail::put_u64(fields.data(), item->page_id);
        snapshot_detail::put_u32(fields.data() + 8, static_cast<uint32_t>(item->bytes.size()));
        crc = snapshot_detail::crc_bytes(crc, std::span(fields).first(12));
        crc = snapshot_detail::crc_bytes(crc, item->bytes);
        length += 12 + item->bytes.size();
    }
    info.total_bytes = length + 4;
    info.final_crc   = crc;
    return native->rewind();
}

Status PublishedByteExport::State::preflight(const KeyRange &range)
{
    auto status = format == snapshot_format::kPortable ? portable_preflight(range) : native_preflight();
    if (status.ok()) {
        snapshot_detail::put_u32(trailer.data(), info.final_crc);
    }
    return status;
}
} // namespace crowdb::tree
