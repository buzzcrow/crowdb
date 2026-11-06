// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "host_completion.h"

namespace crowdb::tree
{
namespace
{
struct PackedReader
{
    const uint8_t *position;
    size_t         left;

    bool integer(size_t bytes, uint64_t *value)
    {
        if (left < bytes) {
            return false;
        }
        *value = 0;
        for (size_t i = 0; i < bytes; ++i) {
            *value |= uint64_t{position[i]} << (8 * i);
        }
        position += bytes;
        left -= bytes;
        return true;
    }

    bool string(Slice *value)
    {
        uint64_t bytes = 0;
        if (!integer(4, &bytes) || left < bytes) {
            return false;
        }
        *value = Slice(position, bytes);
        position += bytes;
        left -= bytes;
        return true;
    }
};

Status unpack(const ScanPackedBuf &packed, std::vector<scan_entry> *out, size_t *count)
{
    PackedReader reader{.position = packed.data(), .left = packed.size()};
    size_t       entries = 0;
    while (reader.left != 0) {
        Slice    key;
        Slice    value;
        uint64_t slot      = 0;
        uint64_t tombstone = 0;
        if (!reader.string(&key) || !reader.integer(8, &slot) || !reader.integer(1, &tombstone) ||
            !reader.string(&value)) {
            return Status::corruption("invalid internal scan record");
        }
        if (out != nullptr) {
            // The copying convenience API returns application-owned strings.
            out->push_back(
                {.key = key.to_string(), .slot = slot, .value = value.to_string(), .tombstone = tombstone != 0});
        }
        ++entries;
    }
    if (count != nullptr) {
        *count = entries;
    }
    return Status::Ok();
}
} // namespace

Status TreeCore::scan(CoreScanOptions options, std::vector<scan_entry> *out, bool *truncated, ScanPackedBuf *packed,
                      size_t *count)
{
    if (truncated == nullptr || (out == nullptr && packed == nullptr)) {
        return Status::invalid_argument("scan needs output and truncation flag");
    }
    if (out != nullptr) {
        out->clear();
    }
    if (count != nullptr) {
        *count = 0;
    }
    *truncated = false;
    detail::HostCompletion done;
    Status                 status;
    ScanPackedBuf          result;
    scan_impl(
        options,
        [&](Status outcome, ScanPackedBuf value, bool more) {
            status     = std::move(outcome);
            result     = std::move(value);
            *truncated = more;
            done.signal();
        },
        false);
    done.wait();
    if (!status.ok()) {
        return status;
    }
    try {
        status = unpack(result, packed == nullptr ? out : nullptr, count);
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted("scan copy allocation failed");
    }
    if (status.ok() && packed != nullptr) {
        *packed = std::move(result);
    }
    return status;
}
} // namespace crowdb::tree
