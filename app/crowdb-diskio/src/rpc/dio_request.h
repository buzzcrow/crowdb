// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/framing.h"
#include "disk/disk.h"

#include <diskio_generated.h>
#include <flatbuffers/flatbuffers.h>

#include <chrono>
#include <limits>

namespace crowdb::diskio
{
template <typename Message> const Message *decode_disk_request(const crowdb::rpc::Frame &request)
{
    if (request.control.size() < sizeof(flatbuffers::uoffset_t))
        return nullptr;
    flatbuffers::Verifier verifier(request.control.data(), request.control.size());
    return verifier.VerifyBuffer<Message>(nullptr) ? flatbuffers::GetRoot<Message>(request.control.data()) : nullptr;
}

inline DiskId parse_disk_id(const crowdb::rpc::proto::FBInt128 *id)
{
    return id == nullptr ? DiskId{0, 0} : DiskId{id->high(), id->low()};
}

inline bool valid_disk_range(const Zone &zone, uint64_t offset, uint32_t size)
{
    if (zone.base_offset < 0 || zone.capacity < 0 || offset > static_cast<uint64_t>(zone.capacity) ||
        size > static_cast<uint64_t>(zone.capacity) - offset)
        return false;
    const auto remaining = static_cast<uint64_t>(std::numeric_limits<off_t>::max() - zone.base_offset);
    return offset <= remaining && size <= remaining - offset;
}

inline uint64_t elapsed_nanos(std::chrono::steady_clock::time_point started)
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count());
}

inline uint64_t unix_time_ms()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}
} // namespace crowdb::diskio
