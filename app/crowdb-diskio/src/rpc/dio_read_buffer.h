// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "disk/disk.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <limits>

namespace crowdb::diskio
{
// One disk-read destination, including alignment-edge staging when required.
// Its asynchronous callback must finish ownership before publishing/releasing.
struct DiskReadBuffer
{
    crowdb::rpc::Buffer         *buffer;
    uint32_t                     size;
    off_t                        offset;
    size_t                       io_size;
    size_t                       prefix = 0;
    std::shared_ptr<uint8_t>     aligned;
    crowdb::rpc::BufferOwnership previous_owner = crowdb::rpc::BufferOwnership::Available;

    IoRetCode prepare_alignment(const Disk &disk, const Zone &zone)
    {
        if (!disk.is_o_direct())
            return IoRetCode::Success;
        const size_t alignment = std::max<size_t>(4096, disk.block_size());
        if ((alignment & (alignment - 1)) != 0 || alignment > static_cast<size_t>(std::numeric_limits<off_t>::max()) ||
            static_cast<uint64_t>(offset) + size > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - alignment)
            return IoRetCode::InvalidAlignment;
        prefix = static_cast<size_t>(offset) % alignment;
        offset -= static_cast<off_t>(prefix);
        io_size = ((prefix + size + alignment - 1) / alignment) * alignment;
        if (offset < zone.base_offset || io_size > static_cast<size_t>(zone.capacity - (offset - zone.base_offset)))
            return IoRetCode::InvalidAlignment;
        if (prefix != 0 || io_size != size || reinterpret_cast<uintptr_t>(buffer->payload_data()) % alignment != 0) {
            aligned =
                std::shared_ptr<uint8_t>(static_cast<uint8_t *>(std::aligned_alloc(alignment, io_size)), std::free);
            if (aligned == nullptr)
                return IoRetCode::IoError;
        }
        return IoRetCode::Success;
    }

    bool begin()
    {
        using crowdb::rpc::BufferOwnership;
        previous_owner = buffer->ownership.load(std::memory_order_acquire);
        if ((previous_owner == BufferOwnership::Available || previous_owner == BufferOwnership::CallerOwned) &&
            buffer->transition(previous_owner, BufferOwnership::DiskOwned))
            return true;
        (void)buffer->quarantine();
        buffer->release();
        return false;
    }

    uint8_t *data() const
    {
        return aligned ? aligned.get() : buffer->payload_data();
    }

    struct Result
    {
        IoRetCode            code;
        crowdb::rpc::Buffer *data;
        bool                 valid_owner;
    };

    Result finish(int result)
    {
        if (!buffer->transition(crowdb::rpc::BufferOwnership::DiskOwned, previous_owner)) {
            (void)buffer->quarantine();
            buffer->release();
            return {IoRetCode::IoError, nullptr, false};
        }
        if (result < 0 || static_cast<size_t>(result) < io_size) {
            buffer->release();
            return {result < 0 ? IoRetCode::IoError : IoRetCode::PartialWrite, nullptr, true};
        }
        if (aligned)
            std::memcpy(buffer->payload_data(), aligned.get() + prefix, size);
        buffer->len = size;
        return {IoRetCode::Success, buffer, true};
    }
};
} // namespace crowdb::diskio
