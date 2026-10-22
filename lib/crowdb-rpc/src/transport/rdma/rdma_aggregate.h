// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport.h"

#include <span>

namespace crowdb::rpc
{
constexpr uint32_t RDMA_MAX_AGGREGATE_FRAMES = 256;

struct RdmaFrameView
{
    Header   header;
    uint32_t offset;
    uint32_t data_offset;
};

struct RdmaAggregate
{
    std::array<RdmaFrameView, RDMA_MAX_AGGREGATE_FRAMES> frames;
    uint32_t                                             count = 0;
};

// The complete aggregate must validate before any application callback.
bool parse_rdma_aggregate(std::span<const uint8_t> bytes, RdmaAggregate &result);
bool rdma_frame_size(const OutFrame &frame, uint32_t limit, uint32_t &size);
void copy_rdma_frame(uint8_t *destination, const OutFrame &frame);
void release_rdma_frames(OutFrame *frame, bool quarantine = false);
} // namespace crowdb::rpc
