// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "rdma_aggregate.h"

namespace crowdb::rpc
{
constexpr uint16_t RDMA_PACKED_MSG_TYPE   = 0xFFFD;
constexpr uint32_t RDMA_SMALL_FRAME_BYTES = 4096;

bool rdma_packable_frame(const OutFrame &frame, uint32_t limit, uint32_t &size);
void write_rdma_packed_control(uint8_t *destination, uint32_t data_length, uint32_t count);
bool parse_rdma_packed_frame(std::span<const uint8_t> control, std::span<const uint8_t> data, uint32_t frame_limit,
                             RdmaAggregate &aggregate);
} // namespace crowdb::rpc
