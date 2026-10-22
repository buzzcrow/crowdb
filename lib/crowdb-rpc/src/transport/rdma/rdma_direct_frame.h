// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport.h"

#include <span>

namespace crowdb::rpc
{
bool rdma_direct_frame_size(const OutFrame &frame, uint32_t wire_limit, uint32_t &size);
void write_rdma_direct_control(uint8_t *destination, const OutFrame &frame);
bool parse_rdma_direct_control(std::span<const uint8_t> control, uint32_t data_capacity, uint32_t wire_length,
                               Header &header);
} // namespace crowdb::rpc
