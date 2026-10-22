// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "rdma_control_stream.h"

namespace crowdb::rpc
{
bool write_control_fragment(std::span<uint8_t> prefix, const ControlFragment &fragment, uint32_t data_length);
bool parse_control_fragment(std::span<const uint8_t> control, ControlFragment &fragment);
bool control_identity_matches(const Frame &frame);
} // namespace crowdb::rpc
