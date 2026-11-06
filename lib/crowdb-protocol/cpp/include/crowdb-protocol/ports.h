// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Generated from ports.rs by tools/ports/generate.py; do not edit.
#pragma once

#include <cstdint>

namespace crowdb::protocol
{
inline constexpr std::uint16_t PAXOS_KV_SERVER_MGMT_BASE       = 10135;
inline constexpr std::uint16_t PAXOS_KV_SERVER_RPC_BASE        = 11135;
inline constexpr std::uint16_t DISKDB_HTTP_BASE                = 13135;
inline constexpr std::uint16_t DISKDB_RPC_BASE                 = 14135;
inline constexpr std::uint16_t CHUNKDB_HTTP_BASE               = 16135;
inline constexpr std::uint16_t CHUNKDB_RPC_BASE                = 17135;
inline constexpr std::uint16_t DISKIO_RPC_BASE                 = 18135;
inline constexpr std::uint16_t WEB_BASE                        = 6135;
inline constexpr std::uint16_t CHUNK_KV_HTTP_BASE              = 20135;
inline constexpr std::uint16_t CHUNK_KV_RPC_BASE               = 21135;
inline constexpr std::uint16_t ACCESS_SERVER_HTTP_BASE         = 7135;
inline constexpr std::uint16_t ACCESS_SERVER_ICEBERG_HTTP_BASE = 8135;
inline constexpr std::uint16_t ACCESS_SERVER_DATASET_HTTP_BASE = 9135;
inline constexpr std::uint16_t ACCESS_SERVER_HEALTH_HTTP_BASE  = 25135;
inline constexpr std::uint16_t MONITOR_HTTP_BASE               = 26135;
inline constexpr std::uint16_t SSH_PORT                        = 2222;
inline constexpr std::uint16_t HOST_SSH_PORT                   = 22;
} // namespace crowdb::protocol
