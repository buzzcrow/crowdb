// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/framing.h"

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <span>

namespace crowdb::rpc
{
constexpr uint16_t RPC_CONTROL_FRAGMENT_MSG_TYPE = 4;
constexpr uint8_t  CONTROL_STREAM_START          = 1;
constexpr uint8_t  CONTROL_STREAM_END            = 2;
constexpr uint8_t  CONTROL_STREAM_ERROR          = 4;

struct ControlStreamBudgetState;

class ControlStreamLease
{
  public:
    ~ControlStreamLease();
    ControlStreamLease(const ControlStreamLease &)            = delete;
    ControlStreamLease &operator=(const ControlStreamLease &) = delete;

  private:
    friend class ControlStreamBudget;
    ControlStreamLease(std::shared_ptr<ControlStreamBudgetState> state, uint64_t bytes);
    std::shared_ptr<ControlStreamBudgetState> state_;
    uint64_t                                  bytes_;
};

// Copies share the same admission. The wake callback must not throw and must
// safely handle a lease released after its transport has stopped.
class ControlStreamBudget
{
  public:
    explicit ControlStreamBudget(uint64_t limit, std::function<void()> wake = {});
    [[nodiscard]] std::shared_ptr<ControlStreamLease> try_reserve(uint64_t bytes);
    [[nodiscard]] uint64_t                            used() const;
    [[nodiscard]] uint64_t                            high_water() const;
    [[nodiscard]] uint64_t                            stalls() const;
    [[nodiscard]] uint64_t                            limit() const;

  private:
    std::shared_ptr<ControlStreamBudgetState> state_;
};

struct ControlFragment
{
    uint64_t request_id           = 0;
    uint64_t rpc_create_nano      = 0;
    uint64_t response_create_nano = 0;
    uint64_t stream_id            = 0;
    uint64_t generation           = 0;
    uint64_t sequence             = 0;
    uint64_t offset               = 0;
    uint64_t total                = 0;
    uint32_t capacity             = 0;
    uint16_t message_type         = 0;
    uint8_t  message_flags        = 0;
    uint8_t  stream_flags         = 0;
};

enum class ControlStreamResult { Accepted, Complete, Backpressure, Invalid, Aborted };

// One QP sends complete logical messages in order, so its CQ owner needs only
// one active assembly. Completed controls retain admission independently.
class ControlStreamReceiver
{
  public:
    using Clock = std::chrono::steady_clock;
    ControlStreamReceiver(ControlStreamBudget budget, uint32_t capacity, uint64_t generation,
                          uint32_t max_message_bytes, std::chrono::milliseconds timeout);
    ControlStreamResult accept(const ControlFragment &fragment, std::span<const uint8_t> bytes, Clock::time_point now,
                               std::unique_ptr<Frame> &completed);
    [[nodiscard]] bool  active() const;
    [[nodiscard]] bool  expired(Clock::time_point now) const;
    void                reset();

  private:
    struct Assembly
    {
        ControlFragment                     first;
        uint64_t                            next_sequence = 0;
        uint64_t                            offset        = 0;
        Clock::time_point                   deadline;
        std::shared_ptr<ControlStreamLease> lease;
        std::vector<uint8_t>                bytes;
    };

    [[nodiscard]] bool        valid(const ControlFragment &fragment, size_t bytes) const;
    [[nodiscard]] bool        matches(const ControlFragment &fragment) const;
    ControlStreamBudget       budget_;
    uint32_t                  capacity_;
    uint64_t                  generation_;
    uint32_t                  max_message_bytes_;
    std::chrono::milliseconds timeout_;
    uint64_t                  last_stream_id_ = 0;
    std::optional<Assembly>   assembly_;
};
} // namespace crowdb::rpc
