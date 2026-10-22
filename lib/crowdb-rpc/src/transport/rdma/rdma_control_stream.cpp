// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_control_stream.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace crowdb::rpc
{
ControlStreamReceiver::ControlStreamReceiver(ControlStreamBudget budget, uint32_t capacity, uint64_t generation,
                                             uint32_t max_message_bytes, std::chrono::milliseconds timeout)
    : budget_(std::move(budget)),
      capacity_(capacity),
      generation_(generation),
      max_message_bytes_(max_message_bytes),
      timeout_(timeout)
{
    BufferCapacityClass type;
    uint32_t            rounded = 0;
    if (!classify_rdma_capacity(capacity, &type, &rounded) || rounded != capacity || generation == 0 ||
        max_message_bytes == 0 || max_message_bytes > budget_.limit() || timeout.count() <= 0)
        throw std::invalid_argument("invalid logical control stream bounds");
}

bool ControlStreamReceiver::valid(const ControlFragment &f, size_t bytes) const
{
    if (f.request_id == 0 || f.stream_id == 0 || f.generation != generation_ || f.capacity != capacity_ ||
        f.total == 0 || f.total > max_message_bytes_ || f.offset > f.total || bytes > f.total - f.offset ||
        bytes > capacity_ || f.message_type == RPC_CONTROL_FRAGMENT_MSG_TYPE ||
        (f.message_flags & ~FLAG_ONE_WAY) != 0 || (f.stream_flags & ~uint8_t{7}) != 0)
        return false;
    const bool start = (f.stream_flags & CONTROL_STREAM_START) != 0;
    const bool end   = (f.stream_flags & CONTROL_STREAM_END) != 0;
    if (start != (f.sequence == 0 && f.offset == 0))
        return false;
    if ((f.stream_flags & CONTROL_STREAM_ERROR) != 0)
        return end && !start && bytes == 0;
    return bytes != 0 && end == (bytes == f.total - f.offset) && (end || bytes == capacity_);
}

bool ControlStreamReceiver::matches(const ControlFragment &f) const
{
    const auto &a     = *assembly_;
    const auto &first = a.first;
    return f.request_id == first.request_id && f.rpc_create_nano == first.rpc_create_nano &&
           f.response_create_nano == first.response_create_nano && f.stream_id == first.stream_id &&
           f.total == first.total && f.message_type == first.message_type && f.message_flags == first.message_flags &&
           f.sequence == a.next_sequence && f.offset == a.offset;
}

ControlStreamResult ControlStreamReceiver::accept(const ControlFragment &f, std::span<const uint8_t> bytes,
                                                  Clock::time_point now, std::unique_ptr<Frame> &completed)
{
    completed.reset();
    if (!valid(f, bytes.size()))
        return ControlStreamResult::Invalid;
    if (!assembly_) {
        if ((f.stream_flags & CONTROL_STREAM_START) == 0 || f.stream_id <= last_stream_id_)
            return ControlStreamResult::Invalid;
        auto lease = budget_.try_reserve(f.total);
        if (lease == nullptr)
            return ControlStreamResult::Backpressure;
        Assembly next{.first = f, .deadline = now + timeout_, .lease = std::move(lease), .bytes = {}};
        next.bytes.resize(static_cast<size_t>(f.total));
        assembly_ = std::move(next);
    }
    if (!matches(f) || now >= assembly_->deadline)
        return ControlStreamResult::Invalid;
    if ((f.stream_flags & CONTROL_STREAM_ERROR) != 0) {
        last_stream_id_ = f.stream_id;
        reset();
        return ControlStreamResult::Aborted;
    }
    auto &a = *assembly_;
    std::memcpy(a.bytes.data() + a.offset, bytes.data(), bytes.size());
    a.offset += bytes.size();
    ++a.next_sequence;
    if ((f.stream_flags & CONTROL_STREAM_END) == 0)
        return ControlStreamResult::Accepted;
    auto frame                  = std::make_unique<Frame>();
    frame->header.msg_type      = f.message_type;
    frame->header.msg_size      = static_cast<uint32_t>(f.total);
    frame->header.flags         = f.message_flags;
    frame->request_id           = f.request_id;
    frame->rpc_create_nano      = f.rpc_create_nano;
    frame->response_create_nano = f.response_create_nano;
    frame->control              = std::move(a.bytes);
    frame->control_owner        = std::move(a.lease);
    completed                   = std::move(frame);
    last_stream_id_             = f.stream_id;
    assembly_.reset();
    return ControlStreamResult::Complete;
}

bool ControlStreamReceiver::active() const
{
    return assembly_.has_value();
}

bool ControlStreamReceiver::expired(Clock::time_point now) const
{
    return assembly_ && now >= assembly_->deadline;
}

void ControlStreamReceiver::reset()
{
    assembly_.reset();
}
} // namespace crowdb::rpc
