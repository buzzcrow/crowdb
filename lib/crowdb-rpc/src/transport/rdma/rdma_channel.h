// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <fcntl.h>
#    include <poll.h>
#    include <rdma/rdma_cma.h>

namespace crowdb::rpc::rdma_detail
{
inline void make_nonblocking(int fd)
{
    if (fd < 0)
        return;
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        (void)::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

inline bool wait_cm_event(rdma_event_channel *channel, rdma_cm_event **event, int timeout_ms)
{
    if (channel == nullptr || event == nullptr)
        return false;
    pollfd descriptor{channel->fd, POLLIN, 0};
    if (::poll(&descriptor, 1, timeout_ms) <= 0 || (descriptor.revents & POLLIN) == 0)
        return false;
    return rdma_get_cm_event(channel, event) == 0;
}
} // namespace crowdb::rpc::rdma_detail

#endif
