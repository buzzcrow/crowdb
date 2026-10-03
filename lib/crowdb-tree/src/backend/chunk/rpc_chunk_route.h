// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_transport.h"
#include "crowdb-tree/c_api.h"

namespace crowdb::tree::detail
{

struct RpcChunkRouteLease
{
    ct_chunk_rpc_route route{};
    void              *owner = nullptr;
    void (*release)(void *)  = nullptr;

    RpcChunkRouteLease()                                      = default;
    RpcChunkRouteLease(const RpcChunkRouteLease &)            = delete;
    RpcChunkRouteLease &operator=(const RpcChunkRouteLease &) = delete;

    ~RpcChunkRouteLease()
    {
        if (owner != nullptr && release != nullptr) {
            release(owner);
        }
    }
};

class RpcChunkRouteResolver
{
  public:
    explicit RpcChunkRouteResolver(const ct_chunk_rpc_transport_options &options)
        : callbacks_(options.chunkdb_resolver),
          fixed_(options.chunkdb)
    {
        if (complete()) {
            callbacks_.retain_context(callbacks_.context);
        }
    }

    RpcChunkRouteResolver(const RpcChunkRouteResolver &)            = delete;
    RpcChunkRouteResolver &operator=(const RpcChunkRouteResolver &) = delete;

    ~RpcChunkRouteResolver()
    {
        if (complete()) {
            callbacks_.release_context(callbacks_.context);
        }
    }

    [[nodiscard]] bool valid() const
    {
        return callbacks_.resolve != nullptr
                 ? complete()
                 : fixed_.client != nullptr && fixed_.server != nullptr && fixed_.connection != nullptr;
    }

    Status resolve(ChunkId chunk, bool refresh, RpcChunkRouteLease *lease) const
    {
        if (!valid() || lease == nullptr) {
            return Status::unavailable("chunk service resolver is unavailable");
        }
        if (callbacks_.resolve == nullptr) {
            lease->route = fixed_;
            return Status::Ok();
        }
        lease->release = callbacks_.release_route;
        const auto status =
            callbacks_.resolve(callbacks_.context, chunk.high, chunk.low, refresh, &lease->route, &lease->owner);
        return status == 0 ? Status::Ok() : Status::unavailable("chunk service slot owner is unavailable");
    }

  private:
    [[nodiscard]] bool complete() const
    {
        return callbacks_.context != nullptr && callbacks_.resolve != nullptr && callbacks_.release_route != nullptr &&
               callbacks_.retain_context != nullptr && callbacks_.release_context != nullptr;
    }

    ct_chunk_rpc_resolver callbacks_;
    ct_chunk_rpc_route    fixed_;
};

} // namespace crowdb::tree::detail
