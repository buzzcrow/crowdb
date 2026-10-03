// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/page.h"
#include "crowdb-tree/status.h"

#include <memory>
#include <vector>

namespace crowdb::tree
{
// One page's raw frame bytes, tagged with its logical PID (plan-tree #16
// native snapshot format). Unlike the portable format's `leaf_entry`
// (decoded key/cell tuples), this is the frame verbatim -- no
// encode/decode, no cell-by-cell rebuild on import, so a leaf/inner/
// overflow page round-trips as one `memcpy`-equivalent copy.
struct NativeFrame
{
    uint64_t             page_id = kInvalidPageId;
    std::vector<uint8_t> frame; // raw in-memory frame bytes (page_bytes() length)
    uint64_t             durable_addr = kNoAddr;
    uint32_t             durable_plen = 0;
    bool                 inherited    = false;
};

// Resumable view over one native tree generation. Creation folds pending leaf
// deltas, then pins only the root. Each next() call advances a bounded DFS
// batch; concurrent writers preserve overwritten pre-generation pages under a
// fixed pin budget, and consumed pins are released immediately. Different
// cursors share no mutable traversal state.
class NativeFrameIterator
{
  public:
    ~NativeFrameIterator();

    NativeFrameIterator(const NativeFrameIterator &)            = delete;
    NativeFrameIterator &operator=(const NativeFrameIterator &) = delete;
    NativeFrameIterator(NativeFrameIterator &&) noexcept;
    NativeFrameIterator &operator=(NativeFrameIterator &&) noexcept;

    Status                 next(size_t max_frames, std::vector<NativeFrame> *out, bool *complete);
    [[nodiscard]] uint64_t root_page_id() const;
    [[nodiscard]] uint64_t at_slot() const;
    [[nodiscard]] uint64_t next_page_id() const;
    [[nodiscard]] uint64_t subtrees_skipped() const;

  private:
    struct Impl;
    explicit NativeFrameIterator(std::shared_ptr<Impl> impl);

    std::shared_ptr<Impl> impl_;
    friend class Crowdbtree;
};

} // namespace crowdb::tree
