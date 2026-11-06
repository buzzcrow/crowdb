// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/page.h"
#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/status.h"

#include <memory>
#include <optional>
#include <span>
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
    uint64_t page_id = kInvalidPageId;
    // Runtime compatibility collectors account the original copied payload.
    // Declared before frame so its storage is freed before returning credit.
    // The outer vector and any caller copies or mutations are caller-owned.
    std::shared_ptr<const void> allocation_owner = nullptr;
    std::vector<uint8_t>        frame; // raw in-memory frame bytes (page_bytes() length)
    uint64_t                    durable_addr = kNoAddr;
    uint32_t                    durable_plen = 0;
    bool                        inherited    = false;
};

// Immutable output window. The owner accounts for and retains the byte
// capacity; copies share bytes, and the next window never overwrites them.
struct NativeFrameView
{
    uint64_t                    page_id = kInvalidPageId;
    std::span<const uint8_t>    bytes;
    std::shared_ptr<const void> owner;
    uint64_t                    durable_addr = kNoAddr;
    uint32_t                    durable_plen = 0;
    bool                        inherited    = false;
};

struct NativeStreamInfo
{
    uint64_t root_page_id      = kInvalidPageId;
    uint64_t at_slot           = 0;
    uint64_t next_page_id      = 0;
    uint64_t frame_count       = 0;
    bool     frame_count_known = false;
};

// Resumable view over one native tree generation. Runtime cursors capture an
// immutable mapping, reserve their overflow PIDs from the shared authority,
// and emit one independently owned frame window at a time.
class PublishedNativeCursor;

class NativeFrameIterator
{
  public:
    ~NativeFrameIterator();

    NativeFrameIterator(const NativeFrameIterator &)            = delete;
    NativeFrameIterator &operator=(const NativeFrameIterator &) = delete;
    NativeFrameIterator(NativeFrameIterator &&) noexcept;
    NativeFrameIterator &operator=(NativeFrameIterator &&) noexcept;

    Status                         next(size_t max_frames, std::vector<NativeFrame> *out, bool *complete);
    Status                         next_owned(std::optional<NativeFrameView> *out, bool *complete);
    [[nodiscard]] NativeStreamInfo stream_info() const;
    [[nodiscard]] uint64_t         root_page_id() const;
    [[nodiscard]] uint64_t         at_slot() const;
    [[nodiscard]] uint64_t         next_page_id() const;
    [[nodiscard]] uint64_t         subtrees_skipped() const;

  private:
    explicit NativeFrameIterator(std::shared_ptr<PublishedNativeCursor> runtime);

    std::shared_ptr<PublishedNativeCursor> runtime_;
    AllocationLease                        facade_credit_;
    friend class Crowdbtree;
};

} // namespace crowdb::tree
