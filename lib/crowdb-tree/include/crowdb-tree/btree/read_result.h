// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/buffer.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/maptable/page.h"

#include <memory>
#include <string>
#include <vector>

namespace crowdb::tree
{
struct scan_entry
{
    std::string key;
    uint64_t    slot;
    std::string value;
    bool        tombstone = false;
};

struct get_result
{
    bool        found = false;
    uint64_t    slot  = 0;
    std::string value;
};

// Zero-copy point-read result (plan-tree #5 B3 remaining). `value()` is a
// borrowed `Slice` for an L1 hit resolved to a non-overflow cell -- it
// points directly into the resident leaf's frame, kept alive for this
// object's lifetime by the epoch guard it owns (no copy). An L0 hit (R50:
// the MemTable's skip-list node is epoch-protected the same way an L1 frame
// is) also borrows directly off the node's cell version. An overflow value
// (assembled from multiple pages, no single frame to borrow) is materialized
// into an owned `buffer` instead; `value()` is transparent to the caller
// either way.
//
// Move-only (like `EpochManager::Guard`): copying would either double-free
// the guard or silently let a caller outlive it. `get()`/`multi_get()` are
// thin wrappers over `get_view()` that clone `value()` into a `std::string`
// and let the guard drop before returning, preserving their existing owned-
// copy contract for every other caller.
class GetView
{
  public:
    GetView() = default;

    // Not defaulted (found this the hard way, via
    // ASan): `owned_` (a `buffer`) relocates its bytes on move when small
    // enough to be inline (SBO, buffer::kInlineCap) -- but `value_` is a
    // *separate* field, a plain Slice pointer+len that a defaulted move
    // would blindly copy byte-for-byte, still aliasing the just-moved-from
    // `owned_`'s old (now-stale, for an inline buffer) storage. Any
    // resolved GetView whose value is owned (owned_ non-empty) must have
    // `value_` re-derived from *this* object's own (possibly relocated)
    // `owned_` after the move -- a borrowed (frame-pointing) value_ is
    // untouched either way, since it aliases external storage the move
    // never touches.
    GetView(GetView &&o) noexcept
        : guard_(std::move(o.guard_)),
          source_guard_(std::move(o.source_guard_)),
          found_(o.found_),
          slot_(o.slot_),
          value_(o.value_),
          owned_(std::move(o.owned_)),
          pins_(std::move(o.pins_))
    {
        if (!owned_.empty()) {
            value_ = owned_.slice();
        }
    }

    GetView &operator=(GetView &&o) noexcept
    {
        if (this != &o) {
            release_pins();
            guard_        = std::move(o.guard_);
            source_guard_ = std::move(o.source_guard_);
            found_        = o.found_;
            slot_         = o.slot_;
            value_        = o.value_;
            owned_        = std::move(o.owned_);
            pins_         = std::move(o.pins_);
            if (!owned_.empty()) {
                value_ = owned_.slice();
            }
        }
        return *this;
    }

    GetView(const GetView &)            = delete;
    GetView &operator=(const GetView &) = delete;

    ~GetView()
    {
        release_pins();
    }

    [[nodiscard]] bool found() const
    {
        return found_;
    }

    [[nodiscard]] uint64_t slot() const
    {
        return slot_;
    }

    // Valid only while this GetView is alive.
    [[nodiscard]] Slice value() const
    {
        return value_;
    }

    // R6 debug-only: the frame address a borrowed value points into, or
    // nullptr for an owned (L0 / overflow) value. Used by tests to verify
    // the get_async slow path returns a borrowed Slice (no copy).
    [[nodiscard]] const uint8_t *frame_base() const
    {
        return owned_.empty() ? value_.bytes() : nullptr;
    }

  private:
    friend class Crowdbtree;
    EpochManager::Guard                  guard_; // keeps an L1 hit's frame resident
    std::shared_ptr<EpochManager::Guard> source_guard_;
    bool                                 found_ = false;
    uint64_t                             slot_  = 0;
    Slice                                value_; // borrowed (L1) or owned_.slice() (L0 / overflow)
    buffer                               owned_; // backing storage when the value can't be borrowed
    // R6: cross-thread pins holding the borrowed value's chain alive after
    // the epoch guard is released (get_async slow path). Empty on the fast
    // path (guard_ alone keeps the frame resident) and for owned values.
    std::vector<PageBase *> pins_;
    // R6: the chain head whose frame/entries back the borrowed value_, set
    // by try_get_view_no_load when the value is borrowed (not L0/overflow).
    // Used by the slow path to walk + pin the chain before releasing guard_.
    PageBase *borrowed_chain_head_ = nullptr;

    void release_pins()
    {
        for (PageBase *p : pins_) {
            p->unpin();
        }
        pins_.clear();
    }
};

} // namespace crowdb::tree
