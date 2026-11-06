// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_reservation.h"
#include "btree/backing_ref.h"
#include "btree/entry_key_arena.h"
#include "btree/merge_sources.h"
#include "btree/owned_key.h"
#include "btree/page_key_range.h"
#include "crowdb-tree/maptable/page.h"
#include "crowdb-tree/memtable/memtable.h"
#include "crowdb-tree/status.h"

#include <limits>
#include <optional>
#include <span>

namespace crowdb::tree
{
struct EntryRef
{
    std::shared_ptr<const EntryKeyArena> key_arena;
    size_t                               key_offset = 0;
    size_t                               key_length = 0;
    std::shared_ptr<const CellVersion>   owner;

    [[nodiscard]] Slice    key() const;
    [[nodiscard]] Slice    value() const;
    [[nodiscard]] uint64_t slot() const;
    [[nodiscard]] uint8_t  flags() const;
};

using EntryAllocator = detail::ReservationAllocator<EntryRef>;
using EntryStorage   = std::vector<EntryRef, EntryAllocator>;

struct RunLimits
{
    size_t max_entries     = 4096;
    size_t max_key_bytes   = 256 * 1024;
    size_t key_block_bytes = 16 * 1024;
};

// The builder owns writable arena blocks; published entries expose only const
// blocks. Blocks never grow after the first view has been handed out.
class KeyArenaBuilder
{
  public:
    explicit KeyArenaBuilder(RunLimits limits, std::shared_ptr<AllocationBudget> metadata = {})
        : limits_(limits),
          metadata_(std::move(metadata))
    {
    }

    Status append(Slice key, std::shared_ptr<const CellVersion> owner, EntryRef *out);

  private:
    RunLimits                         limits_;
    std::shared_ptr<AllocationBudget> metadata_;
    size_t                            entries_   = 0;
    size_t                            bytes_     = 0;
    size_t                            allocated_ = 0;
    size_t                            used_      = 0;
    std::shared_ptr<EntryKeyArena>    block_;
};

class OverlayBatch
{
  public:
    static constexpr bool observe_retirement = true;

    bool retire_step() noexcept
    {
        for (size_t i = 0; i < 64 && !entries_.empty(); ++i) {
            entries_.pop_back();
        }
        return entries_.empty();
    }

    static Status build(uint64_t id, std::vector<EntryRef> entries, const RunLimits &limits,
                        std::shared_ptr<const OverlayBatch> *out, std::shared_ptr<AllocationBudget> metadata = {});
    static Status build_owned(uint64_t id, EntryStorage entries, const RunLimits &limits,
                              std::shared_ptr<const OverlayBatch> *out);

    explicit OverlayBatch(std::shared_ptr<AllocationBudget> metadata = {})
        : entries_(EntryAllocator(std::move(metadata)))
    {
    }

    [[nodiscard]] uint64_t id() const
    {
        return id_;
    }

    [[nodiscard]] std::span<const EntryRef> entries() const
    {
        return entries_;
    }

    [[nodiscard]] const EntryRef *find(Slice key) const;
    [[nodiscard]] size_t          lower_bound(Slice key) const;

    [[nodiscard]] uint64_t min_slot() const
    {
        return min_slot_;
    }

    [[nodiscard]] uint64_t max_slot() const
    {
        return max_slot_;
    }

    [[nodiscard]] size_t key_bytes() const
    {
        return key_bytes_;
    }

    [[nodiscard]] bool has_overflow_values() const noexcept
    {
        return has_overflow_values_;
    }

  private:
    uint64_t     id_                  = 0;
    uint64_t     min_slot_            = UINT64_MAX;
    uint64_t     max_slot_            = 0;
    size_t       key_bytes_           = 0;
    bool         has_overflow_values_ = false;
    EntryStorage entries_;
};

class OverlayBuilder
{
  public:
    explicit OverlayBuilder(RunLimits limits, std::shared_ptr<AllocationBudget> metadata = {})
        : limits_(limits),
          metadata_(std::move(metadata)),
          arena_(limits, metadata_),
          entries_(EntryAllocator(metadata_))
    {
    }

    Status append(Slice key, std::shared_ptr<const CellVersion> owner);
    Status append(EntryRef entry);
    Status finish(uint64_t id, std::shared_ptr<const OverlayBatch> *out);

  private:
    RunLimits                         limits_;
    std::shared_ptr<AllocationBudget> metadata_;
    KeyArenaBuilder                   arena_;
    size_t                            key_bytes_ = 0;
    EntryStorage                      entries_;
};

struct BaseRef
{
    uint64_t                identity   = 0;
    uint64_t                generation = 0;
    PageKeyRange            range{};
    std::optional<uint64_t> max_slot = std::nullopt;
    // A resident owner can be an aliasing shared_ptr backed by a page pin.
    std::shared_ptr<const LeafBase>   resident = nullptr;
    std::shared_ptr<const BackingRef> backing  = nullptr;
    [[nodiscard]] bool                valid() const;
};

struct LogicalCell
{
    std::shared_ptr<const void>       owner;
    uint64_t                          slot  = 0;
    uint8_t                           flags = 0;
    Slice                             value;
    Slice                             encoded;
    std::shared_ptr<const BackingRef> backing = nullptr;

    [[nodiscard]] bool tombstone() const
    {
        return (flags & kFlagTombstone) != 0;
    }

    [[nodiscard]] bool overflow() const
    {
        return (flags & kFlagOverflow) != 0;
    }

    static LogicalCell from_entry(const EntryRef &entry);
    static LogicalCell from_base(const std::shared_ptr<const LeafBase> &base, size_t index,
                                 std::shared_ptr<const BackingRef> backing = {});
};

enum class LogicalLookupState : uint8_t { kMissing, kValue, kTombstone, kNeedsBase, kInvalid };

struct LogicalLookup
{
    LogicalLookupState state = LogicalLookupState::kMissing;
    LogicalCell        cell;
    size_t             base_index = 0;
    Status             status;
};

// Construct off to the side and publish as shared_ptr<const PageState>. No
// published container or frame may be mutated. Residency changes create a new
// descriptor while preserving base/content identity.
using OverlayAllocator = detail::ReservationAllocator<std::shared_ptr<const OverlayBatch>>;
using OverlayStorage   = std::vector<std::shared_ptr<const OverlayBatch>, OverlayAllocator>;

struct PageState
{

    using BaseAllocator = detail::ReservationAllocator<BaseRef>;
    using Bases         = std::vector<BaseRef, BaseAllocator>;

    [[nodiscard]] static size_t allocation_bound() noexcept
    {
        return detail::shared_allocation_bound<PageState>();
    }

    bool retire_step() noexcept
    {
        size_t steps = 0;
        for (; steps < 64 && !overlays.empty(); ++steps) {
            overlays.pop_back();
        }
        for (; steps < 64 && !bases.empty(); ++steps) {
            bases.pop_back();
        }
        return overlays.empty() && bases.empty();
    }

    explicit PageState(std::shared_ptr<AllocationBudget> metadata = {})
        : bases(BaseAllocator(metadata)),
          overlays(OverlayAllocator(std::move(metadata)))
    {
    }

    PageState(const PageState &source, std::shared_ptr<AllocationBudget> metadata = {})
        : range(source.range),
          route_generation(source.route_generation),
          content_id(source.content_id),
          representation_id(source.representation_id),
          right_sibling(source.right_sibling),
          bases(source.bases.begin(), source.bases.end(), BaseAllocator(metadata)),
          overlays(source.overlays.begin(), source.overlays.end(), OverlayAllocator(std::move(metadata)))
    {
    }

    PageKeyRange range;
    uint64_t     route_generation  = 0;
    uint64_t     content_id        = 0;
    uint64_t     representation_id = 0;
    uint64_t     right_sibling     = kInvalidPageId;
    Bases        bases;
    // Stable source order breaks equal-slot ties; callers place overlays first.
    OverlayStorage overlays;

    [[nodiscard]] LogicalLookup lookup(Slice key, uint64_t frontier = UINT64_MAX) const;
};

// Thread-bound merge operation. Its guards never cross an asynchronous suspend.
// next() returns independent key/cell owners that can cross threads freely.
class MemTableRangeMerge
{
  public:
    MemTableRangeMerge(std::initializer_list<std::shared_ptr<MemTable>> sources, PageKeyRange range, uint64_t frontier,
                       uint64_t floor, RunLimits limits, size_t max_sources,
                       std::shared_ptr<AllocationBudget> metadata = {});
    MemTableRangeMerge(std::span<const std::shared_ptr<MemTable>> sources, PageKeyRange range, uint64_t frontier,
                       uint64_t floor, RunLimits limits, size_t max_sources,
                       std::shared_ptr<AllocationBudget> metadata = {});
    MemTableRangeMerge(const MemTableRangeMerge &)            = delete;
    MemTableRangeMerge &operator=(const MemTableRangeMerge &) = delete;

    [[nodiscard]] const Status &status() const
    {
        return status_;
    }

    [[nodiscard]] bool valid() const;
    // Borrowed from the current guarded source; copy before leaving this step.
    [[nodiscard]] Slice key() const;
    Status              next(EntryRef *out);
    // After publishing/finishing a bounded run, start another arena window
    // without changing the source positions. Existing entries keep their blocks.
    void rotate_arena();

    [[nodiscard]] uint64_t candidates() const noexcept
    {
        return candidates_;
    }

  private:
    uint64_t                                                                                          candidates_ = 0;
    std::vector<std::shared_ptr<MemTable>, detail::ReservationAllocator<std::shared_ptr<MemTable>>>   owners_;
    std::vector<EpochManager::Guard, detail::ReservationAllocator<EpochManager::Guard>>               guards_;
    PageKeyRange                                                                                      range_;
    RunLimits                                                                                         limits_;
    std::shared_ptr<AllocationBudget>                                                                 metadata_;
    KeyArenaBuilder                                                                                   arena_;
    std::vector<ConcurrentSkipList::Cursor, detail::ReservationAllocator<ConcurrentSkipList::Cursor>> cursors_;
    std::vector<MergeSource, detail::ReservationAllocator<MergeSource>>                               sources_;
    LoserTree                                                                                         merge_;
    Status                                                                                            status_;
};

struct LogicalCursorStart
{
    OwnedKey key;
    bool     inclusive = true;
};

// Bounded merge of immutable runs and base ranges. Ordinary cold bases need
// a new page descriptor; range frames resume the same cursor after each load.
// max_sources controls the merge window rather than legal leaf contents.
class LogicalPageCursor
{
  public:
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t max_sources) noexcept;
    LogicalPageCursor(std::shared_ptr<const PageState> page, PageKeyRange range, uint64_t frontier, size_t max_sources,
                      bool reverse = false, std::shared_ptr<AllocationBudget> metadata = {},
                      std::optional<LogicalCursorStart> start = {});
    ~LogicalPageCursor();

    [[nodiscard]] const Status &status() const
    {
        return status_;
    }

    [[nodiscard]] std::optional<size_t> needs_base() const
    {
        return needs_base_;
    }

    [[nodiscard]] bool valid() const
    {
        return winner_.has_value();
    }

    [[nodiscard]] Slice       key() const;
    [[nodiscard]] LogicalCell cell() const;
    void                      next();

  private:
    struct Source
    {
        const OverlayBatch *run       = nullptr;
        const BaseRef      *base      = nullptr;
        size_t              index     = 0;
        bool                exhausted = false;
    };

    [[nodiscard]] Slice       source_key(const Source &source) const;
    [[nodiscard]] LogicalCell source_cell(const Source &source) const;
    [[nodiscard]] uint64_t    source_slot(const Source &source) const;
    [[nodiscard]] size_t      source_size(const Source &source) const;
    void                      position(Source *source);
    void                      step(Source *source);
    void                      select();
    void                      seek(Source *source, std::optional<Slice> bound, bool exclusive);
    void                      select_windowed(std::optional<Slice> after);
    [[nodiscard]] std::pair<std::optional<Slice>, bool> initial_bound() const noexcept;

    std::shared_ptr<const PageState>  page_;
    PageKeyRange                      range_;
    uint64_t                          frontier_;
    bool                              reverse_;
    bool                              windowed_ = false;
    detail::ReservedVector<Source>    sources_;
    std::optional<LogicalCursorStart> start_;
    std::optional<size_t>             winner_;
    std::optional<size_t>             needs_base_;
    Status                            status_;
};
} // namespace crowdb::tree
