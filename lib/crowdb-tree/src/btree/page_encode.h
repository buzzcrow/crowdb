// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_maintenance.h"
#include "btree/published_state.h"

#include <span>

namespace crowdb::tree
{
struct PageEncodeLimits
{
    uint32_t                          frame_bytes        = 64 * 1024;
    size_t                            inline_value_bytes = 8 * 1024;
    size_t                            max_sources        = 64;
    size_t                            max_entries        = 4096;
    size_t                            max_work_entries   = 65536;
    size_t                            max_metadata_bytes = 1024 * 1024;
    std::shared_ptr<AllocationBudget> metadata           = nullptr;
    std::shared_ptr<TreeWorkMetrics>  work_metrics       = nullptr;
    TreeWorkCause                     cause              = TreeWorkCause::Other;
};

struct InnerEncodeFences
{
    uint64_t lower_leaf = kInvalidPageId;
    uint64_t upper_leaf = kInvalidPageId;
};

struct CapturedEncodeContext
{
    // Half-open routing bounds supplied by the captured traversal. Durable
    // leaf fences instead contain the actual inclusive first/last output key.
    PageKeyRange range{};
    // Required for an inner page. Both invalid explicitly denotes an empty
    // subtree; otherwise these are captured descendants from the traversal.
    std::optional<InnerEncodeFences> inner_fences = std::nullopt;
    // Owns the captured backend mapping/extents for pre-existing overflow
    // references. A resident leaf frame alone does not protect its chain.
    std::shared_ptr<const void> overflow_dependencies = nullptr;
};

struct ReservedOverflowIds
{
    uint64_t first = kInvalidPageId;
    uint64_t count = 0;
    // The host reserves this interval independently of live page allocation
    // and retains its lifetime/protection through writes and checkpoint commit.
    std::shared_ptr<const void> owner = nullptr;
};

struct ReusedOverflowDependency
{
    uint64_t                                  head  = kInvalidPageId;
    uint64_t                                  bytes = 0;
    std::shared_ptr<const void>               owner;
    std::shared_ptr<const OverflowBackingSet> backing = nullptr;
};

struct FinalPageSummary
{
    // Inner subtree totals require the caller's captured child summaries.
    // Their zero-initialized live fields are unknown, not an empty subtree.
    std::optional<InnerEncodeFences> inner_fences          = std::nullopt;
    bool                             live_counts_known     = false;
    uint64_t                         oldest_tombstone_slot = UINT64_MAX;
    uint64_t                         encoded_entries       = 0;
    uint64_t                         live_keys             = 0;
    uint64_t                         live_key_bytes        = 0;
    uint64_t                         live_value_bytes      = 0;
    uint64_t                         dropped_tombstones    = 0;
    uint64_t                         primary_used_bytes    = 0;
    uint64_t                         new_overflow_frames   = 0;
};

// The page owner supplies the final page and its completed child fences.
// Encoding borrows this exact output; it does not build another mapping.
struct PageEncodeInput
{
    std::shared_ptr<const PublishedPage> page;
    uint64_t                             page_id;
    uint64_t                             coverage;
    uint64_t                             gc_floor;
    uint64_t                             next_page_id;
    CapturedEncodeContext                context;
    std::optional<FinalPageSummary>      children_summary = std::nullopt;
};

struct PageEncodeCounters
{
    uint64_t primary_frames            = 0;
    uint64_t overflow_frames           = 0;
    uint64_t key_copy_bytes            = 0;
    uint64_t inline_value_copy_bytes   = 0;
    uint64_t overflow_value_copy_bytes = 0;
};

struct EncodedFrameInfo
{
    uint64_t  page_id = kInvalidPageId;
    page_type type    = page_type::kLeafBase;
    uint32_t  bytes   = 0;
    // Carry into the asynchronous write and staged backing: bytes alone do
    // not protect the overflow references embedded in an output frame.
    std::shared_ptr<const void> dependencies = nullptr;
};

struct PageEncodeStep
{
    Status                          status;
    std::optional<EncodedFrameInfo> frame    = std::nullopt;
    bool                            complete = false;
};

struct InnerEncodeLoad
{
    uint64_t                          page_id = kInvalidPageId;
    PageKeyRange                      range;
    std::shared_ptr<const BackingRef> backing;
};

namespace detail
{
struct FinalPageEncodeState;
}
class FinalPageEncoder;

struct PageEncodePreparation
{
    Status                                   status;
    std::optional<BaseLoadRequest>           needs_base;
    std::optional<InnerEncodeLoad>           needs_inner;
    detail::ReservedUnique<FinalPageEncoder> encoder;
    // A layout capacity failure, distinct from metadata admission failure.
    bool needs_split = false;
};

// One encoder owns one captured page. It has one consumer and never performs
// I/O, consults a live mapping, or allocates output buffers. Each next() fills
// at most one caller-owned final frame; the caller controls in-flight windows.
class FinalPageEncoder
{
  public:
    ~FinalPageEncoder();
    FinalPageEncoder(const FinalPageEncoder &)                                                  = delete;
    FinalPageEncoder                                       &operator=(const FinalPageEncoder &) = delete;
    [[nodiscard]] const FinalPageSummary                   &summary() const noexcept;
    [[nodiscard]] const PageEncodeCounters                 &counters() const noexcept;
    [[nodiscard]] std::span<const ReusedOverflowDependency> reused_overflow() const noexcept;
    [[nodiscard]] std::shared_ptr<const void>               dependency_owner() const noexcept;
    [[nodiscard]] Status                                    bind_overflow(ReservedOverflowIds ids);
    [[nodiscard]] PageEncodeStep                            next(std::span<uint8_t> output);

  private:
    friend PageEncodePreparation prepare_final_page(const PageEncodeInput &, const PageEncodeLimits &);
    friend PageEncodePreparation prepare_final_page(const std::shared_ptr<const PublishedTreeState> &, uint64_t,
                                                    const CapturedEncodeContext &, const PageEncodeLimits &);
    explicit FinalPageEncoder(std::shared_ptr<detail::FinalPageEncodeState> state) noexcept;
    [[nodiscard]] PageEncodeStep                  next_frame(std::span<uint8_t> output);
    std::shared_ptr<detail::FinalPageEncodeState> state_;
};

// Preparation borrows values and bounds descriptor work. A cold input is
// returned before any output. A physical range request includes an encoder
// continuation; resume it instead of restarting and rescanning the prefix.
// Ordinary cold bases still use a privately loaded descriptor and retry.
// Oversized primary layouts require structural maintenance first.
PageEncodePreparation prepare_final_page(const std::shared_ptr<const PublishedTreeState> &captured, uint64_t pid,
                                         const CapturedEncodeContext &context, const PageEncodeLimits &limits = {});
PageEncodePreparation prepare_final_page(const PageEncodeInput &input, const PageEncodeLimits &limits = {});
} // namespace crowdb::tree
