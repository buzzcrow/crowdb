// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "native_import.h"
#include "owned_key.h"

namespace crowdb::tree
{
// Private import staging owns charged immutable frames and at most two frame
// windows. Input cells are borrowed only for append(); publication happens
// separately after the complete stream and its graph have been validated.
class PortableImportBuilder
{
  public:
    PortableImportBuilder(const Config &config, PageLoadLimits loads, uint64_t coverage,
                          std::shared_ptr<detail::AllocationReservation> metadata);
    Status                                     append(Slice key, Slice encoded);
    [[nodiscard]] static std::optional<size_t> allocation_bound(const NativeImportShape &shape) noexcept;
    Status                                     finish(std::shared_ptr<ReadablePageCatalog> *out);

  private:
    struct Node
    {
        uint64_t pid = kInvalidPageId;
        OwnedKey first;
        OwnedKey last;
        uint64_t first_leaf = kInvalidPageId;
        uint64_t last_leaf  = kInvalidPageId;
    };

    Status               append_impl(Slice key, Slice encoded);
    [[nodiscard]] Status validate(Slice key, Slice encoded) const;
    Status               save_leaf(uint64_t right);
    Status               spill_value(const CellView &cell, std::array<uint8_t, kOverflowCellSize> *encoded);
    Status               build_index();
    Status               build_parent(std::span<const Node> nodes, Node *out);
    Status               reserve_pid(uint64_t *out);
    void                 begin_leaf(uint64_t pid);

    Config                                                    config_;
    PageLoadLimits                                            loads_;
    PageKeyRange                                              range_;
    std::shared_ptr<detail::AllocationReservation>            metadata_;
    std::shared_ptr<ReadablePageCatalog>                      catalog_;
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> leaf_frame_;
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> scratch_;
    detail::ReservedVector<Node>                              level_;
    std::optional<LeafFrameBuilder>                           leaf_;
    OwnedKey                                                  first_;
    OwnedKey                                                  previous_;
    uint64_t                                                  next_pid_      = 0;
    uint64_t                                                  leaf_pid_      = kInvalidPageId;
    size_t                                                    used_          = kFrameHeaderSize + kFrameTrailerSize;
    size_t                                                    key_bytes_     = 0;
    bool                                                      have_previous_ = false;
    bool                                                      finished_      = false;
    Status                                                    status_;
};
} // namespace crowdb::tree
