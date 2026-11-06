// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/portable_import.h"
#include "btree/tree_core.h"

namespace crowdb::tree
{
// A private staging stream holds byte admission but no accepted Tree ticket,
// backend alias, or IO permit while it waits for the caller's next chunk.
class RuntimeSnapshotImport
{
  public:
    static Status create(TreeCore &core, std::shared_ptr<RuntimeSnapshotImport> *out);
    RuntimeSnapshotImport(TreeCore &core, std::shared_ptr<detail::AllocationReservation> metadata);
    ~RuntimeSnapshotImport();
    Status feed(Slice bytes);
    Status finish(uint64_t *coverage);

  private:
    enum class Part : uint8_t {
        Header,
        KeyLength,
        Key,
        Slot,
        Kind,
        ValueLength,
        Value,
        PageId,
        FrameLength,
        Frame,
        Trailer,
        Complete
    };
    Status parse(std::span<const uint8_t> bytes, TreeCore &core);
    Status advance(TreeCore &core);
    Status advance_portable();
    Status advance_native(TreeCore &core);
    Status begin(TreeCore &core);
    void   consume_fixed(std::span<const uint8_t> &bytes, bool include_crc);
    void   consume_window(std::span<const uint8_t>                                  &bytes,
                          detail::ReservedVector<uint8_t, AllocationKind::kScratch> &window, size_t offset);
    void   expect(Part part, size_t bytes);
    void   completed_entry();
    void   expect_entry();
    Status fail(Status status);

    std::weak_ptr<TreeCore>                                   core_;
    std::shared_ptr<detail::AllocationReservation>            metadata_;
    PageLoadLimits                                            loads_;
    size_t                                                    max_key_;
    detail::ReservedUnique<PortableImportBuilder>             portable_;
    std::shared_ptr<ReadablePageCatalog>                      catalog_;
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> key_;
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> window_;
    std::array<uint8_t, 33>                                   fixed_{};
    size_t                                                    fixed_used_ = 0;
    size_t                                                    required_   = 9;
    size_t                                                    copied_     = 0;
    uint64_t                                                  coverage_   = 0;
    uint64_t                                                  expected_   = 0;
    uint64_t                                                  parsed_     = 0;
    uint64_t                                                  slot_       = 0;
    uint64_t                                                  pid_        = kInvalidPageId;
    uint32_t                                                  crc_        = 0;
    uint8_t                                                   kind_       = 0;
    bool                                                      native_     = false;
    bool                                                      finishing_  = false;
    Part                                                      part_       = Part::Header;
    Status                                                    status_;
};
} // namespace crowdb::tree
