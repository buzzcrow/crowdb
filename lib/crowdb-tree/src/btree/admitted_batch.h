// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "tree_core_batch.h"

namespace crowdb::tree
{
// Owns one validated, deduplicated request before consensus assigns its slot.
// This owner has no source entrance or epoch guard. Incoming bytes and commit
// descriptors are charged separately from the prepaid source allocation.
class AdmittedBatch
{
  public:
    struct Plan
    {
        size_t         incoming_bytes = 0;
        size_t         staging_bytes  = 0;
        size_t         source_bytes   = 0;
        WriteFootprint footprint;
    };

    // Derive the whole input/source bound without allocation or source entrance.
    static Status plan(const Config &config, const SourceWriteOptions &options, size_t max_value_bytes, size_t count,
                       const CoreWriteReader &read, Plan *out);
    static Status bind_prepaid(const SourceWriteOptions &options, size_t count, const CoreWriteReader &read,
                               detail::BudgetClass category, const Plan &plan, detail::ResourceBudget::Credit staging,
                               detail::ResourceBudget::Credit source, std::shared_ptr<AdmittedBatch> *out);
    static Status create(const Config &config, const SourceWriteOptions &options, size_t max_value_bytes, size_t count,
                         const CoreWriteReader &read, detail::BudgetClass category,
                         std::shared_ptr<AdmittedBatch> *out);

    // A busy source or distant frontier leaves all bytes and credit intact.
    // Once a source is bound, this batch is consumed exactly once. The caller
    // retains the generation and supplies asynchronous progress on busy.
    Status try_commit(SourceAdmission &sources, ApplyFrontier &frontier, uint64_t slot);

    [[nodiscard]] bool consumed() const noexcept
    {
        return consumed_;
    }

    [[nodiscard]] size_t size() const noexcept
    {
        return cells_.size();
    }

    [[nodiscard]] WriteFootprint footprint() const noexcept
    {
        return source_.footprint();
    }

  private:
    struct Cell
    {
        size_t  key_offset   = 0;
        size_t  key_size     = 0;
        size_t  value_offset = 0;
        size_t  value_size   = 0;
        uint8_t flags        = 0;
    };

    explicit AdmittedBatch(std::shared_ptr<detail::AllocationReservation> metadata);
    Status initialize(const SourceWriteOptions &options, size_t count, const CoreWriteReader &read,
                      detail::BudgetClass category, size_t incoming_bytes, SourceWriteReservation prepaid = {});
    Status prepare_cells(SourceAdmission::Writer &writer);

    std::shared_ptr<detail::AllocationReservation>            metadata_;
    SourceWriteReservation                                    source_;
    detail::ReservedVector<uint8_t, AllocationKind::kPayload> bytes_;
    detail::ReservedVector<Cell>                              cells_;
    detail::ReservedVector<SourceAdmission::Writer::Prepared> prepared_;
    bool                                                      consumed_ = false;
};
} // namespace crowdb::tree
