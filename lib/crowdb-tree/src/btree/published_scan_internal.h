// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/published_read_internal.h"

namespace crowdb::tree
{
struct PublishedScan::State : published_read_detail::ReadPump
{
    struct Source
    {
        std::optional<OwnedKey>           after;
        std::optional<PublishedScanEntry> candidate;
        bool                              done = false;
    };

    State(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<PageLoader> transport,
          PageKeyRange selection, bool descending, PublishedReadLimits bounds, PublishedScanOptions options);
    Status                                 &status() override;
    void                                    begin() override;
    bool                                    advance() override;
    void                                    loaded(const published_read_detail::LoadEvent &event) override;
    bool                                    fill_memtable(size_t index);
    bool                                    fill_leaf();
    bool                                    prepare_sources();
    [[nodiscard]] const PublishedScanEntry *winner() const;
    void                                    consume(Slice key);
    void                                    emit(PublishedScanEntry entry);

    PageKeyRange                              range;
    bool                                      reverse;
    PublishedScanOptions                      options;
    PublishedScanResult                       report;
    detail::ReservedVector<Source>            sources;
    PublishedRouteCursor                      walker;
    bool                                      walked          = false;
    bool                                      leaf_done       = false;
    bool                                      finished        = false;
    size_t                                    result_bytes    = 0;
    size_t                                    remaining_bytes = 0;
    bool                                      emitted         = false;
    std::shared_ptr<const PageState>          leaf;
    detail::ReservedUnique<LogicalPageCursor> cursor;
    std::optional<PublishedScanEntry>         leaf_candidate;
    std::optional<PublishedScanEntry>         pending_entry;
    size_t                                    loading_base  = 0;
    bool                                      loading_value = false;
    bool                                      loading_index = false;
};
} // namespace crowdb::tree
