// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "flush_batch_internal.h"
#include "maintenance_publication_internal.h"
#include "maintenance_split_layout.h"

namespace crowdb::tree
{
// One page task owns its leaf work or joins its direct children. Only the
// parent consumes sibling outputs; no child publishes an ancestor change.
struct FlushBatch::State::Subtree
{
    struct Children;
    Subtree(State &state, uint64_t page_id, PageKeyRange range, bool selected);
    ~Subtree();
    detail::CallbackSender                             sender() noexcept;
    void                                               start(detail::CallbackComplete callback, void *argument);
    void                                               schedule() noexcept;
    static void                                        run(void *raw) noexcept;
    void                                               step();
    void                                               fork();
    void                                               leaf();
    Status                                             coalesce_leaf(const PageMaintenanceLimits &limits);
    void                                               join();
    void                                               finish_pages();
    void                                               persist_next();
    Status                                             prepare_root();
    [[nodiscard]] InnerEncodeFences                    fences(uint64_t page_id) const;
    [[nodiscard]] FinalPageSummary                     child_summary(uint64_t page_id) const;
    [[nodiscard]] std::shared_ptr<const PublishedPage> find_page(uint64_t page_id) const;
    [[nodiscard]] Subtree                             *page_owner(uint64_t page_id);
    void                                               complete(Status result) noexcept;
    void                                               prepare_builder();
    static size_t                                      fork_bytes() noexcept;
    Subtree                                           *leaf_next   = nullptr;
    bool                                               leaf_permit = false;
    void                                               reset_work();
    void                                               request_load();
    void                                               load() noexcept;
    Subtree                                           *load_next      = nullptr;
    bool                                               load_completed = false;
    Status                                             collect(PublicationTransaction &transaction);
    [[nodiscard]] std::shared_ptr<const PublishedPage> lookup(uint64_t page_id) const;

    State       &owner;
    uint64_t     pid;
    PageKeyRange bounds;
    bool         active;
    bool         finished       = false;
    bool         handed_off     = false;
    bool         children_ready = false;
    bool         joined         = false;
    bool         writing        = false;
    size_t       persist_at     = 0;
    uint64_t     final_root     = kInvalidPageId;

    struct FinalPage
    {
        Subtree *task;
        uint64_t pid;
    };

    detail::ReservedVector<FinalPage>                  final_pages;
    std::optional<TreeMetricsOperation>                layout_metrics;
    Status                                             status;
    std::shared_ptr<const PublishedPage>               expected;
    std::shared_ptr<const InnerPageState>              inner;
    std::shared_ptr<PageState>                         loaded_leaf;
    size_t                                             next_base = 0;
    std::shared_ptr<Range>                             range;
    detail::ReservedUnique<detail::SplitWindowCursor>  layout;
    detail::ReservedUnique<detail::MaintenanceBuilder> builder;
    detail::MaintenanceChildren                        output;
    detail::ReservedVector<std::shared_ptr<Subtree>>   children;
    detail::ReservedUnique<Children>                   child_join;
    ApplyDispatcher::Node                              cpu_node{&Subtree::run, this};
    detail::CallbackComplete                           done    = nullptr;
    void                                              *context = nullptr;
};
} // namespace crowdb::tree
