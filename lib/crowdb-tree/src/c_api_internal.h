// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/memory_budget.h"

#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#endif

#include <memory>
#include <string>

struct PageStoreBundle
{
    std::unique_ptr<crowdb::tree::PageStore> store;
#ifdef CROWDB_HAVE_LIBURING
    // Declare uring before async_store so reverse destruction order releases
    // the non-owning async wrapper before its reactor.
    std::unique_ptr<crowdb::common::DiskIOUring> uring;
#endif
    std::unique_ptr<crowdb::tree::AsyncPageStore> async_store;
    crowdb::tree::AsyncPageStore                 *async_store_view = nullptr;
    std::string                                   backend_label;
};

struct ct_page_store
{
    crowdb::tree::AllocationLease    allocation;
    std::shared_ptr<PageStoreBundle> bundle;
};

namespace crowdb::tree
{
class Crowdbtree;
struct Config;
} // namespace crowdb::tree
struct ct_runtime;
struct ct_tree_runtime_options;
crowdb::tree::Crowdbtree *c_api_tree(ct_tree *tree) noexcept;
crowdb::tree::PageStore  *c_api_page_store(ct_tree *tree) noexcept;
struct CompletionSignal;
std::shared_ptr<CompletionSignal> c_api_completion_signal(ct_tree *tree) noexcept;
void                              c_api_notify_completion(const std::shared_ptr<CompletionSignal> &signal) noexcept;
// Backend-specific entry points supply their implementation so ordinary open
// keeps no link-time dependency on an optional migration backend.
using CApiMigration = crowdb::tree::Status (*)(const crowdb::tree::Config &, uint64_t,
                                               std::unique_ptr<crowdb::tree::Crowdbtree> *);
ct_status ct_migrate_configured(const ct_options *opt, ct_runtime *runtime, const ct_tree_runtime_options *binding,
                                uint64_t generation, CApiMigration migrate, ct_tree **out);
ct_status ct_open_configured(const ct_options *options, ct_runtime *runtime, const ct_tree_runtime_options *binding,
                             ct_tree **out);

ct_status ct_rebuild_configured(ct_tree *source, const ct_options *destination_options, crowdb::tree::Config options,
                                ct_tree **out, ct_range_rebuild_stats *stats);
