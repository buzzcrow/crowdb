// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/c_api.h"

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
    std::shared_ptr<PageStoreBundle> bundle;
};
