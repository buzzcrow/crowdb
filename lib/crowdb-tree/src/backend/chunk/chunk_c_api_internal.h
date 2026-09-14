// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_page_store.h"
#include "chunk_transport.h"

#include <memory>

struct ct_root_catalog
{
    std::shared_ptr<crowdb::tree::detail::RootCatalog>    catalog;
    std::shared_ptr<crowdb::tree::detail::ChunkTransport> transport;
};

struct ct_chunk_transport
{
    std::shared_ptr<crowdb::tree::detail::ChunkTransport> transport;
};
