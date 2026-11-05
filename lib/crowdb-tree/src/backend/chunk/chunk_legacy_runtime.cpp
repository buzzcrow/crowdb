// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_page_store.h"

namespace crowdb::tree::detail
{
PageStore::RuntimeFactory ChunkPageStore::runtime_factory() const noexcept
{
    return [](const crowdb::tree::Config &, std::shared_ptr<TreeCore> *) {
        return Status::not_supported("legacy ChunkPageStore requires migrate_legacy_chunk");
    };
}
} // namespace crowdb::tree::detail
