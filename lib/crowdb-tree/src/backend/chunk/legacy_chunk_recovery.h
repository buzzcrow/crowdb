// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/tree_core.h"
#include "legacy_chunk_read_store.h"

namespace crowdb::tree::detail
{
// Host-only, before the destination is exposed. The caller explicitly resolves
// the generation pin after deciding whether root publication is known.
Status recover_legacy_chunk_source(const Config &destination, uint64_t generation,
                                   std::shared_ptr<LegacyChunkReadStore> *store, std::shared_ptr<TreeCore> *source);
} // namespace crowdb::tree::detail
