// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "published_state.h"

namespace crowdb::tree
{
[[nodiscard]] bool   logical_page_change(const PageReplacement &page);
[[nodiscard]] Status validate_snapshot_publication(const PublicationTransaction &transaction,
                                                   const PublishedTreeState     &current);
void                 initialize_snapshot_changes(PublishedTreeState                                   &state,
                                                 const std::shared_ptr<detail::AllocationReservation> &metadata);
void prepare_snapshot_changes(const PublicationTransaction &transaction, const PublishedTreeState &current,
                              PublishedTreeState &next);
} // namespace crowdb::tree
