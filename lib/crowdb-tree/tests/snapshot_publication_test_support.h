// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree::test_snapshot_publication
{
inline std::shared_ptr<PublishedPage> leaf(uint64_t content)
{
    auto page                = std::make_shared<PublishedPage>();
    auto state               = std::make_shared<PageState>();
    state->content_id        = content;
    state->route_generation  = 1;
    state->representation_id = 1;
    page->leaf               = std::move(state);
    page->metrics.dirty      = true;
    return page;
}

inline PublicationResult publish(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    transaction.record   = std::make_shared<InstallRecord>(InstallToken{.generation = publisher.capture()->generation});
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> completed;
    auto                            result = completed.get_future();
    request->on_done                       = [&](PublicationResult value) { completed.set_value(std::move(value)); };
    if (!publisher.try_submit(request)) {
        ADD_FAILURE() << "snapshot publication rejected";
        return {.status = Status::unavailable()};
    }
    return result.get();
}

inline PublicationResult checkpoint(PublicationCoordinator &publisher, CheckpointAction action, uint64_t token)
{
    PublicationTransaction transaction;
    transaction.checkpoint = {.action = action, .token = token};
    if (action == CheckpointAction::Capture) {
        transaction.expected_coverage = publisher.capture()->coverage;
    }
    return publish(publisher, std::move(transaction));
}
} // namespace crowdb::tree::test_snapshot_publication
