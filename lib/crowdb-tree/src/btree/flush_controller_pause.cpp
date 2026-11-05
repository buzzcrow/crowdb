// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_controller_internal.h"

namespace crowdb::tree
{
Status FlushController::State::pause_source(const SourceCatalog &catalog)
{
    if (pause && !pause->matches(catalog)) {
        pause.reset();
    }
    if (!pause) {
        auto status = SourcePause::create(catalog, lifecycle, progress, &pause);
        if (!status.ok()) {
            return status;
        }
    }
    if (!pause->ready()) {
        // No caller owns an accepted plan yet. The pause retains itself until
        // the last short bind exits, then resumes ordinary admission and wakes
        // existing progress waiters. A synchronous rejection cannot strand it.
        pause->wake_on_release();
        pause.reset();
        return Status::unavailable("source binds are completing before flush planning");
    }
    return Status::Ok();
}
} // namespace crowdb::tree
