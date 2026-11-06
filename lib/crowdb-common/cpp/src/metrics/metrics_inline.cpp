// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/metrics/metrics.h"

#include <limits>
#include <stdexcept>

namespace crowdb::common::metrics
{
MetricsRegistry::MetricsRegistry() : resource_(std::pmr::get_default_resource()), inline_gauges_(resource_)
{
}

MetricsRegistry::MetricsRegistry(std::shared_ptr<std::pmr::memory_resource> resource)
    : resource_owner_(std::move(resource)),
      resource_(resource_owner_ ? resource_owner_.get() : std::pmr::get_default_resource()),
      inline_gauges_(resource_)
{
}

InlineCallbackGauge *MetricsRegistry::register_inline(std::string_view prefix, std::string_view suffix,
                                                      InlineCallbackGauge::Callback callback)
{
    if (suffix.size() > std::numeric_limits<size_t>::max() - prefix.size()) {
        throw std::length_error("metrics name length overflow");
    }
    std::scoped_lock lock(flush_mutex_);
    for (auto &entry : inline_gauges_) {
        const std::string_view name(entry.name());
        if (name.size() == prefix.size() + suffix.size() && name.starts_with(prefix) && name.ends_with(suffix)) {
            entry.replace(callback);
            return &entry;
        }
    }
    std::pmr::string name(resource_);
    name.reserve(prefix.size() + suffix.size());
    name.append(prefix);
    name.append(suffix);
    inline_gauges_.emplace_back(std::move(name), callback);
    return &inline_gauges_.back();
}
} // namespace crowdb::common::metrics
