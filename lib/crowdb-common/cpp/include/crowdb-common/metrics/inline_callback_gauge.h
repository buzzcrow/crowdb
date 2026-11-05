// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <string>
#include <string_view>
#include <type_traits>

namespace crowdb::common::metrics
{
// Trivial callback descriptors stay in the paid registry node. No callable
// target, copied metric name or function wrapper is allocated on the side.
class InlineCallbackGauge
{
  public:
    class Callback
    {
      public:
        template <class F> explicit Callback(F function)
        {
            static_assert(std::is_trivially_copyable_v<F>);
            static_assert(sizeof(F) <= kCapacity);
            static_assert(std::is_invocable_r_v<uint64_t, const F &>);
            std::memcpy(storage_.data(), &function, sizeof(F));
            invoke_ = [](const std::byte *data) {
                std::array<std::byte, sizeof(F)> bytes;
                std::memcpy(bytes.data(), data, bytes.size());
                const auto function = std::bit_cast<F>(bytes);
                return static_cast<uint64_t>(function());
            };
        }

        [[nodiscard]] uint64_t operator()() const
        {
            return invoke_(storage_.data());
        }

      private:
        static constexpr size_t          kCapacity = 32;
        std::array<std::byte, kCapacity> storage_{};
        uint64_t (*invoke_)(const std::byte *) = nullptr;
    };

    InlineCallbackGauge(std::pmr::string name, Callback callback) : name_(std::move(name)), callback_(callback)
    {
    }

    [[nodiscard]] uint64_t get() const
    {
        return callback_();
    }

    [[nodiscard]] const std::pmr::string &name() const
    {
        return name_;
    }

    void replace(Callback callback) noexcept
    {
        callback_ = callback;
    }

  private:
    std::pmr::string name_;
    Callback         callback_;
};
} // namespace crowdb::common::metrics
