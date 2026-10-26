// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace crowdb::rpc
{
// Cold status serialization; keys and strings are escaped without a runtime
// JSON dependency in the portable transport library.
class StatusJson
{
  public:
    void number(std::string_view key, uint64_t value)
    {
        raw(key, std::to_string(value));
    }

    void boolean(std::string_view key, bool value)
    {
        raw(key, value ? "true" : "false");
    }

    void string(std::string_view key, std::string_view value)
    {
        raw(key, quote(value));
    }

    void null(std::string_view key)
    {
        raw(key, "null");
    }

    void raw(std::string_view key, std::string_view value)
    {
        if (text_.size() != 1)
            text_ += ',';
        text_ += quote(key);
        text_ += ':';
        text_ += value;
    }

    [[nodiscard]] std::string finish() const
    {
        return text_ + '}';
    }

  private:
    std::string text_ = "{";

    static std::string quote(std::string_view value)
    {
        constexpr char hex[] = "0123456789abcdef";
        std::string    text  = "\"";
        for (const unsigned char byte : value) {
            if (byte == '"' || byte == '\\') {
                text += '\\';
                text += static_cast<char>(byte);
            }
            else if (byte < 32) {
                text += "\\u00";
                text += hex[byte >> 4];
                text += hex[byte & 15];
            }
            else {
                text += static_cast<char>(byte);
            }
        }
        return text + '"';
    }
};
} // namespace crowdb::rpc
