// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// PT10: page compression (LZ4 default, identity fallback).
#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/maptable/compressor.h"
#include "crowdb-tree/maptable/frame_page.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

using namespace crowdb::tree;

namespace
{
constexpr uint32_t kPb = 4096;

std::string key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%05d", i);
    return b.data();
}

// A highly-compressible leaf frame (repetitive values).
std::vector<uint8_t> compressible_frame()
{
    std::vector<uint8_t> f(kPb);
    LeafFrameBuilder     b(f.data(), kPb);
    for (int i = 0; i < 40; ++i) {
        std::string cell = encode_cell(i, OpKind::kPut, Slice(std::string(20, 'a')));
        b.try_append_sorted(Slice(key(i)), Slice(cell));
    }
    b.finish(1, kInvalidPageId);
    return f;
}
} // namespace

TEST(PageCompression, RoundTripPreservesFrame)
{
    auto                 frame = compressible_frame();
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kLz4, &blob);

    std::vector<uint8_t> back(kPb);
    ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), back.data(), kPb).ok());
    EXPECT_EQ(frame, back);
    ASSERT_TRUE(frame_validate(back.data(), kPb));
}

TEST(PageCompression, CompressibleShrinksWhenLz4Available)
{
    auto                 frame = compressible_frame();
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kLz4, &blob);
    if (lz4_available()) {
        EXPECT_LT(blob.size(), static_cast<size_t>(kPb)); // actually compressed
    }
    else {
        EXPECT_GE(blob.size(), static_cast<size_t>(kPb)); // stored raw
    }
}

TEST(PageCompression, IncompressibleStoredRaw)
{
    std::vector<uint8_t> frame(kPb);
    std::mt19937         rng(1234); // NOLINT(bugprone-random-generator-seed) repeatable incompressible input
    for (auto &b : frame) {
        b = static_cast<uint8_t>(rng());
    }
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kLz4, &blob);
    // Random data does not shrink -> stored raw (algo byte 0 = kNone).
    EXPECT_EQ(blob[0], static_cast<uint8_t>(compress_algo::kNone));
    std::vector<uint8_t> back(kPb);
    ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), back.data(), kPb).ok());
    EXPECT_EQ(frame, back);
}

TEST(PageCompression, NonePreferStoresRaw)
{
    auto                 frame = compressible_frame();
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kNone, &blob);
    EXPECT_EQ(blob[0], static_cast<uint8_t>(compress_algo::kNone));
    std::vector<uint8_t> back(kPb);
    ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), back.data(), kPb).ok());
    EXPECT_EQ(frame, back);
}

TEST(PageCompression, CrcTamperRejected)
{
    auto                 frame = compressible_frame();
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kLz4, &blob);
    blob[kDurableBlobHeader + 1] ^= 0xff; // flip a stored byte
    std::vector<uint8_t> back(kPb);
    EXPECT_EQ(decode_durable_page(blob.data(), blob.size(), back.data(), kPb).code(), Code::kCorruption);
}

TEST(PageCompression, ShortBlobRejected)
{
    std::array<uint8_t, 4> b = {0};
    std::vector<uint8_t>   back(kPb);
    EXPECT_EQ(decode_durable_page(b.data(), b.size(), back.data(), kPb).code(), Code::kInvalidArgument);
}

TEST(PageCompression, BoundedEncoderUsesUnalignedCallerScratchAndExistingDecoder)
{
    const auto frame = compressible_frame();
    for (const auto algorithm : {compress_algo::kNone, compress_algo::kLz4}) {
        std::vector<uint8_t> output(kDurableBlobHeader + frame.size() + 2, 0xA5);
        std::vector<uint8_t> scratch(durable_encode_scratch_bytes(algorithm) + 1);
        uint32_t             length = 0;
        ASSERT_TRUE(encode_durable_page_into(frame, algorithm, std::span<uint8_t>(output).subspan(1, output.size() - 2),
                                             std::span<uint8_t>(scratch).subspan(1), &length)
                        .ok());
        EXPECT_EQ(output.front(), 0xA5);
        EXPECT_EQ(output.back(), 0xA5);
        std::vector<uint8_t> restored(frame.size());
        ASSERT_TRUE(decode_durable_page(output.data() + 1, length, restored.data(), restored.size()).ok());
        EXPECT_EQ(restored, frame);
        if (algorithm == compress_algo::kLz4 && lz4_available()) {
            EXPECT_LT(length, frame.size());
        }
        else {
            EXPECT_EQ(length, kDurableBlobHeader + frame.size());
        }
    }
}

TEST(PageCompression, BoundedEncoderRawFallbackOverwritesPartialCompressedOutput)
{
    std::vector<uint8_t> frame(kPb);
    std::mt19937         random(918); // NOLINT(bugprone-random-generator-seed) repeatable incompressible input
    for (auto &byte : frame) {
        byte = static_cast<uint8_t>(random());
    }
    std::vector<uint8_t> output(kDurableBlobHeader + frame.size());
    std::vector<uint8_t> scratch(durable_encode_scratch_bytes(compress_algo::kLz4));
    uint32_t             length = 0;
    ASSERT_TRUE(encode_durable_page_into(frame, compress_algo::kLz4, output, scratch, &length).ok());
    EXPECT_EQ(output[0], static_cast<uint8_t>(compress_algo::kNone));
    std::vector<uint8_t> legacy;
    encode_durable_page(frame.data(), frame.size(), compress_algo::kNone, &legacy);
    EXPECT_EQ(output, legacy);
    EXPECT_EQ(length, output.size());
}

TEST(PageCompression, BoundedEncoderRejectsInsufficientWindowsBeforeMutation)
{
    const auto           frame = compressible_frame();
    std::vector<uint8_t> output(kDurableBlobHeader + frame.size(), 0xA5);
    std::vector<uint8_t> scratch(durable_encode_scratch_bytes(compress_algo::kLz4));
    uint32_t             length = 123;
    EXPECT_EQ(encode_durable_page_into(frame, compress_algo::kLz4, std::span<uint8_t>(output).first(output.size() - 1),
                                       scratch, &length)
                  .code(),
              Code::kResourceExhausted);
    EXPECT_EQ(length, 123U);
    EXPECT_TRUE(std::all_of(output.begin(), output.end(), [](auto byte) { return byte == 0xA5; }));
    if (!scratch.empty()) {
        EXPECT_EQ(encode_durable_page_into(frame, compress_algo::kLz4, output,
                                           std::span<uint8_t>(scratch).first(scratch.size() - 1), &length)
                      .code(),
                  Code::kResourceExhausted);
        EXPECT_EQ(length, 123U);
    }
}
