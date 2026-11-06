// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "recovered_state_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

TEST(InnerLoadProof, OwnsByteFencesAndDropsProofWhenRoutingIsCopied)
{
    TestRecoveredGraph   test;
    std::vector<uint8_t> raw(4096);
    ASSERT_TRUE(inner_frame_build(raw.data(), raw.size(), 0, {1, 2}, {"m"}));
    const Slice lower("a");
    const Slice upper("z");
    ASSERT_TRUE(frame_set_fences(raw.data(), raw.size(), &lower, &upper));
    test.add(0, raw);
    std::shared_ptr<const InnerPageState> result;
    test.loader->load_inner(test.pages->frames.at(0).backing, [&](Status status, auto loaded) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(loaded);
    });
    test.runtime->drain();
    ASSERT_NE(result, nullptr);
    const auto &stored = result->frame_proof;
    if (!stored.has_value()) {
        FAIL() << "decoded index has no frame proof";
        return;
    }
    const auto &proof = *stored;
    EXPECT_TRUE(proof.has_fences);
    EXPECT_FALSE(proof.page_id_fences);
    EXPECT_EQ(proof.self_page_id, 0U);
    EXPECT_EQ(proof.frame_bytes, 4096U);
    EXPECT_EQ(proof.lower, "a");
    EXPECT_EQ(proof.upper, "z");
    auto copied = detail::reserved_shared<InnerPageState>(test.credits.credits.reservation(), *result,
                                                          test.credits.credits.reservation());
    EXPECT_FALSE(copied->frame_proof);
    copied.reset();
    result.reset();
    test.loader.reset();
    test.pages.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(InnerLoadProof, FenceKeysConsumeTheSameBoundAsSeparators)
{
    TestRecoveredGraph   test;
    std::vector<uint8_t> raw(4096);
    ASSERT_TRUE(inner_frame_build(raw.data(), raw.size(), 0, {1, 2}, {"m"}));
    const Slice lower("a");
    const Slice upper("z");
    ASSERT_TRUE(frame_set_fences(raw.data(), raw.size(), &lower, &upper));
    test.add(0, raw);
    PageLoadLimits limits;
    limits.max_inner_key_bytes = 2;
    auto   loader = std::make_shared<AsyncStorePageLoader>(test.store, test.runtime, test.lane, limits,
                                                           OverflowBackingResolver{}, test.credits.credits);
    size_t calls  = 0;
    loader->load_inner(test.pages->frames.at(0).backing, [&](Status status, auto loaded) {
        EXPECT_EQ(status.code(), Code::kResourceExhausted);
        EXPECT_FALSE(loaded);
        ++calls;
    });
    test.runtime->drain();
    EXPECT_EQ(calls, 1U);
}
} // namespace crowdb::tree
