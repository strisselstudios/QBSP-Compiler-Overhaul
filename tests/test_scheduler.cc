#include <common/scheduler.hh>

#include <algorithm>
#include <atomic>

#include <gtest/gtest.h>

#include <tbb/info.h>
#include <tbb/task_group.h>

TEST(SchedulerRuntime, Lifecycle)
{
    auto &runtime = scheduler::runtime::instance();

    EXPECT_FALSE(runtime.configured());
    EXPECT_EQ(runtime.concurrency(), 0);
    EXPECT_FALSE(runtime.cancelled());

    const int default_concurrency = tbb::info::default_concurrency();

    ASSERT_GT(default_concurrency, 0);

    EXPECT_EQ(
        scheduler::runtime::resolve_concurrency(0),
        default_concurrency);

    EXPECT_EQ(
        scheduler::runtime::resolve_concurrency(-1),
        default_concurrency);

    EXPECT_EQ(
        scheduler::runtime::resolve_concurrency(3),
        3);

    const int requested_concurrency =
        std::max(1, std::min(2, default_concurrency));

    ASSERT_TRUE(runtime.configure(requested_concurrency));

    EXPECT_TRUE(runtime.configured());
    EXPECT_EQ(runtime.concurrency(), requested_concurrency);

    EXPECT_EQ(runtime.adaptive_grain(0), 1u);
    EXPECT_EQ(runtime.adaptive_grain(1), 1u);

    EXPECT_GE(
        runtime.adaptive_grain(1000),
        1u);

    EXPECT_LE(
        runtime.adaptive_grain(1000),
        1000u);

    EXPECT_GE(
        runtime.adaptive_grain(1000, 16),
        16u);

    EXPECT_EQ(
        runtime.adaptive_grain(1000, 2000),
        2000u);

    EXPECT_TRUE(runtime.arena().is_active());
    EXPECT_EQ(
        runtime.arena().max_concurrency(),
        requested_concurrency);

    // Configuration is process-lifetime. A second configuration must
    // be rejected without changing the original concurrency.
    EXPECT_FALSE(runtime.configure(requested_concurrency + 1));
    EXPECT_EQ(runtime.concurrency(), requested_concurrency);

    std::atomic<int> executed{0};

    runtime.arena().execute([&] {
        tbb::task_group group(runtime.context());

        constexpr int task_count = 64;

        for (int i = 0; i < task_count; ++i) {
            group.run([&executed] {
                executed.fetch_add(1, std::memory_order_relaxed);
            });
        }

        EXPECT_EQ(
            group.wait(),
            tbb::task_group_status::complete);
    });

    EXPECT_EQ(executed.load(std::memory_order_relaxed), 64);

    // No associated tasks remain active here, so resetting the root
    // cancellation context is safe.
    EXPECT_TRUE(runtime.cancel());
    EXPECT_TRUE(runtime.cancelled());

    // oneTBB returns false when cancellation was already requested.
    EXPECT_FALSE(runtime.cancel());

    runtime.reset_cancellation();

    EXPECT_FALSE(runtime.cancelled());

    runtime.reset_metrics();

    auto metrics = runtime.metrics();

    EXPECT_EQ(metrics.indexed_calls, 0u);
    EXPECT_EQ(metrics.indexed_items, 0u);
    EXPECT_EQ(metrics.indexed_ranges, 0u);
    EXPECT_EQ(metrics.indexed_elapsed_ns, 0u);
    EXPECT_EQ(metrics.foreach_calls, 0u);
    EXPECT_EQ(metrics.foreach_items, 0u);

    runtime.record_indexed_call(100, 8);
    runtime.record_indexed_range();
    runtime.record_indexed_range();
    runtime.record_indexed_elapsed(1000);

    runtime.record_foreach_call(25);
    runtime.record_foreach_elapsed(500);

    metrics = runtime.metrics();

    EXPECT_EQ(metrics.indexed_calls, 1u);
    EXPECT_EQ(metrics.indexed_items, 100u);
    EXPECT_EQ(metrics.indexed_ranges, 2u);
    EXPECT_EQ(metrics.indexed_elapsed_ns, 1000u);
    EXPECT_EQ(metrics.indexed_max_elapsed_ns, 1000u);
    EXPECT_EQ(metrics.last_indexed_grain, 8u);

    EXPECT_EQ(metrics.foreach_calls, 1u);
    EXPECT_EQ(metrics.foreach_items, 25u);
    EXPECT_EQ(metrics.foreach_elapsed_ns, 500u);
    EXPECT_EQ(metrics.foreach_max_elapsed_ns, 500u);

    runtime.record_indexed_elapsed(250);
    runtime.record_indexed_elapsed(2000);

    metrics = runtime.metrics();

    EXPECT_EQ(metrics.indexed_elapsed_ns, 3250u);
    EXPECT_EQ(metrics.indexed_max_elapsed_ns, 2000u);

    runtime.reset_metrics();

    metrics = runtime.metrics();

    EXPECT_EQ(metrics.indexed_calls, 0u);
    EXPECT_EQ(metrics.indexed_items, 0u);
    EXPECT_EQ(metrics.indexed_ranges, 0u);
    EXPECT_EQ(metrics.indexed_elapsed_ns, 0u);
    EXPECT_EQ(metrics.indexed_max_elapsed_ns, 0u);
    EXPECT_EQ(metrics.last_indexed_grain, 0u);
    EXPECT_EQ(metrics.foreach_calls, 0u);
    EXPECT_EQ(metrics.foreach_items, 0u);
    EXPECT_EQ(metrics.foreach_elapsed_ns, 0u);
    EXPECT_EQ(metrics.foreach_max_elapsed_ns, 0u);
}