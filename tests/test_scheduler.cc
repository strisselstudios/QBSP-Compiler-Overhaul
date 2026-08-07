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
}