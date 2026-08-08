#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include <common/log.hh>

namespace qbsp_profile
{

using clock = std::chrono::steady_clock;

inline bool enabled() noexcept
{
    static const bool result = [] {
        const char *value =
            std::getenv("QBSP_SCHEDULER_PROFILE");

        if (!value || value[0] == '\0') {
            return false;
        }

        if (value[0] == '0' &&
            value[1] == '\0') {
            return false;
        }

        return true;
    }();

    return result;
}

inline std::uint64_t elapsed_ns(
    const clock::time_point started) noexcept
{
    const auto elapsed =
        std::chrono::duration_cast<
            std::chrono::nanoseconds>(
                clock::now() - started);

    return elapsed.count() > 0
        ? static_cast<std::uint64_t>(
              elapsed.count())
        : 0;
}

template<typename T>
inline void atomic_max(
    std::atomic<T> &target,
    const T value) noexcept
{
    T current =
        target.load(std::memory_order_relaxed);

    while (current < value &&
           !target.compare_exchange_weak(
               current,
               value,
               std::memory_order_relaxed,
               std::memory_order_relaxed)) {
    }
}

class recursion_metrics final
{
public:
    static constexpr std::size_t bucket_count = 6;

    void record_entry(
        const std::size_t depth,
        const std::size_t brushes,
        const std::size_t sides) noexcept
    {
        calls_.fetch_add(
            1,
            std::memory_order_relaxed);

        input_brushes_.fetch_add(
            brushes,
            std::memory_order_relaxed);

        input_sides_.fetch_add(
            sides,
            std::memory_order_relaxed);

        atomic_max(max_depth_, depth);
        atomic_max(max_input_brushes_, brushes);
        atomic_max(max_input_sides_, sides);

        bucket_calls_[bucket_for(brushes)].fetch_add(
            1,
            std::memory_order_relaxed);
    }

    void record_leaf() noexcept
    {
        leaves_.fetch_add(
            1,
            std::memory_order_relaxed);
    }

    void record_split(
        const std::size_t front_brushes,
        const std::size_t back_brushes) noexcept
    {
        splits_.fetch_add(
            1,
            std::memory_order_relaxed);

        atomic_max(
            max_front_brushes_,
            front_brushes);

        atomic_max(
            max_back_brushes_,
            back_brushes);
    }

    void record_local(
        const std::size_t brushes,
        const std::uint64_t nanoseconds) noexcept
    {
        total_local_ns_.fetch_add(
            nanoseconds,
            std::memory_order_relaxed);

        atomic_max(
            max_local_ns_,
            nanoseconds);

        bucket_local_ns_[bucket_for(brushes)].fetch_add(
            nanoseconds,
            std::memory_order_relaxed);
    }

    void record_subtree(
        const std::size_t brushes,
        const std::uint64_t nanoseconds) noexcept
    {
        total_subtree_ns_.fetch_add(
            nanoseconds,
            std::memory_order_relaxed);

        atomic_max(
            max_subtree_ns_,
            nanoseconds);

        bucket_subtree_ns_[bucket_for(brushes)].fetch_add(
            nanoseconds,
            std::memory_order_relaxed);
    }

    void print() const
    {
        logging::print(
            "\nQBSP scheduler recursion profile\n");

        logging::print(
            "  calls: {}\n"
            "  split nodes: {}\n"
            "  leaf nodes: {}\n"
            "  max depth: {}\n"
            "  max input brushes: {}\n"
            "  max input sides: {}\n"
            "  max front child brushes: {}\n"
            "  max back child brushes: {}\n",
            calls_.load(std::memory_order_relaxed),
            splits_.load(std::memory_order_relaxed),
            leaves_.load(std::memory_order_relaxed),
            max_depth_.load(std::memory_order_relaxed),
            max_input_brushes_.load(std::memory_order_relaxed),
            max_input_sides_.load(std::memory_order_relaxed),
            max_front_brushes_.load(std::memory_order_relaxed),
            max_back_brushes_.load(std::memory_order_relaxed));

        logging::print(
            "  total local work: {} us\n"
            "  max local node: {} us\n"
            "  accumulated subtree time: {} us\n"
            "  max subtree: {} us\n",
            total_local_ns_.load(
                std::memory_order_relaxed) / 1000,
            max_local_ns_.load(
                std::memory_order_relaxed) / 1000,
            total_subtree_ns_.load(
                std::memory_order_relaxed) / 1000,
            max_subtree_ns_.load(
                std::memory_order_relaxed) / 1000);

        static constexpr std::array<
            const char *,
            bucket_count> labels{
                "0-1",
                "2-4",
                "5-16",
                "17-64",
                "65-256",
                "257+"
            };

        logging::print(
            "  brush buckets:\n");

        for (std::size_t i = 0;
             i < bucket_count;
             ++i) {
            const auto calls =
                bucket_calls_[i].load(
                    std::memory_order_relaxed);

            const auto local =
                bucket_local_ns_[i].load(
                    std::memory_order_relaxed);

            const auto subtree =
                bucket_subtree_ns_[i].load(
                    std::memory_order_relaxed);

            const auto avg_local_us =
                calls > 0
                    ? (local / calls) / 1000
                    : 0;

            const auto avg_subtree_us =
                calls > 0
                    ? (subtree / calls) / 1000
                    : 0;

            logging::print(
                "    {:>6}: calls {}, "
                "avg local {} us, "
                "avg subtree {} us\n",
                labels[i],
                calls,
                avg_local_us,
                avg_subtree_us);
        }
    }

private:
    static constexpr std::size_t bucket_for(
        const std::size_t brushes) noexcept
    {
        if (brushes <= 1) {
            return 0;
        }

        if (brushes <= 4) {
            return 1;
        }

        if (brushes <= 16) {
            return 2;
        }

        if (brushes <= 64) {
            return 3;
        }

        if (brushes <= 256) {
            return 4;
        }

        return 5;
    }

    std::atomic<std::uint64_t> calls_{0};
    std::atomic<std::uint64_t> splits_{0};
    std::atomic<std::uint64_t> leaves_{0};

    std::atomic<std::size_t> input_brushes_{0};
    std::atomic<std::size_t> input_sides_{0};

    std::atomic<std::size_t> max_depth_{0};
    std::atomic<std::size_t> max_input_brushes_{0};
    std::atomic<std::size_t> max_input_sides_{0};
    std::atomic<std::size_t> max_front_brushes_{0};
    std::atomic<std::size_t> max_back_brushes_{0};

    std::atomic<std::uint64_t> total_local_ns_{0};
    std::atomic<std::uint64_t> max_local_ns_{0};

    std::atomic<std::uint64_t> total_subtree_ns_{0};
    std::atomic<std::uint64_t> max_subtree_ns_{0};

    std::array<
        std::atomic<std::uint64_t>,
        bucket_count> bucket_calls_{};

    std::array<
        std::atomic<std::uint64_t>,
        bucket_count> bucket_local_ns_{};

    std::array<
        std::atomic<std::uint64_t>,
        bucket_count> bucket_subtree_ns_{};
};

} // namespace qbsp_profile