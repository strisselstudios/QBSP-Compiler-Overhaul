/*
Copyright (C) 2026

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

See file, 'COPYING', for details.
*/

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <tbb/global_control.h>
#include <tbb/task_arena.h>
#include <tbb/task_group.h>

namespace scheduler
{

struct metrics_snapshot final
{
    std::uint64_t indexed_calls{0};
    std::uint64_t indexed_items{0};
    std::uint64_t indexed_ranges{0};
    std::uint64_t indexed_elapsed_ns{0};
    std::uint64_t indexed_max_elapsed_ns{0};

    std::uint64_t foreach_calls{0};
    std::uint64_t foreach_items{0};
    std::uint64_t foreach_elapsed_ns{0};
    std::uint64_t foreach_max_elapsed_ns{0};

    std::size_t last_indexed_grain{0};
};

class runtime final
{
public:
    static runtime &instance();

    [[nodiscard]] static int resolve_concurrency(int max_threads);

    runtime(const runtime &) = delete;
    runtime &operator=(const runtime &) = delete;
    runtime(runtime &&) = delete;
    runtime &operator=(runtime &&) = delete;

    bool configure(int max_threads);

    [[nodiscard]] bool configured() const noexcept;
    [[nodiscard]] int concurrency() const noexcept;

    [[nodiscard]] std::size_t adaptive_grain(
        std::size_t item_count,
        std::size_t minimum_grain = 1,
        std::size_t target_tasks_per_worker = 8) const noexcept;

    tbb::task_arena &arena();
    tbb::task_group_context &context();

    bool cancel();
    [[nodiscard]] bool cancelled() const;
    void reset_cancellation();

    void record_indexed_call(
        std::size_t item_count,
        std::size_t grain) noexcept;

    void record_indexed_range() noexcept;

    void record_indexed_elapsed(
        std::uint64_t elapsed_ns) noexcept;

    void record_foreach_call(
        std::size_t item_count) noexcept;

    void record_foreach_elapsed(
        std::uint64_t elapsed_ns) noexcept;

    [[nodiscard]] metrics_snapshot metrics() const noexcept;

    /**
     * Reset instrumentation counters.
     *
     * Call only when no instrumented scheduler work is active.
     */
    void reset_metrics() noexcept;

private:
    runtime() = default;
    ~runtime() = default;

    bool configured_{false};
    int concurrency_{0};

    std::unique_ptr<tbb::global_control> global_control_;
    std::unique_ptr<tbb::task_arena> arena_;
    std::unique_ptr<tbb::task_group_context> context_;

    std::atomic<std::uint64_t> indexed_calls_{0};
    std::atomic<std::uint64_t> indexed_items_{0};
    std::atomic<std::uint64_t> indexed_ranges_{0};
    std::atomic<std::uint64_t> indexed_elapsed_ns_{0};
    std::atomic<std::uint64_t> indexed_max_elapsed_ns_{0};

    std::atomic<std::uint64_t> foreach_calls_{0};
    std::atomic<std::uint64_t> foreach_items_{0};
    std::atomic<std::uint64_t> foreach_elapsed_ns_{0};
    std::atomic<std::uint64_t> foreach_max_elapsed_ns_{0};

    std::atomic<std::size_t> last_indexed_grain_{0};
};

} // namespace scheduler