#include <common/scheduler.hh>

#include <algorithm>
#include <limits>
#include <stdexcept>

#include <tbb/info.h>
#include <tbb/task_scheduler_observer.h>

namespace
{

template<typename T>
void update_max(
    std::atomic<T> &target,
    const T value) noexcept
{
    auto current = target.load(std::memory_order_relaxed);

    while (current < value &&
           !target.compare_exchange_weak(
               current,
               value,
               std::memory_order_relaxed,
               std::memory_order_relaxed)) {
    }
}

} // namespace

namespace scheduler
{

class runtime::arena_observer final
    : public tbb::task_scheduler_observer
{
public:
    arena_observer(
        runtime &owner,
        tbb::task_arena &arena)
        : tbb::task_scheduler_observer(arena),
          owner_(owner)
    {
    }

    void on_scheduler_entry(
        const bool is_worker) override
    {
        owner_.arena_entries_.fetch_add(
            1,
            std::memory_order_relaxed);

        if (is_worker) {
            owner_.worker_entries_.fetch_add(
                1,
                std::memory_order_relaxed);
        }

        const std::size_t active =
            owner_.active_arena_threads_.fetch_add(
                1,
                std::memory_order_relaxed) + 1;

        update_max(
            owner_.peak_active_arena_threads_,
            active);
    }

    void on_scheduler_exit(
        const bool is_worker) override
    {
        owner_.arena_exits_.fetch_add(
            1,
            std::memory_order_relaxed);

        if (is_worker) {
            owner_.worker_exits_.fetch_add(
                1,
                std::memory_order_relaxed);
        }

        owner_.active_arena_threads_.fetch_sub(
            1,
            std::memory_order_relaxed);
    }

private:
    runtime &owner_;
};

runtime &runtime::instance()
{
    static runtime scheduler_runtime;
    return scheduler_runtime;
}

runtime::~runtime()
{
    if (observer_) {
        observer_->observe(false);
    }
}

int runtime::resolve_concurrency(const int max_threads)
{
    const int resolved =
        max_threads > 0
            ? max_threads
            : tbb::info::default_concurrency();

    if (resolved <= 0) {
        throw std::runtime_error(
            "oneTBB reported invalid default concurrency");
    }

    return resolved;
}

bool runtime::configure(const int max_threads)
{
    if (configured_) {
        return false;
    }

    const int requested_concurrency =
        resolve_concurrency(max_threads);

    if (max_threads > 0) {
        global_control_ =
            std::make_unique<tbb::global_control>(
                tbb::global_control::max_allowed_parallelism,
                static_cast<std::size_t>(max_threads));
    }

    arena_ = std::make_unique<tbb::task_arena>(
        requested_concurrency,
        1,
        tbb::task_arena::priority::normal);

    arena_->initialize();

    context_ =
        std::make_unique<tbb::task_group_context>(
            tbb::task_group_context::isolated);

    observer_ =
        std::make_unique<arena_observer>(
            *this,
            *arena_);

    observer_->observe(true);

    concurrency_ = arena_->max_concurrency();
    configured_ = true;

    return true;
}

bool runtime::configured() const noexcept
{
    return configured_;
}

int runtime::concurrency() const noexcept
{
    return concurrency_;
}

std::size_t runtime::adaptive_grain(
    const std::size_t item_count,
    const std::size_t minimum_grain,
    const std::size_t target_tasks_per_worker) const noexcept
{
    const std::size_t effective_minimum =
        std::max<std::size_t>(1, minimum_grain);

    if (item_count <= effective_minimum) {
        return effective_minimum;
    }

    const std::size_t workers =
        concurrency_ > 0
            ? static_cast<std::size_t>(concurrency_)
            : 1;

    const std::size_t tasks_per_worker =
        std::max<std::size_t>(
            1,
            target_tasks_per_worker);

    std::size_t target_tasks;

    if (workers > item_count / tasks_per_worker) {
        target_tasks = item_count;
    } else {
        target_tasks =
            workers * tasks_per_worker;
    }

    target_tasks =
        std::max<std::size_t>(1, target_tasks);

    const std::size_t calculated_grain =
        ((item_count - 1) / target_tasks) + 1;

    return std::max(
        effective_minimum,
        calculated_grain);
}

std::uint64_t runtime::estimate_cost(
    const std::uint64_t item_count,
    const std::uint64_t cost_per_item) noexcept
{
    if (item_count == 0 || cost_per_item == 0) {
        return 0;
    }

    constexpr auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    if (item_count > maximum / cost_per_item) {
        return maximum;
    }

    return item_count * cost_per_item;
}

bool runtime::should_parallelize(
    const std::uint64_t estimated_cost,
    const std::uint64_t parallel_threshold) noexcept
{
    parallel_decisions_.fetch_add(
        1,
        std::memory_order_relaxed);

    last_estimated_cost_.store(
        estimated_cost,
        std::memory_order_relaxed);

    last_parallel_threshold_.store(
        parallel_threshold,
        std::memory_order_relaxed);

    const bool accepted =
        configured_ &&
        concurrency_ > 1 &&
        parallel_threshold > 0 &&
        estimated_cost >= parallel_threshold;

    if (accepted) {
        parallel_accepted_.fetch_add(
            1,
            std::memory_order_relaxed);
    } else {
        parallel_rejected_.fetch_add(
            1,
            std::memory_order_relaxed);
    }

    return accepted;
}

tbb::task_arena &runtime::arena()
{
    if (!arena_) {
        throw std::logic_error(
            "scheduler runtime has not been configured");
    }

    return *arena_;
}

tbb::task_group_context &runtime::context()
{
    if (!context_) {
        throw std::logic_error(
            "scheduler runtime has not been configured");
    }

    return *context_;
}

bool runtime::cancel()
{
    return context().cancel_group_execution();
}

bool runtime::cancelled() const
{
    if (!context_) {
        return false;
    }

    return context_->is_group_execution_cancelled();
}

void runtime::reset_cancellation()
{
    context().reset();
}

void runtime::record_indexed_call(
    const std::size_t item_count,
    const std::size_t grain) noexcept
{
    indexed_calls_.fetch_add(
        1,
        std::memory_order_relaxed);

    indexed_items_.fetch_add(
        static_cast<std::uint64_t>(item_count),
        std::memory_order_relaxed);

    last_indexed_grain_.store(
        grain,
        std::memory_order_relaxed);
}

void runtime::record_indexed_range() noexcept
{
    indexed_ranges_.fetch_add(
        1,
        std::memory_order_relaxed);
}

void runtime::record_indexed_elapsed(
    const std::uint64_t elapsed_ns) noexcept
{
    indexed_elapsed_ns_.fetch_add(
        elapsed_ns,
        std::memory_order_relaxed);

    update_max(
        indexed_max_elapsed_ns_,
        elapsed_ns);
}

void runtime::record_foreach_call(
    const std::size_t item_count) noexcept
{
    foreach_calls_.fetch_add(
        1,
        std::memory_order_relaxed);

    foreach_items_.fetch_add(
        static_cast<std::uint64_t>(item_count),
        std::memory_order_relaxed);
}

void runtime::record_foreach_elapsed(
    const std::uint64_t elapsed_ns) noexcept
{
    foreach_elapsed_ns_.fetch_add(
        elapsed_ns,
        std::memory_order_relaxed);

    update_max(
        foreach_max_elapsed_ns_,
        elapsed_ns);
}

metrics_snapshot runtime::metrics() const noexcept
{
    metrics_snapshot result;

    result.indexed_calls =
        indexed_calls_.load(std::memory_order_relaxed);

    result.indexed_items =
        indexed_items_.load(std::memory_order_relaxed);

    result.indexed_ranges =
        indexed_ranges_.load(std::memory_order_relaxed);

    result.indexed_elapsed_ns =
        indexed_elapsed_ns_.load(std::memory_order_relaxed);

    result.indexed_max_elapsed_ns =
        indexed_max_elapsed_ns_.load(
            std::memory_order_relaxed);

    result.foreach_calls =
        foreach_calls_.load(std::memory_order_relaxed);

    result.foreach_items =
        foreach_items_.load(std::memory_order_relaxed);

    result.foreach_elapsed_ns =
        foreach_elapsed_ns_.load(std::memory_order_relaxed);

    result.foreach_max_elapsed_ns =
        foreach_max_elapsed_ns_.load(
            std::memory_order_relaxed);

    result.last_indexed_grain =
        last_indexed_grain_.load(
            std::memory_order_relaxed);

    result.arena_entries =
        arena_entries_.load(std::memory_order_relaxed);

    result.arena_exits =
        arena_exits_.load(std::memory_order_relaxed);

    result.worker_entries =
        worker_entries_.load(std::memory_order_relaxed);

    result.worker_exits =
        worker_exits_.load(std::memory_order_relaxed);

    result.active_arena_threads =
        active_arena_threads_.load(
            std::memory_order_relaxed);

    result.peak_active_arena_threads =
        peak_active_arena_threads_.load(
            std::memory_order_relaxed);

    result.parallel_decisions =
        parallel_decisions_.load(
            std::memory_order_relaxed);

    result.parallel_accepted =
        parallel_accepted_.load(
            std::memory_order_relaxed);

    result.parallel_rejected =
        parallel_rejected_.load(
            std::memory_order_relaxed);

    result.last_estimated_cost =
        last_estimated_cost_.load(
            std::memory_order_relaxed);

    result.last_parallel_threshold =
        last_parallel_threshold_.load(
            std::memory_order_relaxed);

    return result;
}

void runtime::reset_metrics() noexcept
{
    indexed_calls_.store(0, std::memory_order_relaxed);
    indexed_items_.store(0, std::memory_order_relaxed);
    indexed_ranges_.store(0, std::memory_order_relaxed);
    indexed_elapsed_ns_.store(0, std::memory_order_relaxed);
    indexed_max_elapsed_ns_.store(
        0,
        std::memory_order_relaxed);

    foreach_calls_.store(0, std::memory_order_relaxed);
    foreach_items_.store(0, std::memory_order_relaxed);
    foreach_elapsed_ns_.store(0, std::memory_order_relaxed);
    foreach_max_elapsed_ns_.store(
        0,
        std::memory_order_relaxed);

    last_indexed_grain_.store(
        0,
        std::memory_order_relaxed);

    arena_entries_.store(0, std::memory_order_relaxed);
    arena_exits_.store(0, std::memory_order_relaxed);
    worker_entries_.store(0, std::memory_order_relaxed);
    worker_exits_.store(0, std::memory_order_relaxed);

    const std::size_t active =
        active_arena_threads_.load(
            std::memory_order_relaxed);

    peak_active_arena_threads_.store(
        active,
        std::memory_order_relaxed);

    parallel_decisions_.store(
        0,
        std::memory_order_relaxed);

    parallel_accepted_.store(
        0,
        std::memory_order_relaxed);

    parallel_rejected_.store(
        0,
        std::memory_order_relaxed);

    last_estimated_cost_.store(
        0,
        std::memory_order_relaxed);

    last_parallel_threshold_.store(
        0,
        std::memory_order_relaxed);
}

} // namespace scheduler