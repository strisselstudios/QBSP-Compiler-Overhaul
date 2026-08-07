#include <common/scheduler.hh>

#include <algorithm>
#include <stdexcept>

#include <tbb/info.h>

namespace scheduler
{

runtime &runtime::instance()
{
    static runtime scheduler_runtime;
    return scheduler_runtime;
}

int runtime::resolve_concurrency(const int max_threads)
{
    const int resolved =
        max_threads > 0 ? max_threads : tbb::info::default_concurrency();

    if (resolved <= 0) {
        throw std::runtime_error("oneTBB reported invalid default concurrency");
    }

    return resolved;
}

bool runtime::configure(const int max_threads)
{
    if (configured_) {
        return false;
    }

    const int requested_concurrency = resolve_concurrency(max_threads);

    if (max_threads > 0) {
        global_control_ = std::make_unique<tbb::global_control>(
            tbb::global_control::max_allowed_parallelism,
            static_cast<std::size_t>(max_threads));
    }

    arena_ = std::make_unique<tbb::task_arena>(
        requested_concurrency,
        1,
        tbb::task_arena::priority::normal);

    arena_->initialize();

    context_ = std::make_unique<tbb::task_group_context>(
        tbb::task_group_context::isolated);

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
        std::max<std::size_t>(1, target_tasks_per_worker);

    std::size_t target_tasks;

    if (workers > item_count / tasks_per_worker) {
        target_tasks = item_count;
    } else {
        target_tasks = workers * tasks_per_worker;
    }

    target_tasks = std::max<std::size_t>(1, target_tasks);

    const std::size_t calculated_grain =
        ((item_count - 1) / target_tasks) + 1;

    return std::max(effective_minimum, calculated_grain);
}

tbb::task_arena &runtime::arena()
{
    if (!arena_) {
        throw std::logic_error("scheduler runtime has not been configured");
    }

    return *arena_;
}

tbb::task_group_context &runtime::context()
{
    if (!context_) {
        throw std::logic_error("scheduler runtime has not been configured");
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

} // namespace scheduler