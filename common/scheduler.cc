#include <common/scheduler.hh>

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