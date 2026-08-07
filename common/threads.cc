#include <common/threads.hh>

#include <common/log.hh>
#include <common/scheduler.hh>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

void configureTBB(const int maxthreads, const bool lowPriority)
{
    auto &scheduler_runtime = scheduler::runtime::instance();

    if (!scheduler_runtime.configure(maxthreads)) {
        logging::print("ignoring multiple configureTBB calls\n");
        return;
    }

    if (maxthreads > 0) {
        logging::print("running with {} thread(s)\n", maxthreads);
    }

    if (lowPriority) {
#ifdef _WIN32
        SetPriorityClass(GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS);
        logging::print("running with lower priority\n");
#else
        logging::print("low priority not compiled into this version\n");
#endif
    }
}