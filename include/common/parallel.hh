/*  Copyright (C) 1996-1997  Id Software, Inc.
    Copyright (C) 2017 Eric Wasylishen

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

    See file, 'COPYING', for details.
*/
#pragma once

#include "common/log.hh"
#include "common/scheduler.hh"

#include <atomic>
#include <cstddef>
#include <type_traits>

#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_for_each.h>
#include <tbb/partitioner.h>

// Parallel extensions to logging.
namespace logging
{

template<typename TS, typename TE, typename Body>
void parallel_for(const TS &start, const TE &end, const Body &func)
{
    using index_type = std::common_type_t<TS, TE>;

    static_assert(
        std::is_integral_v<index_type>,
        "logging::parallel_for requires an integral index range");

    const index_type first = static_cast<index_type>(start);
    const index_type last = static_cast<index_type>(end);

    if (last <= first) {
        return;
    }

    const auto length =
        static_cast<std::size_t>(last - first);

    std::atomic<uint64_t> progress{0};

    auto &runtime = scheduler::runtime::instance();

    const std::size_t grain =
        runtime.adaptive_grain(length);

    runtime.arena().execute([&] {
        tbb::parallel_for(
            tbb::blocked_range<index_type>(
                first,
                last,
                static_cast<index_type>(grain)),
            [&](const tbb::blocked_range<index_type> &range) {
                for (index_type it = range.begin();
                     it != range.end();
                     ++it) {
                    percent(
                        progress.fetch_add(
                            1,
                            std::memory_order_relaxed),
                        length);

                    func(it);
                }
            },
            tbb::auto_partitioner{});
    });

    percent(
        progress.load(std::memory_order_relaxed),
        length);
}

template<typename Container, typename Body>
void parallel_for_each(Container &container, const Body &func)
{
    const auto length = std::size(container);
    std::atomic<uint64_t> progress{0};

    auto &runtime = scheduler::runtime::instance();

    runtime.arena().execute([&] {
        tbb::parallel_for_each(
            container,
            [&](auto &item) {
                percent(
                    progress.fetch_add(
                        1,
                        std::memory_order_relaxed),
                    length);

                func(item);
            });
    });

    percent(
        progress.load(std::memory_order_relaxed),
        length);
}

template<typename Container, typename Body>
void parallel_for_each(const Container &container, const Body &func)
{
    const auto length = std::size(container);
    std::atomic<uint64_t> progress{0};

    auto &runtime = scheduler::runtime::instance();

    runtime.arena().execute([&] {
        tbb::parallel_for_each(
            container,
            [&](const auto &item) {
                percent(
                    progress.fetch_add(
                        1,
                        std::memory_order_relaxed),
                    length);

                func(item);
            });
    });

    percent(
        progress.load(std::memory_order_relaxed),
        length);
}

} // namespace logging