#pragma once

#include "task.h"

#include <util/system/yassert.h>
#include <util/thread/lfqueue.h>

#include <coroutine>
#include <optional>

namespace NActors::NTask {

    class TMpmcCoroutineQueue final {
    public:
        TMpmcCoroutineQueue() = default;

        TMpmcCoroutineQueue(const TMpmcCoroutineQueue&) = delete;
        TMpmcCoroutineQueue& operator=(const TMpmcCoroutineQueue&) = delete;

        void Enqueue(std::coroutine_handle<> handle) {
            Y_ASSERT(handle);
            Queue_.Enqueue(handle);
        }

        template<class T>
        void Enqueue(task<T>&& t) {
            auto handle = std::coroutine_handle<>(t.ReleaseHandle());
            if (handle) {
                Enqueue(handle);
            }
        }

        std::optional<std::coroutine_handle<>> TryDequeue() {
            std::coroutine_handle<> handle;
            if (Queue_.Dequeue(&handle)) {
                return handle;
            }
            return std::nullopt;
        }

        bool TryRunOne() {
            auto handle = TryDequeue();
            if (!handle) {
                return false;
            }

            handle->resume();
            if (handle->done()) {
                handle->destroy();
            }
            return true;
        }

    private:
        TLockFreeQueue<std::coroutine_handle<>, TDefaultLFCounter> Queue_;
    };

} // namespace NActors::NTask

