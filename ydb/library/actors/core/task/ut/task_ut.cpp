#include <ydb/library/actors/core/task/task.h>
#include <ydb/library/actors/core/task/when_all.h>

#include <library/cpp/testing/unittest/registar.h>

#include <optional>

namespace {

    using NActors::NTask::task;

    template<class T>
    class TManualPromise {
    public:
        class TAwaiter {
        public:
            explicit TAwaiter(TManualPromise& self) noexcept
                : Self(self)
            {}

            bool await_ready() const noexcept {
                return Self.Ready;
            }

            void await_suspend(std::coroutine_handle<> h) noexcept {
                Self.Continuation = h;
            }

            T await_resume() {
                UNIT_ASSERT(Self.Value.has_value());
                return std::move(*Self.Value);
            }

        private:
            TManualPromise& Self;
        };

        TAwaiter operator co_await() noexcept {
            return TAwaiter(*this);
        }

        std::coroutine_handle<> SetValue(T value) {
            UNIT_ASSERT(!Ready);
            Value.emplace(std::move(value));
            Ready = true;
            return std::exchange(Continuation, {});
        }

    private:
        bool Ready = false;
        std::optional<T> Value;
        std::coroutine_handle<> Continuation;
    };

    task<int> Return42() {
        co_return 42;
    }

    task<int> AddOne() {
        int x = co_await Return42();
        co_return x + 1;
    }

    task<void> ThrowingTask() {
        throw std::runtime_error("boom");
        co_return;
    }

    task<int> AwaitManualPromise(TManualPromise<int>& p) {
        int x = co_await p;
        co_return x + 1;
    }

    task<void> AwaitManualPromiseAndStore(TManualPromise<int>& p, int& out) {
        out = co_await p;
        co_return;
    }

    task<void> AwaitTwoAndStore(TManualPromise<int>& p1, TManualPromise<int>& p2, int& out1, int& out2) {
        co_await NActors::NTask::WhenAll(
            AwaitManualPromiseAndStore(p1, out1),
            AwaitManualPromiseAndStore(p2, out2));
        co_return;
    }

    task<void> AwaitThreeAndStore(TManualPromise<int>& p1, TManualPromise<int>& p2, TManualPromise<int>& p3, int& out1, int& out2, int& out3) {
        co_await NActors::NTask::WhenAll(
            AwaitManualPromiseAndStore(p1, out1),
            AwaitManualPromiseAndStore(p2, out2),
            AwaitManualPromiseAndStore(p3, out3));
        co_return;
    }

} // namespace

Y_UNIT_TEST_SUITE(Task) {

    Y_UNIT_TEST(ComposesAndReturnsValue) {
        task<int> t = AddOne();
        UNIT_ASSERT(!t.done());
        t.resume();
        UNIT_ASSERT(t.done());
        UNIT_ASSERT_VALUES_EQUAL(t.ExtractValue(), 43);
    }

    Y_UNIT_TEST(PropagatesException) {
        task<void> t = ThrowingTask();
        t.resume();
        UNIT_ASSERT(t.done());
        UNIT_ASSERT_EXCEPTION_CONTAINS(t.ExtractValue(), std::runtime_error, "boom");
    }

    Y_UNIT_TEST(ManualPromiseSuspendsAndResumes) {
        TManualPromise<int> p;
        task<int> t = AwaitManualPromise(p);

        UNIT_ASSERT(!t.done());
        t.resume();
        UNIT_ASSERT(!t.done());

        std::coroutine_handle<> h = p.SetValue(42);
        UNIT_ASSERT(h);
        h.resume();
        UNIT_ASSERT(t.done());
        UNIT_ASSERT_VALUES_EQUAL(t.ExtractValue(), 43);
    }

    Y_UNIT_TEST(WhenAllTwoManualPromises) {
        TManualPromise<int> p1;
        TManualPromise<int> p2;
        int out1 = 0;
        int out2 = 0;

        task<void> t = AwaitTwoAndStore(p1, p2, out1, out2);
        t.resume(); // starts, waits on both promises
        UNIT_ASSERT(!t.done());

        std::coroutine_handle<> h1 = p1.SetValue(10);
        std::coroutine_handle<> h2 = p2.SetValue(20);
        UNIT_ASSERT(h1);
        UNIT_ASSERT(h2);

        h1.resume();
        UNIT_ASSERT(!t.done()); // waiting for the second task

        h2.resume();
        UNIT_ASSERT(t.done());
        UNIT_ASSERT_VALUES_EQUAL(out1, 10);
        UNIT_ASSERT_VALUES_EQUAL(out2, 20);
    }

    Y_UNIT_TEST(WhenAllThreeManualPromises) {
        TManualPromise<int> p1;
        TManualPromise<int> p2;
        TManualPromise<int> p3;
        int out1 = 0;
        int out2 = 0;
        int out3 = 0;

        task<void> t = AwaitThreeAndStore(p1, p2, p3, out1, out2, out3);
        t.resume(); // starts, waits on all promises
        UNIT_ASSERT(!t.done());

        std::coroutine_handle<> h2 = p2.SetValue(20);
        UNIT_ASSERT(h2);
        h2.resume();
        UNIT_ASSERT(!t.done());

        std::coroutine_handle<> h1 = p1.SetValue(10);
        UNIT_ASSERT(h1);
        h1.resume();
        UNIT_ASSERT(!t.done());

        std::coroutine_handle<> h3 = p3.SetValue(30);
        UNIT_ASSERT(h3);
        h3.resume();
        UNIT_ASSERT(t.done());

        UNIT_ASSERT_VALUES_EQUAL(out1, 10);
        UNIT_ASSERT_VALUES_EQUAL(out2, 20);
        UNIT_ASSERT_VALUES_EQUAL(out3, 30);
    }

}
