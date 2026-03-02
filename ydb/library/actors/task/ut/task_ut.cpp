#include <ydb/library/actors/task/task.h>
#include <ydb/library/actors/task/service_map_subsystem.h>
#include <ydb/library/actors/task/task_system.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <optional>
#include <util/generic/string.h>

namespace {

    using NActors::NTask::task;
    using TActorIntMapSubSystem = NActors::NTask::TServiceMapSubSystem<NActors::TActorId, int, NActors::TActorId::THash>;

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

    task<void> AwaitManualPromiseAndStorePlusOne(TManualPromise<int>& p, std::optional<int>& out) {
        out = (co_await p) + 1;
        co_return;
    }

    task<void> SetValue42(int& out) {
        out = 42;
        co_return;
    }

    class TPingResponderActor final : public NActors::TActor<TPingResponderActor> {
    public:
        TPingResponderActor()
            : TActor(&TThis::StateWork)
        {
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NActors::TEvents::TEvPing, HandlePing);
            }
        }

    private:
        void HandlePing(NActors::TEvents::TEvPing::TPtr& ev) {
            Send(ev->Sender, new NActors::TEvents::TEvWakeup, 0, ev->Cookie);
        }
    };

    task<void> WaitWakeupInTaskExecutor(const NActors::TActorId& responder, int& out) {
        const ui64 cookie = NActors::NTask::TTaskSystem::GetUniqueCookieForMessage();
        UNIT_ASSERT(cookie != 0);

        const auto& ctx = NActors::TActivationContext::AsActorContext();
        ctx.Send(responder, new NActors::TEvents::TEvPing, 0, cookie);

        auto ev = co_await NActors::NTask::WaitEvent<NActors::TEvents::TEvWakeup>(cookie);
        UNIT_ASSERT(ev);

        out = 42;
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

    Y_UNIT_TEST(TaskExecutorActorRunsQueuedTask) {
        auto runtime = MakeHolder<NActors::TTestActorRuntimeBase>();
        runtime->SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
        runtime->Initialize();

        NActors::NTask::TTaskSystem sys;
        sys.Initialize(runtime->GetAnyNodeActorSystem(), 1);

        int out = 0;
        sys.Enqueue(SetValue42(out));
        UNIT_ASSERT_VALUES_EQUAL(out, 0); // must not run inline

        runtime->DispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(out, 42);
    }

    Y_UNIT_TEST(TaskExecutorActorResumesContinuationFromManualPromise) {
        auto runtime = MakeHolder<NActors::TTestActorRuntimeBase>();
        runtime->SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
        runtime->Initialize();

        NActors::NTask::TTaskSystem sys;
        sys.Initialize(runtime->GetAnyNodeActorSystem(), 1);

        TManualPromise<int> p;
        std::optional<int> out;
        sys.Enqueue(AwaitManualPromiseAndStorePlusOne(p, out));

        runtime->DispatchEvents();
        UNIT_ASSERT(!out); // suspended in co_await(p)

        std::coroutine_handle<> h = p.SetValue(42);
        UNIT_ASSERT(h);
        sys.Enqueue(h);

        runtime->DispatchEvents();
        UNIT_ASSERT(out);
        UNIT_ASSERT_VALUES_EQUAL(*out, 43);
    }

    Y_UNIT_TEST(TaskWaitEventInExecutorActor) {
        auto runtime = MakeHolder<NActors::TTestActorRuntimeBase>();
        runtime->SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
        runtime->Initialize();

        NActors::NTask::TTaskSystem sys;
        sys.Initialize(runtime->GetAnyNodeActorSystem(), 1);

        const auto responder = runtime->Register(new TPingResponderActor());

        int out = 0;
        sys.Enqueue(WaitWakeupInTaskExecutor(responder, out));
        UNIT_ASSERT_VALUES_EQUAL(out, 0);

        runtime->DispatchEvents();
        UNIT_ASSERT_VALUES_EQUAL(out, 42);
    }

    Y_UNIT_TEST(ServiceMapSubSystemStoresByTemplateKeyValue) {
        NActors::NTask::TServiceMapSubSystem<int, TString> subSystem;

        UNIT_ASSERT_VALUES_EQUAL(subSystem.Find(100), TString());
        UNIT_ASSERT_VALUES_EQUAL(subSystem.Update(100, TString("v1")), TString());
        UNIT_ASSERT_VALUES_EQUAL(subSystem.Find(100), TString("v1"));
        UNIT_ASSERT_VALUES_EQUAL(subSystem.Update(100, TString("v2")), TString("v1"));
        UNIT_ASSERT(subSystem.Erase(100));
        UNIT_ASSERT_VALUES_EQUAL(subSystem.Find(100), TString());
    }

    Y_UNIT_TEST(ServiceMapSubSystemWorksAsActorSubSystem) {
        class TRuntimeWithServiceMapSubSystem final : public NActors::TTestActorRuntimeBase {
        protected:
            void InitActorSystem(NActors::TActorSystem& actorSystem, TNodeDataBase*) override {
                actorSystem.RegisterSubSystem(std::make_unique<TActorIntMapSubSystem>());
            }
        };

        auto runtime = MakeHolder<TRuntimeWithServiceMapSubSystem>();
        runtime->Initialize();

        auto* actorSystem = runtime->GetAnyNodeActorSystem();
        UNIT_ASSERT(actorSystem);

        auto* subSystem = actorSystem->GetSubSystem<TActorIntMapSubSystem>();
        UNIT_ASSERT(subSystem);

        const NActors::TActorId actorId(1, "shstate01");
        UNIT_ASSERT_VALUES_EQUAL(subSystem->Find(actorId), 0);
        UNIT_ASSERT_VALUES_EQUAL(subSystem->Update(actorId, 42), 0);
        UNIT_ASSERT_VALUES_EQUAL(subSystem->Find(actorId), 42);
        UNIT_ASSERT(subSystem->Erase(actorId));
        UNIT_ASSERT_VALUES_EQUAL(subSystem->Find(actorId), 0);
    }

}
