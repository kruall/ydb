#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/task/task.h>
#include <ydb/library/actors/core/task/task_system.h>
#include <ydb/library/actors/util/threadparkpad.h>

#include <util/generic/vector.h>
#include <util/system/hp_timer.h>

#include <atomic>
#include <memory>

namespace NActors::NTests {

struct TTaskBenchmarkSettings {
    static constexpr ui32 DefaultSpinThreshold = 1'000'000;
    static constexpr ui32 DefaultPingPongDurationSeconds = 1;
};

template <typename TSettings_ = TTaskBenchmarkSettings>
struct TTaskBenchmark {
    using TSettings = TSettings_;

    struct TPingPongResult {
        double ElapsedSeconds = 0.0;
        double ElapsedNsPerHop = 0.0;
        ui64 TotalHops = 0;
        ui64 HopsPerSecond = 0;
    };

    struct TCallCounter {
        std::atomic<ui64> Value = 0;

        void Increment() noexcept {
            Value.fetch_add(1, std::memory_order_relaxed);
        }

        ui64 Get() const noexcept {
            return Value.load(std::memory_order_relaxed);
        }
    };

    struct TPairState {
        alignas(128) TCallCounter Left;
        alignas(128) TCallCounter Right;
        std::atomic<bool> Done = false;
    };

    static void AddBasicPool(THolder<TActorSystemSetup>& setup, ui32 threads) {
        TBasicExecutorPoolConfig basic;
        basic.PoolId = setup->GetExecutorsCount();
        basic.PoolName = TStringBuilder() << "b" << basic.PoolId;
        basic.Threads = threads;
        basic.MaxThreadCount = threads;
        basic.SpinThreshold = TSettings::DefaultSpinThreshold;
        basic.HasSharedThread = false;
        setup->CpuManager.Basic.emplace_back(std::move(basic));
    }

    static THolder<TActorSystemSetup> InitActorSystemSetup(ui32 threads) {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;
        setup->Scheduler = new TBasicSchedulerThread(NActors::TSchedulerConfig(512, 0));
        AddBasicPool(setup, threads);
        return setup;
    }

    static NTask::task<void> IncrementViaAwait(TCallCounter& counter) {
        counter.Increment();
        co_return;
    }

    static NTask::task<void> RunPingPongStep(
        TCallCounter& self,
        TCallCounter& peer,
        std::atomic<bool>& stop,
        std::atomic<bool>& pairDone,
        std::atomic<ui32>& finishedPairs,
        ui32 totalPairs,
        TThreadParkPad& donePad,
        NTask::TTaskSystem& taskSystem)
    {
        co_await IncrementViaAwait(peer);
        self.Increment();

        if (stop.load(std::memory_order_acquire)) {
            if (!pairDone.exchange(true, std::memory_order_acq_rel)) {
                if (finishedPairs.fetch_add(1, std::memory_order_acq_rel) + 1 == totalPairs) {
                    donePad.Unpark();
                }
            }
            co_return;
        }

        taskSystem.Enqueue(RunPingPongStep(peer, self, stop, pairDone, finishedPairs, totalPairs, donePad, taskSystem));
    }

    static TPingPongResult BenchPingPong(
        ui32 threads,
        ui32 actorPairs,
        TDuration duration = TDuration::Seconds(TSettings::DefaultPingPongDurationSeconds))
    {
        auto setup = InitActorSystemSetup(threads);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(&actorSystem, threads);

        TVector<std::unique_ptr<TPairState>> pairs;
        pairs.reserve(actorPairs);
        for (ui32 i = 0; i < actorPairs; ++i) {
            pairs.push_back(std::make_unique<TPairState>());
        }
        std::atomic<bool> stop = false;
        std::atomic<ui32> finishedPairs = 0;
        TThreadParkPad donePad;

        for (ui32 i = 0; i < actorPairs; ++i) {
            taskSystem.Enqueue(RunPingPongStep(
                pairs[i]->Left,
                pairs[i]->Right,
                stop,
                pairs[i]->Done,
                finishedPairs,
                actorPairs,
                donePad,
                taskSystem));
        }

        THPTimer timer;
        timer.Reset();

        Sleep(duration);
        stop.store(true, std::memory_order_release);
        donePad.Park();

        const double elapsedSeconds = timer.Passed();
        actorSystem.Stop();

        ui64 hops = 0;
        for (const auto& pair : pairs) {
            hops += pair->Left.Get();
            hops += pair->Right.Get();
        }
        const double elapsedNsPerHop = hops ? (elapsedSeconds * 1e9 / hops) : 0.0;
        const ui64 hopsPerSecond = elapsedSeconds > 0.0 ? static_cast<ui64>(hops / elapsedSeconds) : 0;

        return {
            .ElapsedSeconds = elapsedSeconds,
            .ElapsedNsPerHop = elapsedNsPerHop,
            .TotalHops = hops,
            .HopsPerSecond = hopsPerSecond,
        };
    }

    static void RunPingPongCSV(
        const std::vector<ui32>& threadsList,
        const std::vector<ui32>& actorPairsList,
        TDuration duration = TDuration::Seconds(TSettings::DefaultPingPongDurationSeconds))
    {
        Cout << "threads,task_executors,actor_pairs,duration_seconds,total_hops,hops_per_sec,elapsed_seconds,ns_per_hop" << Endl;
        for (ui32 threads : threadsList) {
            for (ui32 actorPairs : actorPairsList) {
                const auto result = BenchPingPong(threads, actorPairs, duration);
                Cout << threads << ","
                    << threads << ","
                    << actorPairs << ","
                    << duration.Seconds() << ","
                    << result.TotalHops << ","
                    << result.HopsPerSecond << ","
                    << result.ElapsedSeconds << ","
                    << result.ElapsedNsPerHop
                    << Endl;
            }
        }
    }
};

} // namespace NActors::NTests
