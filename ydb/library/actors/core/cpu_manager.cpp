#include "cpu_manager.h"
#include "executor_pool_jail.h"
#include "mon_stats.h"
#include "probes.h"
#include "debug.h"

#include "executor_pool_basic.h"
#include "executor_pool_io.h"
#include "executor_pool_united.h"

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    TCpuManager::TCpuManager(THolder<TActorSystemSetup>& setup)
        : ExecutorPoolCount(setup->GetExecutorsCount())
        , Config(setup->CpuManager)
    {
        if (setup->Executors) { // Explicit mode w/o united pools
            Executors.Reset(setup->Executors.Release());
        } else {
            Setup();
        }
    }

    TCpuManager::~TCpuManager() {
    }

    void TCpuManager::SetupUnited() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::SetupUnited: start");
        TVector<TPoolInfo> poolInfos;

        std::vector<i16> poolIds(Config.Basic.size());
        std::iota(poolIds.begin(), poolIds.end(), 0);
        std::sort(poolIds.begin(), poolIds.end(), [&](i16 a, i16 b) {
            if (Config.Basic[a].Priority != Config.Basic[b].Priority) {
                return Config.Basic[a].Priority > Config.Basic[b].Priority;
            }
            return Config.Basic[a].PoolId < Config.Basic[b].PoolId;
        });

        for (ui32 i = 0; i < Config.Basic.size(); ++i) {
            if (Config.Basic[poolIds[i]].MaxThreadCount > 0) {
                poolInfos.push_back(TPoolInfo{static_cast<i16>(Config.Basic[poolIds[i]].PoolId), Config.Basic[poolIds[i]].DefaultThreadCount, Config.Basic[poolIds[i]].MaxThreadCount, Config.Basic[poolIds[i]].PoolName});
            } else {
                poolInfos.push_back(TPoolInfo{static_cast<i16>(Config.Basic[poolIds[i]].PoolId), static_cast<i16>(Config.Basic[poolIds[i]].Threads), static_cast<i16>(Config.Basic[poolIds[i]].Threads), Config.Basic[poolIds[i]].PoolName});
            }
        }
        for (ui32 i = 0; i < Config.IO.size(); ++i) {
            poolInfos.push_back(TPoolInfo{static_cast<i16>(Config.IO[i].PoolId), 0, 0, Config.IO[i].PoolName});
        }

        Config.United.SoftProcessingDurationTs = Us2Ts(100'000);
        if (Config.Basic.size() > 0 && Config.Basic[0].UseRingQueue) {
            Config.United.UseRingQueue = true;
        }
        United = std::make_unique<TUnitedExecutorPool>(Config.United, poolInfos);
        Executors.Reset(new TAutoPtr<IExecutorPool>[ExecutorPoolCount]);
        for (ui32 i = 0; i < Config.Basic.size(); ++i) {
            ui32 id = Config.Basic[poolIds[i]].PoolId;
            Executors[id].Reset(United->MakePseudoPool(id).release());
        }
        for (ui32 i = 0; i < Config.IO.size(); ++i) {
            Executors[Config.IO[i].PoolId].Reset(CreateExecutorPool(Config.IO[i].PoolId));
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::SetupUnited: end");
    }

    void TCpuManager::Setup() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Setup: start");
        TAffinity available;
        available.Current();

        Config.United.UseUnited = false;
        Config.Shared.SoftProcessingDurationTs = Us2Ts(10'000);
        if (Config.United.UseUnited) {
            SetupUnited();
            return;
        }

        if (Config.Jail) {
            Jail = std::make_unique<TExecutorPoolJail>(ExecutorPoolCount, *Config.Jail);
        }

        std::vector<i16> poolsWithSharedThreads;
        for (TBasicExecutorPoolConfig& cfg : Config.Basic) {
            if (cfg.HasSharedThread) {
                poolsWithSharedThreads.push_back(cfg.PoolId);
            }
        }
        Shared.reset(CreateSharedExecutorPool(Config.Shared, ExecutorPoolCount, poolsWithSharedThreads));
        auto sharedPool = static_cast<ISharedExecutorPool*>(Shared.get());

        ui64 ts = GetCycleCountFast();
        Harmonizer.reset(MakeHarmonizer(ts));
        Harmonizer->SetSharedPool(sharedPool);

        Executors.Reset(new TAutoPtr<IExecutorPool>[ExecutorPoolCount]);

        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx].Reset(CreateExecutorPool(excIdx));
            if (excIdx < Config.PingInfoByPool.size()) {
                Harmonizer->AddPool(Executors[excIdx].Get(), &Config.PingInfoByPool[excIdx]);
            } else {
                Harmonizer->AddPool(Executors[excIdx].Get());
            }
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Setup: end");
    }

    void TCpuManager::PrepareStart(TVector<NSchedulerQueue::TReader*>& scheduleReaders, TActorSystem* actorSystem) {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStart: start");
        NSchedulerQueue::TReader* readers;
        ui32 readersCount = 0;
        if (Shared) {
            Shared->Prepare(actorSystem, &readers, &readersCount);
            for (ui32 i = 0; i != readersCount; ++i, ++readers) {
                scheduleReaders.push_back(readers);
            }
        }
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Y_ABORT_UNLESS(Executors[excIdx].Get() != nullptr, "Executor pool is nullptr excIdx %" PRIu32, excIdx);
            Executors[excIdx]->Prepare(actorSystem, &readers, &readersCount);
            for (ui32 i = 0; i != readersCount; ++i, ++readers) {
                scheduleReaders.push_back(readers);
            }
        }
        if (United) {
            United->Prepare(actorSystem, &readers, &readersCount);
            for (ui32 i = 0; i != readersCount; ++i, ++readers) {
                scheduleReaders.push_back(readers);
            }
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStart: end");
    }

    void TCpuManager::Start() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Start: start");
        if (Shared) {
            Shared->Start();
        }
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Start();
        }
        if (United) {
            United->Start();
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Start: end");
    }

    void TCpuManager::PrepareStop() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStop: start");
        if (United) {
            United->PrepareStop();
        }
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->PrepareStop();
        }
        if (Shared) {
            Shared->PrepareStop();
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStop: end");
    }

    void TCpuManager::Shutdown() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Shutdown: start");
        if (United) {
            United->Shutdown();
        }
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Shutdown();
        }
        if (Shared) {
            Shared->Shutdown();
        }
        if (United) {
            United->Shutdown();
        }
        for (ui32 round = 0, done = 0; done < ExecutorPoolCount; ++round) {
            Y_ABORT_UNLESS(round < 10, "actorsystem cleanup could not be completed in 10 rounds");
            done = 0;
            for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
                if (Executors[excIdx]->Cleanup()) {
                    ++done;
                }
            }
        }
        if (Shared) {
            Shared->Cleanup();
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Shutdown: end");
    }

    void TCpuManager::Cleanup() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Cleanup: start");
        if (United) {
            United->Cleanup();
        }
        for (ui32 round = 0, done = 0; done < ExecutorPoolCount; ++round) {
            Y_ABORT_UNLESS(round < 10, "actorsystem cleanup could not be completed in 10 rounds");
            done = 0;
            for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
                if (Executors[excIdx]->Cleanup()) {
                    ++done;
                }
            }
        }
        if (Shared) {
            Shared->Cleanup();
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Cleanup: Destroy");
        if (United) {
            United.reset();
        }
        if (Shared) {
            Shared.reset();
        }
        Executors.Destroy();

        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Cleanup: end");
    }

    IExecutorPool* TCpuManager::CreateExecutorPool(ui32 poolId) {
        for (TBasicExecutorPoolConfig& cfg : Config.Basic) {
            if (cfg.PoolId == poolId) {
                if (cfg.HasSharedThread) {
                    auto *sharedPool = Shared.get();
                    auto *pool = new TBasicExecutorPool(cfg, Harmonizer.get(), Jail.get());
                    pool->AddSharedThread(sharedPool->GetSharedThread(poolId));
                    return pool;
                } else {
                    return new TBasicExecutorPool(cfg, Harmonizer.get(), Jail.get());
                }
            }
        }
        for (TIOExecutorPoolConfig& cfg : Config.IO) {
            if (cfg.PoolId == poolId) {
                return new TIOExecutorPool(cfg);
            }
        }
        Y_ABORT("missing PoolId: %d", int(poolId));
    }

    TVector<IExecutorPool*> TCpuManager::GetBasicExecutorPools() const {
        TVector<IExecutorPool*> pools;
        for (ui32 idx = 0; idx < ExecutorPoolCount; ++idx) {
            if (auto basicPool = dynamic_cast<TBasicExecutorPool*>(Executors[idx].Get()); basicPool != nullptr) {
                pools.push_back(basicPool);
            }
        }
        return pools;
    }

    void TCpuManager::GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy, TVector<TExecutorThreadStats>& sharedStatsCopy) const {
        if (poolId < ExecutorPoolCount) {
            Executors[poolId]->GetCurrentStats(poolStats, statsCopy);
        }
        if (Shared) {
            Shared->GetSharedStats(poolId, sharedStatsCopy);
        }
    }

    void TCpuManager::GetExecutorPoolState(i16 poolId, TExecutorPoolState &state) const {
        if (static_cast<ui32>(poolId) < ExecutorPoolCount) {
            Executors[poolId]->GetExecutorPoolState(state);
        }
    }

    void TCpuManager::GetExecutorPoolStates(std::vector<TExecutorPoolState> &states) const {
        states.resize(ExecutorPoolCount);
        for (i16 poolId = 0; poolId < static_cast<ui16>(ExecutorPoolCount); ++poolId) {
            GetExecutorPoolState(poolId, states[poolId]);
        }
    }

}
