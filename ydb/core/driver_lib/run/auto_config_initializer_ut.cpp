#include "auto_config_initializer.h"
#include "config_helpers.h"

#include <ydb/core/protos/config.pb.h>
#include <library/cpp/testing/unittest/registar.h>


Y_UNIT_TEST_SUITE(AutoConfig) {

using namespace NKikimr;
using namespace NAutoConfigInitializer;

namespace {

NActors::TCpuManagerConfig BuildCpuManagerFromActorSystemConfig(const NKikimrConfig::TActorSystemConfig& config) {
    NActors::TCpuManagerConfig cpuManager;
    for (ui32 poolId = 0; poolId < config.ExecutorSize(); ++poolId) {
        NActorSystemConfigHelpers::AddExecutorPool(cpuManager, config.GetExecutor(poolId), config, poolId, nullptr);
    }
    return cpuManager;
}

NActors::TCpuManagerConfig BuildCpuManagerFromTinyAutoConfig(ui32 cpuCount) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetUseSharedThreads(true);
    config.SetCpuCount(cpuCount);
    ApplyAutoConfig(&config, false, false);
    return BuildCpuManagerFromActorSystemConfig(config);
}

std::vector<NActors::EHarmonizerPoolKind> GetPoolKinds(const NActors::TCpuManagerConfig& cpuManager) {
    std::vector<NActors::EHarmonizerPoolKind> kinds;
    kinds.reserve(cpuManager.Basic.size());
    for (const auto& pool : cpuManager.Basic) {
        kinds.push_back(pool.HarmonizerPoolKind);
    }
    return kinds;
}

std::vector<i16> GetForcedForeignSlots(const NActors::TCpuManagerConfig& cpuManager) {
    std::vector<i16> slots;
    slots.reserve(cpuManager.Basic.size());
    for (const auto& pool : cpuManager.Basic) {
        slots.push_back(pool.ForcedForeignSlotCount);
    }
    return slots;
}

} // namespace

#define ASSERT_POOLS(pools, sys, user, batch, io, ic) \
    do { \
        UNIT_ASSERT_VALUES_EQUAL(pools.SystemPoolId, sys); \
        UNIT_ASSERT_VALUES_EQUAL(pools.UserPoolId, user); \
        UNIT_ASSERT_VALUES_EQUAL(pools.BatchPoolId, batch); \
        UNIT_ASSERT_VALUES_EQUAL(pools.IOPoolId, io); \
        UNIT_ASSERT_VALUES_EQUAL(pools.ICPoolId, ic); \
    } while (false) \
// ASSERT_POOLS

Y_UNIT_TEST(GetASPoolsith1CPU) {
    TASPools pools = GetASPools(1);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(1);
    pools = GetASPools(config, true);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndeces(),  (std::vector<ui8>{0, 0, 0, 1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{40, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolNames(), (std::vector<TString>{"Common", "IO"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolCount(), 2);
}

Y_UNIT_TEST(GetASPoolsWith2CPUs) {
    TASPools pools = GetASPools(2);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(2);
    pools = GetASPools(config, true);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndeces(),  (std::vector<ui8>{0, 0, 0, 1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{40, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolNames(), (std::vector<TString>{"Common", "IO"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolCount(), 2);
}

Y_UNIT_TEST(GetASPoolsWith3CPUs) {
    TASPools pools = GetASPools(3);
    ASSERT_POOLS(pools, 0, 0, 1, 2, 3);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(3);
    pools = GetASPools(config, true);
    ASSERT_POOLS(pools, 0, 0, 1, 2, 3);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndeces(),  (std::vector<ui8>{0, 0, 1, 2, 3}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{30, 10, 0, 40}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolNames(), (std::vector<TString>{"Common", "Batch", "IO", "IC"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolCount(), 4);
}

Y_UNIT_TEST(GetASPoolsWith4AndMoreCPUs) {
    for (ui32 threadCount = 4; threadCount < 128; ++threadCount) {
        TASPools pools = GetASPools(threadCount);
        ASSERT_POOLS(pools, 0, 1, 2, 3, 4);

        NKikimrConfig::TActorSystemConfig config;
        config.SetUseAutoConfig(true);
        config.SetCpuCount(threadCount);
        pools = GetASPools(config, true);
        ASSERT_POOLS(pools, 0, 1, 2, 3, 4);

        UNIT_ASSERT_VALUES_EQUAL(pools.GetIndeces(),  (std::vector<ui8>{0, 1, 2, 3, 4}));
        UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{30, 20, 10, 0, 40}));
        UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolNames(), (std::vector<TString>{"System", "User", "Batch", "IO", "IC"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolCount(), 5);
    }
}


Y_UNIT_TEST(GetServicePoolsWith1CPU) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(1);
    TMap<TString, ui32> services = GetServicePools(config, true);
    UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Interconnect", 0}}));
}

Y_UNIT_TEST(GetServicePoolsWith2CPUs) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(2);
    TMap<TString, ui32> services = GetServicePools(config, true);
    UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Interconnect", 0}}));
}

Y_UNIT_TEST(GetServicePoolsWith3CPUs) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(3);
    TMap<TString, ui32> services = GetServicePools(config, true);
    UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Interconnect", 3}}));
}

Y_UNIT_TEST(GetServicePoolsWith4AndMoreCPUs) {
    for (ui32 threadCount = 4; threadCount < 128; ++threadCount) {
        NKikimrConfig::TActorSystemConfig config;
        config.SetUseAutoConfig(true);
        config.SetCpuCount(threadCount);
        TMap<TString, ui32> services = GetServicePools(config, true);
        UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Interconnect", 4}}));
    }
}

Y_UNIT_TEST(TinySharedBorrowedPoolKeepsZeroForeignLease) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseSharedThreads(true);

    auto* sharedOnlyExecutor = config.AddExecutor();
    sharedOnlyExecutor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    sharedOnlyExecutor->SetName("System");
    sharedOnlyExecutor->SetThreads(1);
    sharedOnlyExecutor->SetMaxThreads(1);
    sharedOnlyExecutor->SetHasSharedThread(true);

    auto* borrowedExecutor = config.AddExecutor();
    borrowedExecutor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    borrowedExecutor->SetName("Batch");
    borrowedExecutor->SetThreads(0);
    borrowedExecutor->SetMaxThreads(0);
    borrowedExecutor->SetHasSharedThread(false);

    NActors::TCpuManagerConfig cpuManager = BuildCpuManagerFromActorSystemConfig(config);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[0].HarmonizerPoolKind, NActors::EHarmonizerPoolKind::OwnedSharedOnly);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[1].HarmonizerPoolKind, NActors::EHarmonizerPoolKind::ForeignSharedOnly);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[1].ForcedForeignSlotCount, 0);
}

Y_UNIT_TEST(NonTinySharedPoolsStayRegularKinds) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseSharedThreads(true);

    auto* sharedExecutor = config.AddExecutor();
    sharedExecutor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    sharedExecutor->SetName("System");
    sharedExecutor->SetThreads(1);
    sharedExecutor->SetMaxThreads(1);
    sharedExecutor->SetHasSharedThread(true);

    auto* regularExecutor = config.AddExecutor();
    regularExecutor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    regularExecutor->SetName("User");
    regularExecutor->SetThreads(1);
    regularExecutor->SetMaxThreads(1);
    regularExecutor->SetHasSharedThread(false);

    NActors::TCpuManagerConfig cpuManager = BuildCpuManagerFromActorSystemConfig(config);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[0].HarmonizerPoolKind, NActors::EHarmonizerPoolKind::RegularWithOwnedShared);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[1].HarmonizerPoolKind, NActors::EHarmonizerPoolKind::Regular);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[1].ForcedForeignSlotCount, 0);
}

Y_UNIT_TEST(TinyAutoConfigWith1CPUUsesOnlySharedAndBorrowedPools) {
    const auto cpuManager = BuildCpuManagerFromTinyAutoConfig(1);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(
        GetPoolKinds(cpuManager),
        (std::vector<NActors::EHarmonizerPoolKind>{
            NActors::EHarmonizerPoolKind::ForeignSharedOnly,
            NActors::EHarmonizerPoolKind::ForeignSharedOnly,
            NActors::EHarmonizerPoolKind::ForeignSharedOnly,
            NActors::EHarmonizerPoolKind::OwnedSharedOnly,
        })
    );
    UNIT_ASSERT_VALUES_EQUAL(GetForcedForeignSlots(cpuManager), (std::vector<i16>{0, 0, 0, 0}));
}

Y_UNIT_TEST(TinyAutoConfigWith2CPUsUsesOnlySharedAndBorrowedPools) {
    const auto cpuManager = BuildCpuManagerFromTinyAutoConfig(2);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(
        GetPoolKinds(cpuManager),
        (std::vector<NActors::EHarmonizerPoolKind>{
            NActors::EHarmonizerPoolKind::ForeignSharedOnly,
            NActors::EHarmonizerPoolKind::OwnedSharedOnly,
            NActors::EHarmonizerPoolKind::ForeignSharedOnly,
            NActors::EHarmonizerPoolKind::OwnedSharedOnly,
        })
    );
    UNIT_ASSERT_VALUES_EQUAL(GetForcedForeignSlots(cpuManager), (std::vector<i16>{1, 1, 0, 1}));
}

Y_UNIT_TEST(TinyAutoConfigWith3CPUsUsesOnlySharedAndBorrowedPools) {
    const auto cpuManager = BuildCpuManagerFromTinyAutoConfig(3);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(
        GetPoolKinds(cpuManager),
        (std::vector<NActors::EHarmonizerPoolKind>{
            NActors::EHarmonizerPoolKind::OwnedSharedOnly,
            NActors::EHarmonizerPoolKind::OwnedSharedOnly,
            NActors::EHarmonizerPoolKind::ForeignSharedOnly,
            NActors::EHarmonizerPoolKind::OwnedSharedOnly,
        })
    );
    UNIT_ASSERT_VALUES_EQUAL(GetForcedForeignSlots(cpuManager), (std::vector<i16>{1, 2, 0, 1}));
}

} // Y_UNIT_TEST_SUITE(AutoConfig)
