#pragma once

#include <ydb/core/memory_controller/memory_controller.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/config.h>

namespace NActors {
    struct TActorSystemSetup;
}

namespace NKikimr {

namespace NActorSystemConfigHelpers {

void AddExecutorPool(NActors::TCpuManagerConfig& cpuManager, const NKikimrConfig::TActorSystemConfig::TExecutor& poolConfig, const NKikimrConfig::TActorSystemConfig& systemConfig, ui32 poolId, NMonitoring::TDynamicCounterPtr counters);

NActors::TSchedulerConfig CreateSchedulerConfig(const NKikimrConfig::TActorSystemConfig::TScheduler& config);

void AddTaskSystemForUserPool(NActors::TActorSystemSetup& setup, ui32 userPoolId);

}  // namespace NActorSystemConfigHelpers

namespace NKikimrConfigHelpers {

NMemory::TResourceBrokerConfig CreateMemoryControllerResourceBrokerConfig(const NKikimrConfig::TAppConfig& config);

}  // namespace NKikimrConfigHelpers

}  // namespace NKikimr
