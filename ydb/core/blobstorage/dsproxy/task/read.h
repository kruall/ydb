#pragma once

#include "../dsproxy.h"

#include <ydb/library/actors/task/task.h>

#include <memory>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    struct TReadTaskRequestArgs {
        std::unique_ptr<TEvBlobStorage::TEvGet> Event;
        TActorId Source;
        ui64 Cookie = 0;
        TMonotonic Now = TMonotonic::Zero();
        ui32 RestartCounter = 0;
        NWilson::TTraceId TraceId = {};
        std::shared_ptr<TEvBlobStorage::TExecutionRelay> ExecutionRelay = nullptr;
        std::optional<ui32> ForceGroupGeneration;
        bool LogAccEnabled = false;
        TMaybe<TGroupStat::EKind> LatencyQueueKind = {};
    };

    struct TReadTaskArgs {
        ui32 GroupId = 0;
        TReadTaskRequestArgs Request;
    };

    using TReadTaskResult = std::unique_ptr<TEvBlobStorage::TEvGetResult>;

    NActors::NTask::task<TReadTaskResult> RunReadTask(TReadTaskArgs args);

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
