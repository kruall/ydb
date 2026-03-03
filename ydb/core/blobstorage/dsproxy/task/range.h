#pragma once

#include "../dsproxy.h"

#include <ydb/library/actors/task/task.h>

#include <memory>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    struct TReadRangeTaskRequestArgs {
        std::unique_ptr<TEvBlobStorage::TEvRange> Event;
        TActorId Source;
        ui64 Cookie = 0;
        TMonotonic Now = TMonotonic::Zero();
        ui32 RestartCounter = 0;
        NWilson::TTraceId TraceId = {};
        std::shared_ptr<TEvBlobStorage::TExecutionRelay> ExecutionRelay = nullptr;
        std::optional<ui32> ForceGroupGeneration;
    };

    struct TReadRangeTaskArgs {
        ui32 GroupId = 0;
        TReadRangeTaskRequestArgs Request;
    };

    using TReadRangeTaskResult = std::unique_ptr<TEvBlobStorage::TEvRangeResult>;

    NActors::NTask::task<TReadRangeTaskResult> RunReadRangeTask(TReadRangeTaskArgs args);

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
