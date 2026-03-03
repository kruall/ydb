#include "range.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/actors/task/task_system.h>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    namespace {

        ui32 ResolveGroupId(const TReadRangeTaskArgs& args) {
            return args.GroupId;
        }

        TReadRangeTaskResult MakeErrorResult(
                const TReadRangeTaskArgs& args,
                NKikimrProto::EReplyStatus status,
                TString errorReason) {
            const ui32 groupId = ResolveGroupId(args);

            if (args.Request.Event) {
                return args.Request.Event->MakeErrorResponse(status, errorReason, TGroupId::FromValue(groupId));
            }

            auto result = std::make_unique<TEvBlobStorage::TEvRangeResult>(status, TLogoBlobID(), TLogoBlobID(), groupId);
            result->ErrorReason = std::move(errorReason);
            return result;
        }

        void SetCommonResultFields(const TReadRangeTaskArgs& args, TEvBlobStorage::TEvRangeResult& result) {
            result.ExecutionRelay = args.Request.ExecutionRelay;
        }

        NActors::NTask::task<TReadRangeTaskResult> ForwardRangeToProxy(
                TReadRangeTaskArgs& args,
                std::unique_ptr<TEvBlobStorage::TEvRange> request,
                TString reason) {
            if (!request) {
                auto result = MakeErrorResult(args, NKikimrProto::ERROR, reason + ": missing original request");
                SetCommonResultFields(args, *result);
                co_return result;
            }

            const ui32 groupId = ResolveGroupId(args);
            const TActorId proxyActorId = MakeBlobStorageProxyID(groupId);
            auto queue = NActors::NTask::TTaskSystem::CreateEventQueue();

            request->ExecutionRelay = args.Request.ExecutionRelay;
            request->ForceGroupGeneration = args.Request.ForceGroupGeneration;

            const auto& ctx = NActors::TActivationContext::AsActorContext();
            ctx.Send(proxyActorId, request.release(), 0, queue.Cookie());

            auto response = co_await NActors::NTask::WaitEvent<TEvBlobStorage::TEvRangeResult>(queue);
            Y_ABORT_UNLESS(response, "proxy must return TEvRangeResult");

            TReadRangeTaskResult result(response->Release().Release());
            Y_ABORT_UNLESS(result, "proxy TEvRangeResult payload is empty");
            SetCommonResultFields(args, *result);
            co_return result;
        }

    } // namespace

    NActors::NTask::task<TReadRangeTaskResult> RunReadRangeTask(TReadRangeTaskArgs args) {
        if (!args.Request.Event) {
            auto result = MakeErrorResult(args, NKikimrProto::ERROR, "missing range task arguments");
            SetCommonResultFields(args, *result);
            co_return result;
        }

        TBlobStorageGroupSharedStatePtr sharedState;
        if (auto* subSystem = NActors::TActivationContext::ActorSystem()
            ->GetSubSystem<TBlobStorageGroupSharedStateSubSystem>())
        {
            sharedState = subSystem->Find(ResolveGroupId(args));
        }

        auto request = std::make_unique<TEvBlobStorage::TEvRange>(TEvBlobStorage::CloneEventPolicy, *args.Request.Event);
        request->RestartCounter = args.Request.RestartCounter;

        if (!sharedState) {
            co_return co_await ForwardRangeToProxy(args, std::move(request), "shared state is missing");
        }

        if (!sharedState->IsReadyForGet || !sharedState->GroupInfo || !sharedState->GroupQueues) {
            co_return co_await ForwardRangeToProxy(args, std::move(request), "shared state is not ready for range");
        }

        // NOTE: range hot path via queues is not implemented yet in task-mode.
        co_return co_await ForwardRangeToProxy(args, std::move(request), "range task hot path is not implemented");
    }

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
