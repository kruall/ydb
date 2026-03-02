#include "read.h"

#include "../dsproxy_get_impl.h"
#include <ydb/library/actors/task/task_system.h>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    namespace {

        TReadTaskResult MakeErrorResult(const TReadTaskArgs& args, NKikimrProto::EReplyStatus status, TString errorReason) {
            const ui32 groupId = args.SharedState && args.SharedState->GroupInfo
                ? args.SharedState->GroupInfo->GroupID.GetRawId()
                : 0;

            if (args.Request.Event) {
                return args.Request.Event->MakeErrorResponse(status, errorReason, TGroupId(groupId));
            }

            auto result = std::make_unique<TEvBlobStorage::TEvGetResult>(status, 0, groupId);
            result->ErrorReason = std::move(errorReason);
            return result;
        }

        void SetCommonResultFields(const TReadTaskArgs& args, TEvBlobStorage::TEvGetResult& result) {
            result.ExecutionRelay = args.Request.ExecutionRelay;
        }

        void SendVGets(const TBlobStorageGroupSharedState& sharedState, NActors::NTask::TTaskSystem::TEventQueue& queue,
                TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>>& vGets, const NWilson::TTraceId& traceId) {
            auto& topology = sharedState.GroupInfo->GetTopology();
            const TActorId sender = NActors::TActivationContext::AsActorContext().SelfID;
            const ui64 cookie = queue.Cookie();

            for (auto& ev : vGets) {
                Y_ABORT_UNLESS(ev->Record.HasVDiskID());
                const TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Record.GetVDiskID());
                const auto queueId = TGroupQueues::TVDisk::TQueues::VDiskQueueId(*ev);
                const auto& queues = sharedState.GroupQueues
                    ->FailDomains[topology.GetFailDomainOrderNumber(vdiskId)]
                    .VDisks[vdiskId.VDisk]
                    .Queues;

                const TActorId queueActorId = queues.GetQueue(queueId).ActorId;
                NActors::TActivationContext::Send(new IEventHandle(
                    queueActorId,
                    sender,
                    ev.release(),
                    0,
                    cookie,
                    nullptr,
                    traceId));
            }
        }

    } // namespace

    NActors::NTask::task<TReadTaskResult> RunReadTask(TReadTaskArgs args) {
        if (!args.SharedState || !args.Request.Event) {
            auto result = MakeErrorResult(args, NKikimrProto::ERROR, "missing read task arguments");
            SetCommonResultFields(args, *result);
            co_return result;
        }

        const auto& sharedState = *args.SharedState;
        if (!sharedState.IsReadyForGet || !sharedState.GroupInfo || !sharedState.GroupQueues) {
            auto result = MakeErrorResult(args, NKikimrProto::ERROR, "shared state is not ready for get");
            SetCommonResultFields(args, *result);
            co_return result;
        }

        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, args.Request.LogAccEnabled);
        TGetImpl getImpl(
            sharedState.GroupInfo,
            sharedState.GroupQueues,
            args.Request.Event.get(),
            TNodeLayoutInfoPtr(sharedState.NodeLayout),
            sharedState.AccelerationParams,
            logCtx.RequestPrefix);

        auto queue = NActors::NTask::TTaskSystem::CreateEventQueue();
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;

        getImpl.GenerateInitialRequests(logCtx, vGets);
        if (!vPuts.empty()) {
            auto result = MakeErrorResult(args, NKikimrProto::ERROR, "VPut is not supported in task read yet");
            SetCommonResultFields(args, *result);
            co_return result;
        }
        SendVGets(sharedState, queue, vGets, args.Request.TraceId);

        for (;;) {
            auto ev = co_await NActors::NTask::WaitEvent<TEvBlobStorage::TEvVGetResult>(queue);
            Y_ABORT_UNLESS(ev);
            const auto& record = ev->Get()->Record;
            Y_ABORT_UNLESS(record.HasStatus());

            const NKikimrProto::EReplyStatus status = record.GetStatus();
            if (status == NKikimrProto::RACE || status == NKikimrProto::BLOCKED || status == NKikimrProto::DEADLINE) {
                auto result = MakeErrorResult(args, status, "terminal status from VGetResult");
                if (status == NKikimrProto::RACE && record.HasVDiskID()) {
                    result->RacingGeneration = VDiskIDFromVDiskID(record.GetVDiskID()).GroupGeneration;
                }
                SetCommonResultFields(args, *result);
                co_return result;
            }
            if (status == NKikimrProto::NOTREADY) {
                auto result = MakeErrorResult(args, NKikimrProto::ERROR, "queue is not ready");
                SetCommonResultFields(args, *result);
                co_return result;
            }

            TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
            getImpl.OnVGetResult(logCtx, *ev->Get(), vGets, vPuts, getResult);

            if (!vPuts.empty()) {
                auto result = MakeErrorResult(args, NKikimrProto::ERROR, "VPut is not supported in task read yet");
                SetCommonResultFields(args, *result);
                co_return result;
            }
            SendVGets(sharedState, queue, vGets, args.Request.TraceId);

            if (getResult) {
                TReadTaskResult result(getResult.Release());
                SetCommonResultFields(args, *result);
                co_return result;
            }
        }
    }

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
