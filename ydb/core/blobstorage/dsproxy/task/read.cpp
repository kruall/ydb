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
                return args.Request.Event->MakeErrorResponse(status, errorReason, TGroupId::FromValue(groupId));
            }

            auto result = std::make_unique<TEvBlobStorage::TEvGetResult>(status, 0, groupId);
            result->ErrorReason = std::move(errorReason);
            return result;
        }

        void SetCommonResultFields(const TReadTaskArgs& args, TEvBlobStorage::TEvGetResult& result) {
            result.ExecutionRelay = args.Request.ExecutionRelay;
        }

        NActors::NTask::task<TReadTaskResult> ForwardGetToProxy(TReadTaskArgs& args, TString reason) {
            if (!args.Request.Event) {
                auto result = MakeErrorResult(args, NKikimrProto::ERROR, reason + ": missing original request");
                SetCommonResultFields(args, *result);
                co_return result;
            }
            if (!args.ProxyActorId) {
                auto result = MakeErrorResult(args, NKikimrProto::ERROR, reason + ": proxy actor id is not set");
                SetCommonResultFields(args, *result);
                co_return result;
            }

            auto queue = NActors::NTask::TTaskSystem::CreateEventQueue();
            auto request = std::make_unique<TEvBlobStorage::TEvGet>(TEvBlobStorage::CloneEventPolicy, *args.Request.Event);
            request->RestartCounter = args.Request.RestartCounter;
            request->ExecutionRelay = args.Request.ExecutionRelay;
            request->ForceGroupGeneration = args.Request.ForceGroupGeneration;

            const auto& ctx = NActors::TActivationContext::AsActorContext();
            ctx.Send(args.ProxyActorId, request.release(), 0, queue.Cookie());

            auto response = co_await NActors::NTask::WaitEvent<TEvBlobStorage::TEvGetResult>(queue);
            Y_ABORT_UNLESS(response, "proxy must return TEvGetResult");

            TReadTaskResult result(response->Release().Release());
            Y_ABORT_UNLESS(result, "proxy TEvGetResult payload is empty");
            SetCommonResultFields(args, *result);
            co_return result;
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
                auto& queues = sharedState.GroupQueues
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
                    NWilson::TTraceId(traceId))));
            }
        }

    } // namespace

    NActors::NTask::task<TReadTaskResult> RunReadTask(TReadTaskArgs args) {
        if (!args.Request.Event) {
            auto result = MakeErrorResult(args, NKikimrProto::ERROR, "missing read task arguments");
            SetCommonResultFields(args, *result);
            co_return result;
        }
        if (!args.SharedState) {
            co_return co_await ForwardGetToProxy(args, "shared state is missing");
        }

        const auto& sharedState = *args.SharedState;
        if (!sharedState.IsReadyForGet || !sharedState.GroupInfo || !sharedState.GroupQueues) {
            co_return co_await ForwardGetToProxy(args, "shared state is not ready for get");
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
            if (status == NKikimrProto::RACE || status == NKikimrProto::NOTREADY) {
                co_return co_await ForwardGetToProxy(args, "fallback requested for terminal VGet status");
            }
            if (status == NKikimrProto::BLOCKED || status == NKikimrProto::DEADLINE) {
                auto result = MakeErrorResult(args, status, "terminal status from VGetResult");
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
