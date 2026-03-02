#include "read.h"

#include "../dsproxy_get_impl.h"
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/actors/task/task_system.h>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    namespace {

        ui32 ResolveGroupId(const TReadTaskArgs& args) {
            return args.GroupId;
        }

        TReadTaskResult MakeErrorResult(const TReadTaskArgs& args, NKikimrProto::EReplyStatus status, TString errorReason) {
            const ui32 groupId = ResolveGroupId(args);

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

        std::unique_ptr<TEvBlobStorage::TEvGet> BuildRestartRequest(
                TGetImpl& getImpl, const TReadTaskArgs& args, ui32 restartCounter) {
            auto base = getImpl.RestartQuery(restartCounter);
            Y_ABORT_UNLESS(base && base->Type() == TEvBlobStorage::EvGet, "RestartQuery must return TEvGet");
            auto request = std::unique_ptr<TEvBlobStorage::TEvGet>(static_cast<TEvBlobStorage::TEvGet*>(base.release()));
            request->IsIndexOnly = args.Request.Event->IsIndexOnly;
            request->ExecutionRelay = args.Request.ExecutionRelay;
            request->ForceGroupGeneration = args.Request.ForceGroupGeneration;
            return request;
        }

        TReadTaskResult MakeErrorResultFromRequest(TEvBlobStorage::TEvGet& request,
                const TReadTaskArgs& args, NKikimrProto::EReplyStatus status, TString errorReason) {
            auto result = request.MakeErrorResponse(status, errorReason, TGroupId::FromValue(ResolveGroupId(args)));
            SetCommonResultFields(args, *result);
            return result;
        }

        NActors::NTask::task<TReadTaskResult> ForwardGetToProxy(
                TReadTaskArgs& args, std::unique_ptr<TEvBlobStorage::TEvGet> request, TString reason) {
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

            while (!vGets.empty()) {
                std::unique_ptr<TEvBlobStorage::TEvVGet> ev = std::move(vGets.front());
                vGets.pop_front();
                Y_ABORT_UNLESS(ev, "empty VGet request in queue");
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
                    NWilson::TTraceId(traceId)));
            }
        }

    } // namespace

    NActors::NTask::task<TReadTaskResult> RunReadTask(TReadTaskArgs args) {
        if (!args.Request.Event) {
            auto result = MakeErrorResult(args, NKikimrProto::ERROR, "missing read task arguments");
            SetCommonResultFields(args, *result);
            co_return result;
        }
        TBlobStorageGroupSharedStatePtr sharedState;
        if (auto* subSystem = NActors::TActivationContext::ActorSystem()
            ->GetSubSystem<TBlobStorageGroupSharedStateSubSystem>())
        {
            sharedState = subSystem->Find(ResolveGroupId(args));
        }
        if (!sharedState) {
            auto request = std::make_unique<TEvBlobStorage::TEvGet>(TEvBlobStorage::CloneEventPolicy, *args.Request.Event);
            request->RestartCounter = args.Request.RestartCounter;
            co_return co_await ForwardGetToProxy(args, std::move(request), "shared state is missing");
        }

        const auto& resolvedSharedState = *sharedState;
        if (!resolvedSharedState.IsReadyForGet || !resolvedSharedState.GroupInfo || !resolvedSharedState.GroupQueues) {
            auto request = std::make_unique<TEvBlobStorage::TEvGet>(TEvBlobStorage::CloneEventPolicy, *args.Request.Event);
            request->RestartCounter = args.Request.RestartCounter;
            co_return co_await ForwardGetToProxy(args, std::move(request), "shared state is not ready for get");
        }

        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, args.Request.LogAccEnabled);
        TGetImpl getImpl(
            resolvedSharedState.GroupInfo,
            resolvedSharedState.GroupQueues,
            args.Request.Event.get(),
            TNodeLayoutInfoPtr(resolvedSharedState.NodeLayout),
            resolvedSharedState.AccelerationParams,
            logCtx.RequestPrefix);

        auto queue = NActors::NTask::TTaskSystem::CreateEventQueue();
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;

        getImpl.GenerateInitialRequests(logCtx, vGets);
        if (!vPuts.empty()) {
            auto request = BuildRestartRequest(getImpl, args, args.Request.RestartCounter);
            co_return MakeErrorResultFromRequest(*request, args, NKikimrProto::ERROR, "VPut is not supported in task read yet");
        }
        SendVGets(resolvedSharedState, queue, vGets, args.Request.TraceId);

        for (;;) {
            auto ev = co_await NActors::NTask::WaitEvent<TEvBlobStorage::TEvVGetResult>(queue);
            Y_ABORT_UNLESS(ev);
            const auto& record = ev->Get()->Record;
            Y_ABORT_UNLESS(record.HasStatus());

            const NKikimrProto::EReplyStatus status = record.GetStatus();
            if (status == NKikimrProto::RACE || status == NKikimrProto::NOTREADY) {
                const ui32 restartCounter = status == NKikimrProto::RACE
                    ? args.Request.RestartCounter + 1
                    : args.Request.RestartCounter;
                auto request = BuildRestartRequest(getImpl, args, restartCounter);
                co_return co_await ForwardGetToProxy(args, std::move(request), "fallback requested for terminal VGet status");
            }
            if (status == NKikimrProto::BLOCKED || status == NKikimrProto::DEADLINE) {
                auto request = BuildRestartRequest(getImpl, args, args.Request.RestartCounter);
                co_return MakeErrorResultFromRequest(*request, args, status, "terminal status from VGetResult");
            }

            TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
            getImpl.OnVGetResult(logCtx, *ev->Get(), vGets, vPuts, getResult);

            if (!vPuts.empty()) {
                auto request = BuildRestartRequest(getImpl, args, args.Request.RestartCounter);
                co_return MakeErrorResultFromRequest(*request, args, NKikimrProto::ERROR, "VPut is not supported in task read yet");
            }
            SendVGets(resolvedSharedState, queue, vGets, args.Request.TraceId);

            if (getResult) {
                TReadTaskResult result(getResult.Release());
                SetCommonResultFields(args, *result);
                co_return result;
            }
        }
    }

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
