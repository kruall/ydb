#include "range.h"

#include "read.h"

#include "../dsproxy_blob_tracker.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/library/actors/task/task_system.h>

#include <algorithm>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    namespace {

        constexpr ui32 MaxBlobsToQueryAtOnce = 8096;

        struct TBlobQueryItem {
            TLogoBlobID BlobId;
            bool RequiredToBePresent;
        };

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

        std::unique_ptr<TEvBlobStorage::TEvVGet> MakeRangeIndexQuery(const TEvBlobStorage::TEvRange& request,
                const TVDiskID& vdisk, const TLogoBlobID& from, const TLogoBlobID& to) {
            auto msg = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(
                vdisk,
                request.Deadline,
                NKikimrBlobStorage::EGetHandleClass::FastRead,
                TEvBlobStorage::TEvVGet::EFlags::ShowInternals,
                {},
                from,
                to,
                MaxBlobsToQueryAtOnce,
                nullptr,
                TEvBlobStorage::TEvVGet::TForceBlockTabletData(request.TabletId, request.ForceBlockedGeneration));
            msg->Record.SetSuppressBarrierCheck(true);
            return msg;
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

        std::unique_ptr<TEvBlobStorage::TEvGet> BuildFollowupGetRequest(const TEvBlobStorage::TEvRange& request,
                const TVector<TBlobQueryItem>& blobsToGet) {
            const ui32 queryCount = blobsToGet.size();
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[queryCount]);
            TEvBlobStorage::TEvGet::TQuery *query = queries.Get();
            for (const TBlobQueryItem& item : blobsToGet) {
                query->Set(item.BlobId, 0, 0);
                ++query;
            }
            Y_ABORT_UNLESS(query == queries.Get() + queryCount);

            auto get = std::make_unique<TEvBlobStorage::TEvGet>(
                queries,
                queryCount,
                request.Deadline,
                NKikimrBlobStorage::EGetHandleClass::FastRead,
                request.MustRestoreFirst,
                request.IsIndexOnly,
                TEvBlobStorage::TEvGet::TForceBlockTabletData(request.TabletId, request.ForceBlockedGeneration));
            get->IsInternal = true;
            get->Decommission = request.Decommission;
            return get;
        }

        void ReverseIfNeeded(const TEvBlobStorage::TEvRange& request, TEvBlobStorage::TEvRangeResult& result) {
            if (request.To < request.From) {
                std::reverse(result.Responses.begin(), result.Responses.end());
            }
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

        // Use the task hot path for ready shared-state.
        const auto& shared = *sharedState;
        Y_ABORT_UNLESS(request->TabletId == request->From.TabletID());
        Y_ABORT_UNLESS(request->TabletId == request->To.TabletID());

        TMap<TLogoBlobID, TBlobStatusTracker> blobStatus;
        TBlobStorageGroupInfo::TGroupVDisks failedDisks(&shared.GroupInfo->GetTopology());
        TVector<TBlobQueryItem> blobsToGet;

        auto queue = NActors::NTask::TTaskSystem::CreateEventQueue();
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        ui32 numVGetsPending = 0;

        for (const auto& vdisk : shared.GroupInfo->GetVDisks()) {
            vGets.push_back(MakeRangeIndexQuery(*request, shared.GroupInfo->GetVDiskId(vdisk.OrderNumber), request->From, request->To));
            ++numVGetsPending;
        }
        SendVGets(shared, queue, vGets, args.Request.TraceId);

        while (numVGetsPending) {
            auto ev = co_await NActors::NTask::WaitEvent<TEvBlobStorage::TEvVGetResult>(queue);
            Y_ABORT_UNLESS(ev);

            const auto& record = ev->Get()->Record;
            Y_ABORT_UNLESS(record.HasStatus());
            const NKikimrProto::EReplyStatus status = record.GetStatus();

            Y_ABORT_UNLESS(record.HasVDiskID());
            const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

            Y_ABORT_UNLESS(numVGetsPending > 0);
            --numVGetsPending;

            bool isOk = status == NKikimrProto::OK;
            switch (status) {
                case NKikimrProto::ERROR:
                case NKikimrProto::VDISK_ERROR_STATE:
                    failedDisks |= TBlobStorageGroupInfo::TGroupVDisks(&shared.GroupInfo->GetTopology(), vdisk);
                    if (!shared.GroupInfo->GetQuorumChecker().CheckFailModelForGroup(failedDisks)) {
                        auto result = MakeErrorResult(args, NKikimrProto::ERROR, "failed disks check fails on non-OK event status");
                        SetCommonResultFields(args, *result);
                        co_return result;
                    }
                    break;

                case NKikimrProto::OK:
                    if (record.ResultSize() == 0 && record.GetIsRangeOverflow()) {
                        isOk = false;
                        failedDisks |= TBlobStorageGroupInfo::TGroupVDisks(&shared.GroupInfo->GetTopology(), vdisk);
                        if (!shared.GroupInfo->GetQuorumChecker().CheckFailModelForGroup(failedDisks)) {
                            auto result = MakeErrorResult(args, NKikimrProto::ERROR, "failed disks check fails on OK event status");
                            SetCommonResultFields(args, *result);
                            co_return result;
                        }
                    }
                    break;

                default:
                    Y_ABORT("unexpected queryStatus# %s", NKikimrProto::EReplyStatus_Name(status).data());
            }

            if (isOk) {
                TLogoBlobID lastBlobId = request->From;
                for (const NKikimrBlobStorage::TQueryResult& blob : record.GetResult()) {
                    Y_ABORT_UNLESS(blob.HasBlobID());
                    const TLogoBlobID blobId(LogoBlobIDFromLogoBlobID(blob.GetBlobID()));
                    const TLogoBlobID fullId(blobId.FullID());
                    Y_ABORT_UNLESS(blobId == fullId);

                    auto it = blobStatus.find(fullId);
                    if (it == blobStatus.end()) {
                        it = blobStatus.emplace(fullId, TBlobStatusTracker(fullId, shared.GroupInfo.Get())).first;
                    }
                    it->second.UpdateFromResponseData(blob, vdisk, shared.GroupInfo.Get());

                    Y_ABORT_UNLESS(request->From <= request->To ? lastBlobId <= fullId : lastBlobId >= fullId,
                        "Blob IDs are out of order in TEvVGetResult");
                    lastBlobId = fullId;
                }

                if (record.ResultSize() >= MaxBlobsToQueryAtOnce || record.GetIsRangeOverflow()) {
                    TLogoBlobID from(request->From), to(request->To);
                    bool send = true;

                    ui32 cookie = lastBlobId.Cookie();
                    ui32 step = lastBlobId.Step();
                    ui32 generation = lastBlobId.Generation();
                    ui8 channel = lastBlobId.Channel();

                    if (from <= to) {
                        if (cookie++ == TLogoBlobID::MaxCookie) {
                            cookie = 0;
                            if (step++ == Max<ui32>()) {
                                if (generation++ == Max<ui32>()) {
                                    if (channel++ == Max<ui8>()) {
                                        send = false;
                                    }
                                }
                            }
                        }

                        from = TLogoBlobID(request->TabletId, generation, step, channel, 0, cookie);
                        send = send && from <= to;
                    } else {
                        if (!cookie--) {
                            cookie = TLogoBlobID::MaxCookie;
                            if (!step--) {
                                if (!generation--) {
                                    if (!channel--) {
                                        send = false;
                                    }
                                }
                            }
                        }

                        from = TLogoBlobID(request->TabletId, generation, step, channel, TLogoBlobID::MaxBlobSize, cookie);
                        send = send && to < from;
                    }

                    if (send) {
                        vGets.push_back(MakeRangeIndexQuery(*request, vdisk, from, to));
                        ++numVGetsPending;
                    }
                }
            }

            SendVGets(shared, queue, vGets, args.Request.TraceId);
        }

        std::unique_ptr<TEvBlobStorage::TEvRangeResult> result;
        if (request->IsIndexOnly && !request->MustRestoreFirst) {
            result = std::make_unique<TEvBlobStorage::TEvRangeResult>(NKikimrProto::OK, request->From, request->To, shared.GroupInfo->GroupID);
        }

        for (const auto& [blobId, tracker] : blobStatus) {
            bool lostByIngress = false;
            bool requiredToBePresent = false;
            switch (tracker.GetBlobState(shared.GroupInfo.Get(), &lostByIngress)) {
                case TBlobStorageGroupInfo::EBS_DISINTEGRATED: {
                    auto err = MakeErrorResult(args, NKikimrProto::ERROR, "BS disintegrated");
                    SetCommonResultFields(args, *err);
                    co_return err;
                }
                case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
                    requiredToBePresent = lostByIngress && IngressAsAReasonForErrorEnabled;
                    break;
                case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
                    requiredToBePresent = true;
                    break;
                case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
                    break;
                case TBlobStorageGroupInfo::EBS_FULL:
                    requiredToBePresent = true;
                    break;
            }

            if (result) {
                const auto& [keep, doNotKeep] = tracker.GetKeepFlags();
                result->Responses.emplace_back(blobId, TString(), keep, doNotKeep);
            } else {
                blobsToGet.push_back({blobId, requiredToBePresent});
            }
        }

        if (result) {
            ReverseIfNeeded(*request, *result);
            SetCommonResultFields(args, *result);
            co_return result;
        }

        if (!blobsToGet) {
            auto empty = std::make_unique<TEvBlobStorage::TEvRangeResult>(NKikimrProto::OK, request->From, request->To, shared.GroupInfo->GroupID);
            SetCommonResultFields(args, *empty);
            co_return empty;
        }

        auto getRequest = BuildFollowupGetRequest(*request, blobsToGet);
        auto getResult = co_await RunReadTask(TReadTaskArgs{
            .GroupId = args.GroupId,
            .Request = {
                .Event = std::move(getRequest),
                .Source = args.Request.Source,
                .Cookie = args.Request.Cookie,
                .Now = args.Request.Now,
                .RestartCounter = args.Request.RestartCounter,
                .TraceId = NWilson::TTraceId(args.Request.TraceId),
                .ExecutionRelay = args.Request.ExecutionRelay,
                .ForceGroupGeneration = args.Request.ForceGroupGeneration,
            }
        });

        if (!getResult || getResult->Status != NKikimrProto::OK) {
            auto err = MakeErrorResult(args, getResult ? getResult->Status : NKikimrProto::ERROR,
                getResult ? getResult->ErrorReason : TString("task get stage failed"));
            SetCommonResultFields(args, *err);
            co_return err;
        }

        auto out = std::make_unique<TEvBlobStorage::TEvRangeResult>(NKikimrProto::OK, request->From, request->To, shared.GroupInfo->GroupID);
        out->Responses.reserve(getResult->ResponseSz);
        Y_ABORT_UNLESS(getResult->ResponseSz == blobsToGet.size());
        for (ui32 i = 0; i < getResult->ResponseSz; ++i) {
            TEvBlobStorage::TEvGetResult::TResponse &response = getResult->Responses[i];
            Y_ABORT_UNLESS(response.Id == blobsToGet[i].BlobId);

            if (response.Status == NKikimrProto::OK) {
                out->Responses.emplace_back(response.Id, request->IsIndexOnly ? TString() : response.Buffer.ConvertToString(),
                    response.Keep, response.DoNotKeep);
            } else if (response.Status != NKikimrProto::NODATA || blobsToGet[i].RequiredToBePresent) {
                auto err = MakeErrorResult(args, NKikimrProto::ERROR, getResult->ErrorReason);
                SetCommonResultFields(args, *err);
                co_return err;
            }
        }

        ReverseIfNeeded(*request, *out);
        SetCommonResultFields(args, *out);
        co_return out;
    }

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
