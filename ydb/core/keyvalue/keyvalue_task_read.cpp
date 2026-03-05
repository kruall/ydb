#include "keyvalue_task_read.h"
#include "keyvalue_const.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/dsproxy/task/read.h>
#include <ydb/library/actors/task/task_system.h>

#include <util/generic/algorithm.h>

#include <limits>

namespace NKikimr::NKeyValue::NTask {

namespace {

constexpr ui64 ReadResultSizeEstimationNewApi = 1 + 5 // Key id, length
    + 1 + 5 // Value id, length
    + 1 + 8 // Offset id, value
    + 1 + 8 // Size id, value
    + 1 + 1 // Status id, value
    ;

constexpr ui64 ErrorMessageSizeEstimation = 128;

bool IsSnapshotActual(const TReadSharedSnapshotPtr& snapshot) {
    return snapshot && snapshot->IsActual.load(std::memory_order_acquire);
}

template <typename TypeWithPriority>
void SetPriority(NKikimrBlobStorage::EGetHandleClass *outHandleClass, ui8 priority) {
    *outHandleClass = NKikimrBlobStorage::FastRead;
    if constexpr (std::is_same_v<TypeWithPriority, NKikimrKeyValue::Priorities>) {
        switch (priority) {
            case TypeWithPriority::PRIORITY_UNSPECIFIED:
            case TypeWithPriority::PRIORITY_REALTIME:
                *outHandleClass = NKikimrBlobStorage::FastRead;
                break;
            case TypeWithPriority::PRIORITY_BACKGROUND:
                *outHandleClass = NKikimrBlobStorage::AsyncRead;
                break;
        }
    } else {
        switch (priority) {
            case TypeWithPriority::REALTIME:
                *outHandleClass = NKikimrBlobStorage::FastRead;
                break;
            case TypeWithPriority::BACKGROUND:
                *outHandleClass = NKikimrBlobStorage::AsyncRead;
                break;
        }
    }
}

template <typename TypeWithPriority, bool WithOverrun = false, ui64 SpecificReadResultSizeEstimation = ReadResultSizeEstimationNewApi>
bool PrepareOneRead(const TString &key, const TReadIndexRecordSnapshot &indexRecord, ui64 offset, ui64 size,
        ui8 priority, ui64 cmdLimitBytes, THolder<TIntermediate> &intermediate, TIntermediate::TRead &response,
        bool &outIsInlineOnly)
{
    for (const auto& item : indexRecord.Chain) {
        if (!item.IsInline()) {
            outIsInlineOnly = false;
            break;
        }
    }

    if (!size) {
        size = std::numeric_limits<decltype(size)>::max();
    }
    const ui64 fullValueSize = indexRecord.GetFullValueSize();
    offset = std::min(offset, fullValueSize);
    size = std::min(size, fullValueSize - offset);
    const ui64 metadataSize = key.size() + SpecificReadResultSizeEstimation;
    const ui64 recSize = std::max(size, ErrorMessageSizeEstimation) + metadataSize;

    response.Offset = offset;
    response.RequestedSize = size;
    bool isOverrun = false;

    if (intermediate->IsTruncated
            || intermediate->TotalSize + recSize > intermediate->TotalSizeLimit
            || (cmdLimitBytes && intermediate->TotalSize + recSize > cmdLimitBytes)) {
        response.Status = NKikimrProto::OVERRUN;
        if (!WithOverrun
                || std::min(intermediate->TotalSizeLimit, cmdLimitBytes) < intermediate->TotalSize + metadataSize) {
            return true;
        }
        if (cmdLimitBytes) {
            size = std::min(intermediate->TotalSizeLimit, cmdLimitBytes) - intermediate->TotalSize - metadataSize;
        } else {
            size = intermediate->TotalSizeLimit - intermediate->TotalSize - metadataSize;
        }
        isOverrun = true;
    }

    response.ValueSize = size;
    response.CreationUnixTime = indexRecord.CreationUnixTime;
    response.Key = key;

    SetPriority<TypeWithPriority>(&response.HandleClass, priority);

    if (size) {
        const ui32 numReads = indexRecord.GetReadItems(offset, size, response);
        intermediate->TotalSize += recSize;
        intermediate->TotalReadsScheduled += numReads;
    } else if (response.Status != NKikimrProto::OVERRUN) {
        response.Status = NKikimrProto::OK;
    }
    return isOverrun;
}

THolder<TIntermediate> MakeReadIntermediate(const TReadSharedSnapshot& snapshot, const TReadTaskArgs& args) {
    auto intermediate = MakeHolder<TIntermediate>(
        args.RespondTo,
        args.KeyValueActorId,
        snapshot.ChannelGeneration,
        snapshot.ChannelStep,
        TRequestType::ReadOnly,
        NWilson::TTraceId(args.TraceId));
    intermediate->EvType = TEvKeyValue::TEvRead::EventType;
    intermediate->UsePayloadInResponse = args.UsePayloadInResponse;
    return intermediate;
}

bool CheckDeadline(const NKikimrKeyValue::ReadRequest& request, THolder<TIntermediate>& intermediate) {
    const ui64 deadlineInstantMs = request.deadline_instant_ms();
    if (!deadlineInstantMs) {
        return false;
    }

    intermediate->Deadline = TInstant::MicroSeconds(deadlineInstantMs * 1000ull);

    const TInstant now = TAppData::TimeProvider->Now();
    return intermediate->Deadline <= now;
}

std::unique_ptr<TEvKeyValue::TEvReadResponse> BuildReadResponse(THolder<TIntermediate>& intermediate,
        const TActorId& respondTo, NKikimrKeyValue::Statuses::ReplyStatus status)
{
    auto response = std::make_unique<TEvKeyValue::TEvReadResponse>();
    response->Record.set_status(status);
    if (intermediate->HasCookie) {
        response->Record.set_cookie(intermediate->Cookie);
    }

    auto& cmd = *intermediate->ReadCommand;
    Y_ABORT_UNLESS(std::holds_alternative<TIntermediate::TRead>(cmd));
    auto& interRead = std::get<TIntermediate::TRead>(cmd);
    response->Record.set_requested_key(interRead.Key);
    response->Record.set_requested_offset(interRead.Offset);
    response->Record.set_requested_size(interRead.RequestedSize);

    TRope value = interRead.BuildRope();
    if (intermediate->UsePayloadInResponse) {
        response->SetBuffer(std::move(value));
    } else {
        const TContiguousSpan span = value.GetContiguousSpan();
        response->Record.set_value(span.data(), span.size());
    }

    if (respondTo.NodeId() != NActors::TActivationContext::AsActorContext().SelfID.NodeId()) {
        response->Record.set_node_id(NActors::TActivationContext::AsActorContext().SelfID.NodeId());
    }

    return response;
}

struct TReadItemInfo {
    TIntermediate::TRead* Read = nullptr;
    TIntermediate::TRead::TReadItem* ReadItem = nullptr;
};

struct TGetBatch {
    TStackVec<ui32, 1> ReadItemIndecies;
    ui32 GroupId = 0;
};

} // namespace

ui64 TReadIndexRecordSnapshot::GetFullValueSize() const {
    return Chain.empty() ? 0 : Chain.back().Offset + Chain.back().GetSize();
}

ui32 TReadIndexRecordSnapshot::GetReadItems(ui64 offset, ui64 size, TIntermediate::TRead& read) const {
    if (!size) {
        return 0;
    }

    auto it = UpperBound(Chain.begin(), Chain.end(), offset);
    Y_ABORT_UNLESS(it != Chain.begin());
    --it;
    Y_ABORT_UNLESS(offset >= it->Offset);
    offset -= it->Offset;

    ui64 valueOffset = 0;
    ui32 numReads = 0;
    while (size) {
        Y_ABORT_UNLESS(it != Chain.end());
        const ui32 readSize = Min<ui64>(size, it->GetSize() - offset);
        if (it->IsInline()) {
            const auto begin = it->InlineData.Position(offset);
            const auto end = begin + readSize;
            read.Value.Write(valueOffset, TRope(begin, end));
        } else {
            read.ReadItems.emplace_back(it->LogoBlobId, static_cast<ui32>(offset), readSize, valueOffset);
        }
        size -= readSize;
        offset = 0;
        valueOffset += readSize;
        ++it;
        ++numReads;
    }

    return numReads;
}

void TReadSharedSnapshot::Invalidate() const {
    IsActual.store(false, std::memory_order_release);
}

NActors::NTask::task<TReadTaskResult> RunReadTask(TReadTaskArgs args) {
    TReadTaskResult out;

    TReadSharedSnapshotPtr snapshot;
    if (auto* subSystem = NActors::TActivationContext::ActorSystem()->GetSubSystem<TReadSharedSnapshotSubSystem>()) {
        snapshot = subSystem->Find(args.TabletId);
    }

    if (!IsSnapshotActual(snapshot) || !snapshot->TabletInfo) {
        co_return out;
    }

    auto intermediate = MakeReadIntermediate(*snapshot, args);
    auto& request = args.Request;

    intermediate->HasCookie = true;
    intermediate->Cookie = request.cookie();

    intermediate->ReadCommand = TIntermediate::TRead();
    auto& response = std::get<TIntermediate::TRead>(*intermediate->ReadCommand);
    response.Key = request.key();
    response.Offset = request.offset();

    if (CheckDeadline(request, intermediate)) {
        co_return out;
    }

    if (request.has_lock_generation() && request.lock_generation() != snapshot->UserGeneration) {
        co_return out;
    }

    const auto it = snapshot->Index.find(request.key());
    if (it == snapshot->Index.end()) {
        co_return out;
    }

    bool isInlineOnly = true;
    const bool isOverrun = PrepareOneRead<NKikimrKeyValue::Priorities, true>(
        it->first,
        it->second,
        request.offset(),
        request.size(),
        request.priority(),
        request.limit_bytes(),
        intermediate,
        response,
        isInlineOnly);
    Y_UNUSED(isInlineOnly);
    intermediate->IsTruncated = isOverrun;

    if (!IsSnapshotActual(snapshot)) {
        co_return out;
    }

    TStackVec<TReadItemInfo, 1> readItems;
    readItems.reserve(response.ReadItems.size());
    for (auto& readItem : response.ReadItems) {
        readItems.push_back({&response, &readItem});
    }

    if (!readItems.empty()) {
        TStackVec<TGetBatch, 1> batches;
        batches.reserve(readItems.size());
        THashMap<ui32, ui32> mapFromGroupToBatch;

        for (ui32 readItemIdx = 0; readItemIdx < readItems.size(); ++readItemIdx) {
            auto& readItem = *readItems[readItemIdx].ReadItem;
            const TLogoBlobID& id = readItem.LogoBlobId;
            const ui32 group = snapshot->TabletInfo->GroupFor(id.Channel(), id.Generation());
            if (group == Max<ui32>()) {
                co_return out;
            }

            auto batchIt = mapFromGroupToBatch.find(group);
            if (batchIt == mapFromGroupToBatch.end()) {
                batchIt = mapFromGroupToBatch.emplace(group, batches.size()).first;
                TGetBatch batch;
                batch.GroupId = group;
                batches.push_back(std::move(batch));
            }
            batches[batchIt->second].ReadItemIndecies.push_back(readItemIdx);
        }

        auto& ctx = NActors::TActivationContext::AsActorContext();
        for (auto& batch : batches) {
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> readQueries(
                new TEvBlobStorage::TEvGet::TQuery[batch.ReadItemIndecies.size()]);
            for (ui32 readQueryIdx = 0; readQueryIdx < batch.ReadItemIndecies.size(); ++readQueryIdx) {
                const ui32 readItemIdx = batch.ReadItemIndecies[readQueryIdx];
                auto& readItem = *readItems[readItemIdx].ReadItem;
                readQueries[readQueryIdx].Set(readItem.LogoBlobId, readItem.BlobOffset, readItem.BlobSize);
                readItem.InFlight = true;
            }

            auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
                readQueries,
                batch.ReadItemIndecies.size(),
                intermediate->Deadline,
                response.HandleClass,
                false);
            ev->ReaderTabletData = {snapshot->TabletId, static_cast<ui32>(snapshot->ChannelGeneration)};

            NBlobStorage::NDSProxy::NTask::TReadTaskArgs dsProxyTaskArgs;
            dsProxyTaskArgs.GroupId = batch.GroupId;
            dsProxyTaskArgs.Request.Event = std::move(ev);
            dsProxyTaskArgs.Request.Source = ctx.SelfID;
            auto result = co_await NBlobStorage::NDSProxy::NTask::RunReadTask(std::move(dsProxyTaskArgs));
            if (!result) {
                co_return out;
            }
            if (result->Status != NKikimrProto::OK || result->ResponseSz != batch.ReadItemIndecies.size()) {
                co_return out;
            }

            for (ui32 readQueryIdx = 0; readQueryIdx < batch.ReadItemIndecies.size(); ++readQueryIdx) {
                const ui32 readItemIdx = batch.ReadItemIndecies[readQueryIdx];
                auto& itemInfo = readItems[readItemIdx];
                auto& read = *itemInfo.Read;
                auto& readItem = *itemInfo.ReadItem;
                auto& itemResponse = result->Responses[readQueryIdx];
                read.Status = itemResponse.Status;
                readItem.Status = itemResponse.Status;
                readItem.InFlight = false;

                if (itemResponse.Status != NKikimrProto::OK) {
                    co_return out;
                }

                if (itemResponse.Buffer.size() != readItem.BlobSize) {
                    co_return out;
                }
                if (readItem.ValueOffset + readItem.BlobSize > read.ValueSize) {
                    co_return out;
                }

                read.Value.Write(readItem.ValueOffset, std::move(itemResponse.Buffer));
            }
        }
    }

    if (!IsSnapshotActual(snapshot)) {
        co_return out;
    }

    out.Response = BuildReadResponse(
        intermediate,
        args.RespondTo,
        intermediate->IsTruncated ? NKikimrKeyValue::Statuses::RSTATUS_OVERRUN
                                  : NKikimrKeyValue::Statuses::RSTATUS_OK);
    out.NeedsFallback = false;
    co_return out;
}

} // namespace NKikimr::NKeyValue::NTask
