#include "keyvalue_task_read_prepare.h"
#include "keyvalue_const.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/keyvalue/keyvalue_events.h>

#include <util/charset/utf8.h>
#include <util/generic/algorithm.h>
#include <util/string/escape.h>

namespace NKikimr::NKeyValue::NTask {

namespace {

constexpr ui64 KeyValuePairSizeEstimationNewApi = 1 + 5 // Key id, length
    + 1 + 5 // Value id, length
    + 1 + 4 // ValueSize id, value
    + 1 + 8 // CreationUnixTime id, value
    + 1 + 4 // StorageChannel id, value
    + 1 + 1 // Status id, value
    ;

constexpr ui64 KeyInfoSizeEstimation = 1 + 5 // Key id, length
    + 1 + 4 // ValueSize id, value
    + 1 + 8 // CreationUnixTime id, value
    + 1 + 4 // StorageChannel id, value
    ;

constexpr ui64 ReadRangeRequestMetaDataSizeEstimation = 1 + 5 // pair id, length
    + 1 + 1 // Status id, value
    ;

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
    for (ui64 idx = 0; idx < indexRecord.Chain.size(); ++idx) {
        if (!indexRecord.Chain[idx].IsInline()) {
            outIsInlineOnly = false;
            break;
        }
    }

    if (!size) {
        size = std::numeric_limits<decltype(size)>::max();
    }
    ui64 fullValueSize = indexRecord.GetFullValueSize();
    offset = std::min(offset, fullValueSize);
    size = std::min(size, fullValueSize - offset);
    ui64 metaDataSize = key.size() + SpecificReadResultSizeEstimation;
    ui64 recSize = std::max(size, ErrorMessageSizeEstimation) + metaDataSize;

    response.RequestedSize = size;
    bool isOverRun = false;

    if (intermediate->IsTruncated
            || intermediate->TotalSize + recSize > intermediate->TotalSizeLimit
            || (cmdLimitBytes && intermediate->TotalSize + recSize > cmdLimitBytes)) {
        response.Status = NKikimrProto::OVERRUN;
        if (!WithOverrun || std::min(intermediate->TotalSizeLimit, cmdLimitBytes) < intermediate->TotalSize + metaDataSize) {
            return true;
        }
        if (cmdLimitBytes) {
            size = std::min(intermediate->TotalSizeLimit, cmdLimitBytes) - intermediate->TotalSize - metaDataSize;
        } else {
            size = intermediate->TotalSizeLimit - intermediate->TotalSize - metaDataSize;
        }
        isOverRun = true;
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
    return isOverRun;
}

template <typename Container, typename Iterator = typename Container::const_iterator>
std::pair<Iterator, Iterator> GetRange(const TKeyRange& range, const Container& container) {
    auto first = !range.HasFrom ? container.begin()
                                : range.IncludeFrom ? container.lower_bound(range.KeyFrom)
                                                    : container.upper_bound(range.KeyFrom);

    auto last = !range.HasTo ? container.end()
                             : range.IncludeTo ? container.upper_bound(range.KeyTo)
                                               : container.lower_bound(range.KeyTo);

    return {first, last};
}

struct TSeqInfo {
    ui32 Reads = 0;
    ui32 RunLen = 0;
    ui32 Generation = 0;
    ui32 Step = 0;
    ui32 Cookie = 0;
};

template <typename TypeWithPriority, ui64 SpecificKeyValuePairSizeEstimation>
bool PrepareOneReadFromRangeReadWithData(const TString &key, const TReadIndexRecordSnapshot &indexRecord, ui8 priority,
        THolder<TIntermediate> &intermediate, TIntermediate::TRangeRead &response, ui64 &cmdSizeBytes,
        ui64 cmdLimitBytes, TSeqInfo &seq, bool *outIsInlineOnly)
{
    if (intermediate->IsTruncated) {
        return false;
    }

    NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel = NKikimrClient::TKeyValueRequest::MAIN;
    if (!indexRecord.Chain.empty()) {
        if (indexRecord.Chain[0].IsInline()) {
            storageChannel = NKikimrClient::TKeyValueRequest::INLINE;
        } else {
            *outIsInlineOnly = false;
            ui32 storageChannelIdx = indexRecord.Chain[0].LogoBlobId.Channel();
            ui32 storageChannelOffset = storageChannelIdx - BLOB_CHANNEL;
            storageChannel = static_cast<NKikimrClient::TKeyValueRequest::EStorageChannel>(storageChannelOffset);
        }
    }

    bool isSeq = false;
    bool isInline = false;
    if (indexRecord.Chain.size() == 1) {
        if (indexRecord.Chain.front().IsInline()) {
            isSeq = true;
            isInline = true;
        } else {
            const TLogoBlobID& id = indexRecord.Chain.front().LogoBlobId;
            isSeq = id.Generation() == seq.Generation
                && id.Step() == seq.Step
                && id.Cookie() == seq.Cookie;
            seq.Generation = id.Generation();
            seq.Step = id.Step();
            seq.Cookie = id.Cookie() + 1;
        }
    }
    if (isSeq) {
        seq.Reads++;
        if (seq.Reads > intermediate->SequentialReadLimit && !isInline) {
            isSeq = false;
        } else {
            ++seq.RunLen;
        }
    }
    if (!isSeq) {
        seq.RunLen = 1;
    }

    ui64 valueSize = indexRecord.GetFullValueSize();
    ui64 metadataSize = key.size() + SpecificKeyValuePairSizeEstimation;
    if (intermediate->TotalSize + valueSize + metadataSize > intermediate->TotalSizeLimit
            || cmdSizeBytes + valueSize + metadataSize > cmdLimitBytes
            || (seq.RunLen == 1 && intermediate->TotalReadsScheduled >= intermediate->TotalReadsLimit)) {
        return true;
    }

    TIntermediate::TRead read(key, valueSize, indexRecord.CreationUnixTime, storageChannel);
    const ui32 numReads = indexRecord.GetReadItems(0, valueSize, read);
    SetPriority<TypeWithPriority>(&response.HandleClass, priority);
    SetPriority<TypeWithPriority>(&read.HandleClass, priority);

    response.Reads.push_back(std::move(read));

    intermediate->TotalSize += valueSize + metadataSize;
    intermediate->TotalReadsScheduled += numReads;

    cmdSizeBytes += valueSize + metadataSize;
    return false;
}

template <typename TypeWithPriority, ui64 SpecificKeyValuePairSizeEstimation>
bool PrepareOneReadFromRangeReadWithoutData(const TString &key, const TReadIndexRecordSnapshot &indexRecord,
        ui8 priority, THolder<TIntermediate> &intermediate, TIntermediate::TRangeRead &response,
        ui64 &cmdSizeBytes, ui64 cmdLimitBytes, bool *outIsInlineOnly)
{
    if (intermediate->IsTruncated) {
        return false;
    }

    NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel = NKikimrClient::TKeyValueRequest::MAIN;
    if (!indexRecord.Chain.empty()) {
        if (indexRecord.Chain[0].IsInline()) {
            storageChannel = NKikimrClient::TKeyValueRequest::INLINE;
        } else {
            *outIsInlineOnly = false;
            ui32 storageChannelIdx = indexRecord.Chain[0].LogoBlobId.Channel();
            ui32 storageChannelOffset = storageChannelIdx - BLOB_CHANNEL;
            storageChannel = static_cast<NKikimrClient::TKeyValueRequest::EStorageChannel>(storageChannelOffset);
        }
    }

    ui64 metadataSize = key.size() + SpecificKeyValuePairSizeEstimation;
    if (intermediate->TotalSize + metadataSize > intermediate->TotalSizeLimit
            || cmdSizeBytes + metadataSize > cmdLimitBytes) {
        return true;
    }

    response.Reads.emplace_back(key, indexRecord.GetFullValueSize(), indexRecord.CreationUnixTime, storageChannel);
    intermediate->TotalSize += metadataSize;
    SetPriority<TypeWithPriority>(&response.HandleClass, priority);

    cmdSizeBytes += metadataSize;
    return false;
}

bool ConvertRange(const NKikimrKeyValue::KVRange& range, TKeyRange *to) {
    if (range.has_from_key_inclusive()) {
        to->HasFrom = true;
        to->KeyFrom = range.from_key_inclusive();
        to->IncludeFrom = true;
    } else if (range.has_from_key_exclusive()) {
        to->HasFrom = true;
        to->KeyFrom = range.from_key_exclusive();
        to->IncludeFrom = false;
    } else {
        to->HasFrom = false;
    }

    if (range.has_to_key_inclusive()) {
        to->HasTo = true;
        to->KeyTo = range.to_key_inclusive();
        to->IncludeTo = true;
    } else if (range.has_to_key_exclusive()) {
        to->HasTo = true;
        to->KeyTo = range.to_key_exclusive();
        to->IncludeTo = false;
    } else {
        to->HasTo = false;
    }

    if (to->HasFrom && to->HasTo) {
        if (!to->IncludeFrom && !to->IncludeTo && to->KeyFrom >= to->KeyTo) {
            return false;
        }
        if (to->KeyFrom > to->KeyTo) {
            return false;
        }
    }

    return true;
}

template <bool CheckUtf8, ui64 MetaDataSizeWithData = KeyValuePairSizeEstimationNewApi, ui64 MetaDataSizeWithoutData = KeyInfoSizeEstimation>
void ProcessOneCmdReadRange(const TReadSharedSnapshot& snapshot, const TKeyRange &range, ui64 cmdLimitBytes,
        bool includeData, ui8 priority, TIntermediate::TRangeRead &response, THolder<TIntermediate> &intermediate,
        bool *outIsInlineOnly)
{
    ui64 cmdSizeBytes = 0;
    TSeqInfo seq;
    seq.RunLen = 1;

    const auto [first, last] = GetRange(range, snapshot.Index);
    for (auto it = first; it != last; ++it) {
        if (intermediate->IsTruncated) {
            break;
        }

        const TString& key = it->first;
        const auto& indexRecord = it->second;

        if constexpr (CheckUtf8) {
            if (!IsUtf(key)) {
                TIntermediate::TRead read;
                read.CreationUnixTime = indexRecord.CreationUnixTime;
                EscapeC(key, read.Key);
                read.Status = NKikimrProto::ERROR;
                read.Message = "Key isn't UTF8";
                response.Reads.push_back(std::move(read));
                continue;
            }
        }

        bool isOverRun = includeData
            ? PrepareOneReadFromRangeReadWithData<NKikimrKeyValue::Priorities, MetaDataSizeWithData>(
                key, indexRecord, priority, intermediate, response, cmdSizeBytes, cmdLimitBytes, seq, outIsInlineOnly)
            : PrepareOneReadFromRangeReadWithoutData<NKikimrKeyValue::Priorities, MetaDataSizeWithoutData>(
                key, indexRecord, priority, intermediate, response, cmdSizeBytes, cmdLimitBytes, outIsInlineOnly);

        if (isOverRun) {
            intermediate->IsTruncated = true;
        }
    }

    if (intermediate->IsTruncated) {
        response.Status = NKikimrProto::OVERRUN;
    } else if (response.Reads.empty()) {
        response.Status = NKikimrProto::NODATA;
    }
}

THolder<TIntermediate> MakeReadIntermediate(const TPrepareReadTaskCommonArgs& common, ui32 evType) {
    auto intermediate = MakeHolder<TIntermediate>(
        common.RespondTo,
        common.KeyValueActorId,
        common.Snapshot->ChannelGeneration,
        common.Snapshot->ChannelStep,
        TRequestType::ReadOnly,
        NWilson::TTraceId(common.TraceId));
    intermediate->EvType = evType;
    intermediate->UsePayloadInResponse = common.UsePayloadInResponse;
    return intermediate;
}

template <typename TRequest>
bool CheckDeadline(const TRequest& request, THolder<TIntermediate>& intermediate) {
    const ui64 deadlineInstantMs = request.deadline_instant_ms();
    if (!deadlineInstantMs) {
        return false;
    }

    intermediate->Deadline = TInstant::MicroSeconds(deadlineInstantMs * 1000ull);

    const TInstant now = TAppData::TimeProvider->Now();
    return intermediate->Deadline <= now;
}

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
        ui32 readSize = Min<ui64>(size, it->GetSize() - offset);
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

NActors::NTask::task<TPrepareReadTaskResult> RunPrepareReadTask(TPrepareReadTaskArgs args) {
    TPrepareReadTaskResult result;
    if (!IsSnapshotActual(args.Common.Snapshot)) {
        co_return result;
    }

    auto intermediate = MakeReadIntermediate(args.Common, TEvKeyValue::TEvRead::EventType);
    auto& request = args.Request;

    intermediate->HasCookie = true;
    intermediate->Cookie = request.cookie();

    intermediate->ReadCommand = TIntermediate::TRead();
    auto& response = std::get<TIntermediate::TRead>(*intermediate->ReadCommand);
    response.Key = request.key();

    if (CheckDeadline(request, intermediate)) {
        co_return result;
    }

    if (request.has_lock_generation() && request.lock_generation() != args.Common.Snapshot->UserGeneration) {
        co_return result;
    }

    auto it = args.Common.Snapshot->Index.find(request.key());
    if (it == args.Common.Snapshot->Index.end()) {
        co_return result;
    }

    bool isInlineOnly = true;
    bool isOverrun = PrepareOneRead<NKikimrKeyValue::Priorities, true>(
        it->first,
        it->second,
        request.offset(),
        request.size(),
        request.priority(),
        request.limit_bytes(),
        intermediate,
        response,
        isInlineOnly);

    intermediate->IsTruncated = isOverrun;

    if (!IsSnapshotActual(args.Common.Snapshot)) {
        co_return result;
    }

    result.NeedsFallback = false;
    result.RequestType = isInlineOnly ? TRequestType::ReadOnlyInline : TRequestType::ReadOnly;
    result.Intermediate = std::move(intermediate);
    co_return result;
}

NActors::NTask::task<TPrepareReadTaskResult> RunPrepareReadRangeTask(TPrepareReadRangeTaskArgs args) {
    TPrepareReadTaskResult result;
    if (!IsSnapshotActual(args.Common.Snapshot)) {
        co_return result;
    }

    auto intermediate = MakeReadIntermediate(args.Common, TEvKeyValue::TEvReadRange::EventType);
    auto& request = args.Request;

    intermediate->HasCookie = true;
    intermediate->Cookie = request.cookie();
    intermediate->TotalSize = ReadRangeRequestMetaDataSizeEstimation;

    intermediate->ReadCommand = TIntermediate::TRangeRead();
    auto& response = std::get<TIntermediate::TRangeRead>(*intermediate->ReadCommand);

    if (CheckDeadline(request, intermediate)) {
        co_return result;
    }

    if (request.has_lock_generation() && request.lock_generation() != args.Common.Snapshot->UserGeneration) {
        co_return result;
    }

    TKeyRange range;
    if (!ConvertRange(request.range(), &range)) {
        co_return result;
    }

    ui64 cmdLimitBytes = request.limit_bytes();
    if (!cmdLimitBytes) {
        cmdLimitBytes = Max<ui64>();
    }
    const bool includeData = request.include_data();
    const ui8 priority = request.priority();

    response.Status = NKikimrProto::OK;
    response.LimitBytes = cmdLimitBytes;
    response.IncludeData = includeData;

    bool isInlineOnly = true;
    ProcessOneCmdReadRange<true>(
        *args.Common.Snapshot,
        range,
        cmdLimitBytes,
        includeData,
        priority,
        response,
        intermediate,
        &isInlineOnly);

    if (!IsSnapshotActual(args.Common.Snapshot)) {
        co_return result;
    }

    result.NeedsFallback = false;
    result.RequestType = isInlineOnly ? TRequestType::ReadOnlyInline : TRequestType::ReadOnly;
    result.Intermediate = std::move(intermediate);
    co_return result;
}

} // namespace NKikimr::NKeyValue::NTask
