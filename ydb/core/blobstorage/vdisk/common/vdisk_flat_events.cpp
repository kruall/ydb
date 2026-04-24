#include "vdisk_flat_events.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>

namespace NKikimr {

    namespace {

        template <class TArray, class TValue>
        void AppendFlatArrayItem(TArray array, const TValue& value) {
            TVector<TValue> items(array.size() + 1);
            for (size_t i = 0; i < array.size(); ++i) {
                items[i] = array.Get(i);
            }
            items.back() = value;
            array.CopyFrom(items.data(), items.size());
        }

        NVDiskFlat::TTimestampsRaw MakeEmptyTimestampsRaw() {
            return {};
        }

    } // anonymous namespace

    TEvBlobStorage::TEvVPutFlat* TEvBlobStorage::TEvVPutFlat::MakeSinglePut(const TLogoBlobID& logoBlobId, TRope buffer,
            const TVDiskID& vdisk, bool ignoreBlock, const ui64 *cookie, TInstant deadline,
            NKikimrBlobStorage::EPutHandleClass cls, bool checksumming) {
        THolder<TEvVPutFlat> holder(TBase::MakeEvent<TSinglePutV1>());
        auto frontend = holder->GetFrontend<TSinglePutV1>();

        frontend.template Field<TVDiskIdTag>() = NVDiskFlat::ToRaw(vdisk);
        frontend.template Field<TMsgQoSTag>() = NVDiskFlat::MakeMsgQoSRaw(deadline, HandleClassToQueueId(cls));
        frontend.template Field<THandleClassTag>() = static_cast<ui32>(cls);
        frontend.template Field<TTimestampsTag>() = MakeEmptyTimestampsRaw();
        frontend.template Field<TBlobIdTag>() = NVDiskFlat::ToRaw(logoBlobId);
        frontend.template Field<TFullDataSizeTag>() = logoBlobId.BlobSize();
        frontend.template Field<TCookieTag>() = cookie ? *cookie : 0;
        frontend.template Field<TChecksumTag>() = checksumming ? TDiskBlob::CalculateChecksum(buffer) : 0;

        NVDiskFlat::TPutFlagsRaw flags;
        flags.SetIgnoreBlock(ignoreBlock);
        flags.SetHasCookie(cookie != nullptr);
        flags.SetHasChecksum(checksumming);
        frontend.template Field<TFlagsTag>() = flags;
        frontend.template Bytes<TPayloadTag>().Set(std::move(buffer));

        return holder.Release();
    }

    TEvBlobStorage::TEvVPutFlat* TEvBlobStorage::TEvVPutFlat::MakeMultiPut(const TVDiskID& vdisk, TInstant deadline,
            NKikimrBlobStorage::EPutHandleClass cls, bool ignoreBlock, const ui64 *cookie) {
        THolder<TEvVPutFlat> holder(TBase::MakeEvent<TMultiPutV1>());
        auto frontend = holder->GetFrontend<TMultiPutV1>();

        frontend.template Field<TVDiskIdTag>() = NVDiskFlat::ToRaw(vdisk);
        frontend.template Field<TMsgQoSTag>() = NVDiskFlat::MakeMsgQoSRaw(deadline, HandleClassToQueueId(cls));
        frontend.template Field<THandleClassTag>() = static_cast<ui32>(cls);
        frontend.template Field<TTimestampsTag>() = MakeEmptyTimestampsRaw();
        frontend.template Field<TCookieTag>() = cookie ? *cookie : 0;

        NVDiskFlat::TPutFlagsRaw flags;
        flags.SetIgnoreBlock(ignoreBlock);
        flags.SetHasCookie(cookie != nullptr);
        frontend.template Field<TFlagsTag>() = flags;

        return holder.Release();
    }

    void TEvBlobStorage::TEvVPutFlat::AddVPut(const TLogoBlobID& logoBlobId, const TRcBuf& buffer, ui64 *cookie,
            bool issueKeepFlag, bool ignoreBlock, bool isZeroEntry,
            std::vector<std::pair<ui64, ui32>> *extraBlockChecks, NWilson::TTraceId traceId, bool checksumming) {
        Y_UNUSED(traceId);
        Y_ENSURE(IsMultiPut(), "AddVPut is only available for TEvVPutFlat multi-put scheme");

        TRope rope(buffer);
        auto frontend = GetFrontend<TMultiPutV1>();
        const ui64 payloadOffset = frontend.template GetSize<TPayloadTag>();
        const ui64 payloadSize = rope.GetSize();

        NVDiskFlat::TPutItemRaw item;
        item.BlobId = NVDiskFlat::ToRaw(logoBlobId);
        item.FullDataSize = logoBlobId.BlobSize();
        item.Cookie = cookie ? *cookie : 0;
        item.Checksum = checksumming ? TDiskBlob::CalculateChecksum(rope) : 0;
        item.PayloadOffset = payloadOffset;
        item.PayloadSize = payloadSize;
        item.ExtraBlockChecksOffset = frontend.template ArraySize<TExtraBlockChecksTag>();
        item.ExtraBlockChecksCount = extraBlockChecks ? extraBlockChecks->size() : 0;
        item.Flags.SetIssueKeepFlag(issueKeepFlag);
        item.Flags.SetIgnoreBlock(ignoreBlock);
        item.Flags.SetIsZeroEntry(isZeroEntry);
        item.Flags.SetHasCookie(cookie != nullptr);
        item.Flags.SetHasChecksum(checksumming);

        if (extraBlockChecks) {
            auto checks = frontend.template Array<TExtraBlockChecksTag>();
            TVector<NVDiskFlat::TExtraBlockCheckRaw> values(checks.size() + extraBlockChecks->size());
            for (size_t i = 0; i < checks.size(); ++i) {
                values[i] = checks.Get(i);
            }
            for (size_t i = 0; i < extraBlockChecks->size(); ++i) {
                const auto& [tabletId, generation] = (*extraBlockChecks)[i];
                values[checks.size() + i] = {.TabletId = tabletId, .Generation = generation};
            }
            checks.CopyFrom(values.data(), values.size());
        }

        AppendFlatArrayItem(frontend.template Array<TItemsTag>(), item);
        frontend.template Bytes<TPayloadTag>().Append(std::move(rope));
    }

    ui64 TEvBlobStorage::TEvVPutFlat::GetBufferBytes() const {
        if (IsSinglePut()) {
            return GetSize<TPayloadTag>();
        }
        ui64 bytes = 0;
        auto frontend = GetFrontend<TMultiPutV1>();
        auto items = frontend.template Array<TItemsTag>();
        for (size_t i = 0; i < items.size(); ++i) {
            bytes += items.Get(i).PayloadSize;
        }
        return bytes;
    }

    ui64 TEvBlobStorage::TEvVPutFlat::GetBufferBytes(ui64 idx) const {
        Y_ENSURE(IsMultiPut(), "Indexed payload size is only available for TEvVPutFlat multi-put scheme");
        auto items = GetFrontend<TMultiPutV1>().template Array<TItemsTag>();
        Y_ENSURE(idx < items.size(), "Put item index is out of range");
        return items.Get(idx).PayloadSize;
    }

    TRope TEvBlobStorage::TEvVPutFlat::GetBuffer() const {
        Y_ENSURE(IsSinglePut(), "GetBuffer is only available for TEvVPutFlat single-put scheme");
        return Bytes<TPayloadTag>().Rope();
    }

    TRope TEvBlobStorage::TEvVPutFlat::GetItemBuffer(ui64 itemIdx) const {
        Y_ENSURE(IsMultiPut(), "GetItemBuffer is only available for TEvVPutFlat multi-put scheme");
        auto frontend = GetFrontend<TMultiPutV1>();
        auto items = frontend.template Array<TItemsTag>();
        Y_ENSURE(itemIdx < items.size(), "Put item index is out of range");
        const auto item = items.Get(itemIdx);
        const TRope& payload = frontend.template Bytes<TPayloadTag>().Rope();
        return TRope(payload.Position(item.PayloadOffset), payload.Position(item.PayloadOffset + item.PayloadSize));
    }

    ui64 TEvBlobStorage::TEvVPutFlat::GetSumBlobSize() const {
        if (IsSinglePut()) {
            return NVDiskFlat::FromRaw(GetFrontend<TSinglePutV1>().template Field<TBlobIdTag>()).BlobSize();
        }
        ui64 sum = 0;
        auto items = GetFrontend<TMultiPutV1>().template Array<TItemsTag>();
        for (size_t i = 0; i < items.size(); ++i) {
            sum += NVDiskFlat::FromRaw(items.Get(i).BlobId).BlobSize();
        }
        return sum;
    }

    bool TEvBlobStorage::TEvVPutFlat::Validate(TString& errorReason) const {
        if (IsSinglePut()) {
            auto frontend = GetFrontend<TSinglePutV1>();
            if (NVDiskFlat::FromRaw(frontend.template Field<TBlobIdTag>()) == TLogoBlobID()) {
                errorReason = "TEvVPutFlat rejected by VDisk. It has no query";
            } else if (GetVDiskID().GroupID == TGroupId::Zero()) {
                errorReason = "TEvVPutFlat rejected by VDisk. It has no VDiskID::GroupID";
            } else if (GetExtQueueId() == NKikimrBlobStorage::EVDiskQueueId::Unknown) {
                errorReason = "TEvVPutFlat rejected by VDisk. ExtQueueId is undefined";
            } else if (GetSize<TPayloadTag>() == 0) {
                errorReason = "TEvVPutFlat rejected by VDisk. Payload empty and no buffer provided";
            } else {
                return true;
            }
        } else if (IsMultiPut()) {
            auto frontend = GetFrontend<TMultiPutV1>();
            if (frontend.template ArraySize<TItemsTag>() == 0) {
                errorReason = "TEvVPutFlat rejected by VDisk. It has 0 blobs to put";
            } else if (GetVDiskID().GroupID == TGroupId::Zero()) {
                errorReason = "TEvVPutFlat rejected by VDisk. It has no VDiskID::GroupID";
            } else if (GetExtQueueId() == NKikimrBlobStorage::EVDiskQueueId::Unknown) {
                errorReason = "TEvVPutFlat rejected by VDisk. ExtQueueId is undefined";
            } else {
                return true;
            }
        } else {
            errorReason = "TEvVPutFlat rejected by VDisk. Unknown schema";
        }

        return false;
    }

    TString TEvBlobStorage::TEvVPutFlat::ToString() const {
        TStringStream str;
        str << "{EvVPutFlat";
        if (IsSinglePut()) {
            auto frontend = GetFrontend<TSinglePutV1>();
            const TLogoBlobID id = NVDiskFlat::FromRaw(frontend.template Field<TBlobIdTag>());
            const auto flags = static_cast<NVDiskFlat::TPutFlagsRaw>(frontend.template Field<TFlagsTag>());
            str << " Single ID# " << id.ToString()
                << " FullDataSize# " << static_cast<ui64>(frontend.template Field<TFullDataSizeTag>())
                << " DataSize# " << GetBufferBytes();
            if (flags.GetIgnoreBlock()) {
                str << " IgnoreBlock";
            }
        } else if (IsMultiPut()) {
            str << " Multi Items# " << GetFrontend<TMultiPutV1>().template ArraySize<TItemsTag>()
                << " DataSize# " << GetBufferBytes();
        }
        str << " VDiskId# " << GetVDiskID()
            << " HandleClass# " << GetHandleClass()
            << "}";
        return str.Str();
    }

    TEvBlobStorage::TEvVPutResultFlat* TEvBlobStorage::TEvVPutResultFlat::MakeSinglePutResult(
            NKikimrProto::EReplyStatus status, const TLogoBlobID& logoBlobId, const TVDiskID& vdisk,
            const ui64 *cookie, TOutOfSpaceStatus oosStatus, ui64 incarnationGuid, const TString& errorReason) {
        THolder<TEvVPutResultFlat> holder(TBase::MakeEvent<TSinglePutResultV1>());
        auto frontend = holder->GetFrontend<TSinglePutResultV1>();

        frontend.template Field<TStatusTag>() = static_cast<ui32>(status);
        frontend.template Field<TVDiskIdTag>() = NVDiskFlat::ToRaw(vdisk);
        frontend.template Field<TBlobIdTag>() = NVDiskFlat::ToRaw(logoBlobId);
        frontend.template Field<TCookieTag>() = cookie ? *cookie : 0;
        frontend.template Field<TStatusFlagsTag>() = oosStatus.Flags;
        frontend.template Field<TFreeSpaceShareTag>() = oosStatus.ApproximateFreeSpaceShare;
        frontend.template Field<TIncarnationGuidTag>() = status == NKikimrProto::OK ? incarnationGuid : 0;
        frontend.template Field<TTimestampsTag>() = MakeEmptyTimestampsRaw();
        frontend.template Field<TMsgQoSTag>() = {};
        frontend.template Bytes<TErrorReasonTag>().Set(TRope(errorReason));

        NVDiskFlat::TPutResultFlagsRaw flags;
        flags.SetHasCookie(cookie != nullptr);
        frontend.template Field<TFlagsTag>() = flags;

        return holder.Release();
    }

    TEvBlobStorage::TEvVPutResultFlat* TEvBlobStorage::TEvVPutResultFlat::MakeMultiPutResult(
            NKikimrProto::EReplyStatus status, const TVDiskID& vdisk, const ui64 *cookie,
            TOutOfSpaceStatus oosStatus, ui64 incarnationGuid, const TString& errorReason) {
        THolder<TEvVPutResultFlat> holder(TBase::MakeEvent<TMultiPutResultV1>());
        auto frontend = holder->GetFrontend<TMultiPutResultV1>();

        frontend.template Field<TStatusTag>() = static_cast<ui32>(status);
        frontend.template Field<TVDiskIdTag>() = NVDiskFlat::ToRaw(vdisk);
        frontend.template Field<TCookieTag>() = cookie ? *cookie : 0;
        frontend.template Field<TStatusFlagsTag>() = oosStatus.Flags;
        frontend.template Field<TFreeSpaceShareTag>() = oosStatus.ApproximateFreeSpaceShare;
        frontend.template Field<TIncarnationGuidTag>() = status == NKikimrProto::OK ? incarnationGuid : 0;
        frontend.template Field<TTimestampsTag>() = MakeEmptyTimestampsRaw();
        frontend.template Field<TMsgQoSTag>() = {};
        frontend.template Bytes<TErrorReasonTag>().Set(TRope(errorReason));

        NVDiskFlat::TPutResultFlagsRaw flags;
        flags.SetHasCookie(cookie != nullptr);
        frontend.template Field<TFlagsTag>() = flags;

        return holder.Release();
    }

    void TEvBlobStorage::TEvVPutResultFlat::AddVPutResult(NKikimrProto::EReplyStatus status, const TString& errorReason,
            const TLogoBlobID& logoBlobId, ui64 *cookie, ui32 statusFlags, bool writtenBeyondBarrier) {
        Y_ENSURE(IsMultiPutResult(), "AddVPutResult is only available for TEvVPutResultFlat multi-put scheme");

        auto frontend = GetFrontend<TMultiPutResultV1>();
        NVDiskFlat::TPutResultItemRaw item;
        item.BlobId = NVDiskFlat::ToRaw(logoBlobId);
        item.Cookie = cookie ? *cookie : 0;
        item.Status = static_cast<ui32>(status);
        item.StatusFlags = statusFlags;
        item.ErrorReasonOffset = frontend.template GetSize<TErrorReasonTag>();
        item.ErrorReasonSize = errorReason.size();
        item.Flags.SetHasCookie(cookie != nullptr);
        item.Flags.SetWrittenBeyondBarrier(writtenBeyondBarrier);

        AppendFlatArrayItem(frontend.template Array<TItemsTag>(), item);
        frontend.template Bytes<TErrorReasonTag>().Append(TRope(errorReason));
    }

    TString TEvBlobStorage::TEvVPutResultFlat::ToString() const {
        TStringStream str;
        str << "{EvVPutResultFlat Status# " << NKikimrProto::EReplyStatus_Name(GetStatus());
        if (IsSinglePutResult()) {
            str << " Single ID# " << NVDiskFlat::FromRaw(GetFrontend<TSinglePutResultV1>().template Field<TBlobIdTag>()).ToString();
        } else if (IsMultiPutResult()) {
            str << " Multi Items# " << GetFrontend<TMultiPutResultV1>().template ArraySize<TItemsTag>();
        }
        str << "}";
        return str.Str();
    }

    namespace {

        NVDiskFlat::TGetFlagsRaw MakeGetFlags(bool notifyIfNotReady, bool showInternals, TMaybe<ui64> requestCookie,
                bool indexOnly, std::optional<TEvBlobStorage::TEvVGetFlat::TForceBlockTabletData> forceBlockTabletData) {
            NVDiskFlat::TGetFlagsRaw flags;
            flags.SetNotifyIfNotReady(notifyIfNotReady);
            flags.SetShowInternals(showInternals);
            flags.SetHasCookie(requestCookie.Defined());
            flags.SetIndexOnly(indexOnly);
            flags.SetHasForceBlockTabletData(forceBlockTabletData.has_value());
            flags.SetEnablePayload(true);
            return flags;
        }

        template <class TFrontend>
        void InitVGetCommon(TFrontend frontend, const TVDiskID& vdisk, TInstant deadline,
                NKikimrBlobStorage::EGetHandleClass cls, TEvBlobStorage::TEvVGet::EFlags eventFlags,
                TMaybe<ui64> requestCookie, bool indexOnly,
                std::optional<TEvBlobStorage::TEvVGetFlat::TForceBlockTabletData> forceBlockTabletData) {
            const bool notifyIfNotReady = bool(ui32(eventFlags) & ui32(TEvBlobStorage::TEvVGet::EFlags::NotifyIfNotReady));
            const bool showInternals = bool(ui32(eventFlags) & ui32(TEvBlobStorage::TEvVGet::EFlags::ShowInternals));

            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TVDiskIdTag>() = NVDiskFlat::ToRaw(vdisk);
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TMsgQoSTag>() = NVDiskFlat::MakeMsgQoSRaw(deadline, HandleClassToQueueId(cls));
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::THandleClassTag>() = static_cast<ui32>(cls);
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TTimestampsTag>() = MakeEmptyTimestampsRaw();
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TFlagsTag>() = MakeGetFlags(notifyIfNotReady, showInternals,
                requestCookie, indexOnly, forceBlockTabletData);
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TCookieTag>() = requestCookie.GetOrElse(0);
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TTabletIdTag>() = 0;
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TForceBlockedGenerationTag>() = 0;
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TReaderTabletDataTag>() = {};
            frontend.template Field<TEvBlobStorage::TEvVGetFlat::TForceBlockTabletDataTag>() = forceBlockTabletData
                ? NVDiskFlat::TTabletDataRaw{.Id = forceBlockTabletData->Id, .Generation = forceBlockTabletData->Generation}
                : NVDiskFlat::TTabletDataRaw{};
        }

        template <class TFrontend>
        void AddExtremeQueryToFrontend(TFrontend frontend, const TLogoBlobID& logoBlobId, ui32 sh, ui32 sz, const ui64 *cookie) {
            NVDiskFlat::TExtremeQueryRaw item;
            item.BlobId = NVDiskFlat::ToRaw(logoBlobId);
            item.Shift = sh;
            item.Size = sz;
            item.Cookie = cookie ? *cookie : 0;
            item.Flags.SetHasCookie(cookie != nullptr);
            AppendFlatArrayItem(frontend.template Array<TEvBlobStorage::TEvVGetFlat::TExtremeQueriesTag>(), item);
        }

        template <class TFrontend>
        void AddVGetResultItem(TFrontend frontend, NVDiskFlat::TGetResultItemRaw item, TRope data) {
            item.PayloadOffset = frontend.template GetSize<TEvBlobStorage::TEvVGetResultFlat::TPayloadTag>();
            item.PayloadSize = data.GetSize();
            item.Flags.SetHasBlob(bool(data));
            AppendFlatArrayItem(frontend.template Array<TEvBlobStorage::TEvVGetResultFlat::TItemsTag>(), item);
            frontend.template Bytes<TEvBlobStorage::TEvVGetResultFlat::TPayloadTag>().Append(std::move(data));
        }

    } // anonymous namespace

    std::unique_ptr<TEvBlobStorage::TEvVGetFlat> TEvBlobStorage::TEvVGetFlat::CreateExtremeIndexQuery(
            const TVDiskID& vdisk, TInstant deadline, NKikimrBlobStorage::EGetHandleClass cls, TEvBlobStorage::TEvVGet::EFlags flags,
            TMaybe<ui64> requestCookie, std::initializer_list<TExtremeQuery> queries,
            std::optional<TForceBlockTabletData> forceBlockTabletData) {
        std::unique_ptr<TEvVGetFlat> res(TBase::MakeEvent<TExtremeIndexV1>());
        InitVGetCommon(res->GetFrontend<TExtremeIndexV1>(), vdisk, deadline, cls, flags, requestCookie, true, forceBlockTabletData);
        for (const auto& q : queries) {
            res->AddExtremeQuery(std::get<0>(q), std::get<1>(q), std::get<2>(q), std::get<3>(q));
        }
        return res;
    }

    std::unique_ptr<TEvBlobStorage::TEvVGetFlat> TEvBlobStorage::TEvVGetFlat::CreateExtremeDataQuery(
            const TVDiskID& vdisk, TInstant deadline, NKikimrBlobStorage::EGetHandleClass cls, TEvBlobStorage::TEvVGet::EFlags flags,
            TMaybe<ui64> requestCookie, std::initializer_list<TExtremeQuery> queries,
            std::optional<TForceBlockTabletData> forceBlockTabletData) {
        std::unique_ptr<TEvVGetFlat> res(TBase::MakeEvent<TExtremeDataV1>());
        auto frontend = res->GetFrontend<TExtremeDataV1>();
        InitVGetCommon(frontend, vdisk, deadline, cls, flags, requestCookie, false, forceBlockTabletData);
        frontend.template Field<TEvVGetFlatTExtremeDataMarkerTag>() = 1;
        for (const auto& q : queries) {
            res->AddExtremeQuery(std::get<0>(q), std::get<1>(q), std::get<2>(q), std::get<3>(q));
        }
        return res;
    }

    std::unique_ptr<TEvBlobStorage::TEvVGetFlat> TEvBlobStorage::TEvVGetFlat::CreateRangeIndexQuery(
            const TVDiskID& vdisk, TInstant deadline, NKikimrBlobStorage::EGetHandleClass cls, TEvBlobStorage::TEvVGet::EFlags flags,
            TMaybe<ui64> requestCookie, const TLogoBlobID& fromId, const TLogoBlobID& toId, ui32 maxResults,
            const ui64 *cookie, std::optional<TForceBlockTabletData> forceBlockTabletData) {
        std::unique_ptr<TEvVGetFlat> res(TBase::MakeEvent<TRangeIndexV1>());
        auto frontend = res->GetFrontend<TRangeIndexV1>();
        InitVGetCommon(frontend, vdisk, deadline, cls, flags, requestCookie, true, forceBlockTabletData);
        NVDiskFlat::TRangeQueryRaw query;
        query.From = NVDiskFlat::ToRaw(fromId);
        query.To = NVDiskFlat::ToRaw(toId);
        query.Cookie = cookie ? *cookie : 0;
        query.MaxResults = maxResults;
        query.Flags.SetHasCookie(cookie != nullptr);
        query.Flags.SetHasMaxResults(maxResults != 0);
        frontend.template Field<TRangeQueryTag>() = query;
        return res;
    }

    void TEvBlobStorage::TEvVGetFlat::AddExtremeQuery(const TLogoBlobID& logoBlobId, ui32 sh, ui32 sz, const ui64 *cookie) {
        Y_ENSURE(IsExtremeQuery(), "AddExtremeQuery is only available for TEvVGetFlat extreme query schemes");
        if (IsExtremeIndexQuery()) {
            AddExtremeQueryToFrontend(GetFrontend<TExtremeIndexV1>(), logoBlobId, sh, sz, cookie);
        } else {
            AddExtremeQueryToFrontend(GetFrontend<TExtremeDataV1>(), logoBlobId, sh, sz, cookie);
        }
    }

    bool TEvBlobStorage::TEvVGetFlat::Validate(TString& errorReason) const {
        if (IsExtremeQuery()) {
            const size_t queries = IsExtremeIndexQuery()
                ? GetFrontend<TExtremeIndexV1>().template ArraySize<TExtremeQueriesTag>()
                : GetFrontend<TExtremeDataV1>().template ArraySize<TExtremeQueriesTag>();
            if (queries == 0) {
                errorReason = "TEvVGetFlat rejected by VDisk. It has no query";
                return false;
            }
        }
        if (GetVDiskID().GroupID == TGroupId::Zero()) {
            errorReason = "TEvVGetFlat rejected by VDisk. It has no VDiskID::GroupID";
        } else if (GetExtQueueId() == NKikimrBlobStorage::EVDiskQueueId::Unknown) {
            errorReason = "TEvVGetFlat rejected by VDisk. ExtQueueId is undefined";
        } else {
            return true;
        }
        return false;
    }

    TString TEvBlobStorage::TEvVGetFlat::ToString() const {
        TStringStream str;
        str << "{EvVGetFlat";
        if (IsExtremeQuery()) {
            const bool index = IsExtremeIndexQuery();
            const auto frontendIndex = GetFrontend<TExtremeIndexV1>();
            const auto frontendData = GetFrontend<TExtremeDataV1>();
            const size_t size = index
                ? frontendIndex.template ArraySize<TExtremeQueriesTag>()
                : frontendData.template ArraySize<TExtremeQueriesTag>();
            str << (index ? " ExtremeIndex" : " ExtremeData") << " Items# " << size;
        } else if (IsRangeIndexQuery()) {
            const auto query = static_cast<NVDiskFlat::TRangeQueryRaw>(GetFrontend<TRangeIndexV1>().template Field<TRangeQueryTag>());
            str << " RangeIndex From# " << NVDiskFlat::FromRaw(query.From)
                << " To# " << NVDiskFlat::FromRaw(query.To);
        }
        str << " VDiskId# " << GetVDiskID()
            << " HandleClass# " << GetHandleClass()
            << "}";
        return str.Str();
    }

    TEvBlobStorage::TEvVGetResultFlat* TEvBlobStorage::TEvVGetResultFlat::Make(
            NKikimrProto::EReplyStatus status, const TVDiskID& vdisk, TMaybe<ui64> cookie, ui64 incarnationGuid) {
        THolder<TEvVGetResultFlat> holder(TBase::MakeEvent<TV1>());
        auto frontend = holder->GetFrontend<TV1>();

        frontend.template Field<TStatusTag>() = static_cast<ui32>(status);
        frontend.template Field<TVDiskIdTag>() = NVDiskFlat::ToRaw(vdisk);
        frontend.template Field<TCookieTag>() = cookie.GetOrElse(0);
        frontend.template Field<TMsgQoSTag>() = {};
        frontend.template Field<TBlockedGenerationTag>() = 0;
        frontend.template Field<TTimestampsTag>() = MakeEmptyTimestampsRaw();
        frontend.template Field<TIncarnationGuidTag>() = status == NKikimrProto::OK ? incarnationGuid : 0;

        NVDiskFlat::TGetResultFlagsRaw flags;
        flags.SetHasCookie(cookie.Defined());
        frontend.template Field<TFlagsTag>() = flags;

        return holder.Release();
    }

    void TEvBlobStorage::TEvVGetResultFlat::MarkRangeOverflow() {
        auto flags = static_cast<NVDiskFlat::TGetResultFlagsRaw>(Field<TFlagsTag>());
        flags.SetIsRangeOverflow(true);
        Field<TFlagsTag>() = flags;
    }

    void TEvBlobStorage::TEvVGetResultFlat::AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID& logoBlobId,
            ui64 sh, std::variant<TRope, ui32> dataOrSize, const ui64 *cookie, const ui64 *ingress, bool keep,
            bool doNotKeep) {
        auto frontend = GetFrontend<TV1>();
        TRope data;
        ui32 size = 0;
        std::visit(TOverloaded{
            [&](TRope& x) { size = x.size(); data = std::move(x); },
            [&](ui32& x) { size = x; }
        }, dataOrSize);

        NVDiskFlat::TGetResultItemRaw item;
        item.BlobId = NVDiskFlat::ToRaw(logoBlobId);
        item.Shift = sh;
        item.Size = size;
        item.FullDataSize = logoBlobId.BlobSize();
        item.Cookie = cookie ? *cookie : 0;
        item.Ingress = ingress ? *ingress : 0;
        item.Status = static_cast<ui32>(status);
        item.Flags.SetHasCookie(cookie != nullptr);
        item.Flags.SetHasIngress(ingress != nullptr);
        item.Flags.SetKeep(keep);
        item.Flags.SetDoNotKeep(doNotKeep);
        Y_DEBUG_ABORT_UNLESS(keep + doNotKeep <= 1);

        AddVGetResultItem(frontend, item, std::move(data));
    }

    void TEvBlobStorage::TEvVGetResultFlat::AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID& logoBlobId,
            const ui64 *cookie, const ui64 *ingress, const NMatrix::TVectorType *local, bool keep, bool doNotKeep) {
        auto frontend = GetFrontend<TV1>();
        NVDiskFlat::TGetResultItemRaw item;
        item.BlobId = NVDiskFlat::ToRaw(logoBlobId);
        item.FullDataSize = logoBlobId.BlobSize();
        item.Cookie = cookie ? *cookie : 0;
        item.Ingress = ingress ? *ingress : 0;
        item.Status = static_cast<ui32>(status);
        item.PartsOffset = frontend.template ArraySize<TPartsTag>();
        item.Flags.SetHasCookie(cookie != nullptr);
        item.Flags.SetHasIngress(ingress != nullptr);
        item.Flags.SetKeep(keep);
        item.Flags.SetDoNotKeep(doNotKeep);
        Y_DEBUG_ABORT_UNLESS(keep + doNotKeep <= 1);

        if (local) {
            auto parts = frontend.template Array<TPartsTag>();
            TVector<ui32> values(parts.size() + local->CountBits());
            for (size_t i = 0; i < parts.size(); ++i) {
                values[i] = parts.Get(i);
            }
            ui32 pos = parts.size();
            for (ui8 i = local->FirstPosition(); i != local->GetSize(); i = local->NextPosition(i)) {
                values[pos++] = i + 1;
            }
            item.PartsCount = pos - parts.size();
            parts.CopyFrom(values.data(), values.size());
        }

        AddVGetResultItem(frontend, item, {});
    }

    void TEvBlobStorage::TEvVGetResultFlat::MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
            const TEvVGetFlat& request) {
        auto frontend = GetFrontend<TV1>();
        frontend.template Field<TStatusTag>() = static_cast<ui32>(status);
        frontend.template Field<TVDiskIdTag>() = NVDiskFlat::ToRaw(request.GetVDiskID());
        frontend.template Bytes<TErrorReasonTag>().Set(TRope(errorReason));

        if (request.IsExtremeQuery()) {
            const NKikimrProto::EReplyStatus queryStatus = status != NKikimrProto::NOTREADY ? status : NKikimrProto::ERROR;
            if (request.IsExtremeIndexQuery()) {
                auto queries = request.GetFrontend<TEvVGetFlat::TExtremeIndexV1>().template Array<TEvVGetFlat::TExtremeQueriesTag>();
                for (size_t i = 0; i < queries.size(); ++i) {
                    const auto query = queries.Get(i);
                    ui64 cookie = query.Cookie;
                    AddResult(queryStatus, NVDiskFlat::FromRaw(query.BlobId), query.Shift, 0u,
                        query.Flags.HasCookie() ? &cookie : nullptr);
                }
            } else {
                auto queries = request.GetFrontend<TEvVGetFlat::TExtremeDataV1>().template Array<TEvVGetFlat::TExtremeQueriesTag>();
                for (size_t i = 0; i < queries.size(); ++i) {
                    const auto query = queries.Get(i);
                    ui64 cookie = query.Cookie;
                    AddResult(queryStatus, NVDiskFlat::FromRaw(query.BlobId), query.Shift, 0u,
                        query.Flags.HasCookie() ? &cookie : nullptr);
                }
            }
        }
    }

    bool TEvBlobStorage::TEvVGetResultFlat::HasBlob(const NVDiskFlat::TGetResultItemRaw& item) const {
        return item.Flags.HasBlob();
    }

    ui32 TEvBlobStorage::TEvVGetResultFlat::GetBlobSize(const NVDiskFlat::TGetResultItemRaw& item) const {
        return HasBlob(item) ? item.PayloadSize : 0;
    }

    TRope TEvBlobStorage::TEvVGetResultFlat::GetBlobData(const NVDiskFlat::TGetResultItemRaw& item) const {
        if (!HasBlob(item)) {
            return {};
        }
        const TRope& payload = Bytes<TPayloadTag>().Rope();
        return TRope(payload.Position(item.PayloadOffset), payload.Position(item.PayloadOffset + item.PayloadSize));
    }

    TString TEvBlobStorage::TEvVGetResultFlat::ToString() const {
        TStringStream str;
        str << "{EvVGetResultFlat Status# " << NKikimrProto::EReplyStatus_Name(GetStatus());
        auto items = Array<TItemsTag>();
        str << " Items# " << items.size();
        str << "}";
        return str.Str();
    }

} // namespace NKikimr
